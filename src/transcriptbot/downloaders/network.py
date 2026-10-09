"""HTTPX/HTTPcore transport: public DNS only, numeric connection, original Host/TLS."""

import ipaddress
import socket
import ssl
import time
from collections.abc import Callable
from queue import Empty, Queue
from threading import Thread
from urllib.parse import urljoin, urlsplit

import httpcore
import httpx
from httpcore._backends.sync import SyncStream

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.retries import retry_after


def validate_url(value: str, *, mailru: bool = False) -> str:
    try:
        url = urlsplit(value)
        host = url.hostname
        port = url.port
        if (
            url.scheme not in {"http", "https"}
            or not host
            or url.username is not None
            or url.password is not None
            or port == 0
            or "\\" in value
            or any(ord(c) <= 32 or ord(c) == 127 for c in value)
            or "%" in host
        ):
            raise ValueError
        host = host.encode("idna").decode("ascii").lower().rstrip(".")
        if mailru and (
            url.scheme != "https"
            or port not in {None, 443}
            or not any(
                host == domain or host.endswith("." + domain)
                for domain in ("cloud.mail.ru", "datacloudmail.ru")
            )
        ):
            raise ValueError
    except (ValueError, UnicodeError):
        raise DownloadError("unsafe_url") from None
    return host


def public_address(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        if isinstance(address, ipaddress.IPv6Address) and (
            address.sixtofour
            or address.teredo
            or address in ipaddress.ip_network("64:ff9b::/96")
            or address in ipaddress.ip_network("64:ff9b:1::/48")
        ):
            return False
        if str(address) == "168.63.129.16":  # Azure platform/metadata virtual address.
            return False
        return address.is_global and not (
            address.is_multicast or address.is_reserved or address.is_unspecified
        )
    except ValueError:
        return False


class PublicBackend(httpcore.NetworkBackend):
    def __init__(self, resolver: Callable = socket.getaddrinfo):
        self.resolver = resolver

    def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        # Reject every mixed answer. Resolve once; connect() receives a numeric address.
        deadline = time.monotonic() + (timeout or 10)
        result = Queue(maxsize=1)

        def resolve():
            try:
                result.put(self.resolver(host, port, type=socket.SOCK_STREAM))
            except OSError:
                result.put(None)

        # DNS is blocking OS I/O. A daemon cannot keep the isolated child alive on timeout.
        Thread(target=resolve, daemon=True).start()
        try:
            answers = result.get(timeout=timeout or 10)
        except Empty:
            raise DownloadError("download_network", retryable=True) from None
        if answers is None:
            raise DownloadError("download_network", retryable=True)
        if not answers or any(not public_address(answer[4][0]) for answer in answers):
            raise DownloadError("unsafe_address")
        for family, kind, protocol, _, address in answers:
            if family not in {socket.AF_INET, socket.AF_INET6}:
                raise DownloadError("unsafe_address")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock = socket.socket(family, kind, protocol)
            try:
                sock.settimeout(remaining)
                for option in socket_options or ():
                    sock.setsockopt(*option)
                sock.connect(address)
                return SyncStream(sock)
            except OSError:
                sock.close()
        raise DownloadError("download_network", retryable=True)

    def connect_unix_socket(self, *args, **kwargs):
        raise DownloadError("unsafe_address")


class PublicTransport(httpx.HTTPTransport):
    def __init__(self, backend: httpcore.NetworkBackend | None = None):
        super().__init__(trust_env=False)
        self._pool.close()
        # The sole version-specific bridge; HTTPX has no public network_backend argument.
        self._pool = httpcore.ConnectionPool(
            ssl_context=httpcore.default_ssl_context(),
            network_backend=backend or PublicBackend(),
            max_connections=2,
            max_keepalive_connections=0,
            retries=0,
        )

    def handle_request(self, request):
        validate_url(str(request.url))
        return super().handle_request(request)


class SafeHTTP:
    def __init__(self, *, connect=10, read=60, redirects=5, transport=None):
        self.redirects = redirects
        self.client = httpx.Client(
            transport=transport or PublicTransport(),
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(read, connect=connect),
            headers={"Accept-Encoding": "identity"},
        )

    def open(self, url, *, method="GET", headers=None, content=None, mailru=False):
        headers = dict(headers or {})
        # Host belongs to the validated URL; no caller can override it.
        headers = {k: v for k, v in headers.items() if k.lower() not in {"host", "authorization"}}
        headers["Accept-Encoding"] = "identity"
        for hop in range(self.redirects + 1):
            validate_url(url, mailru=mailru)
            try:
                response = self.client.send(
                    self.client.build_request(method, url, headers=headers, content=content),
                    stream=True,
                )
            except httpx.HTTPError as exc:
                cause = exc
                while cause is not None:
                    if isinstance(cause, ssl.SSLCertVerificationError):
                        raise DownloadError("download_tls") from None
                    cause = cause.__cause__ or cause.__context__
                raise DownloadError("download_network", retryable=True) from None
            if response.status_code not in {301, 302, 303, 307, 308}:
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    response.close()
                    raise DownloadError("unsupported_encoding")
                return response
            location = response.headers.get("location")
            response.close()
            if location is None or hop == self.redirects:
                raise DownloadError("download_redirects")
            target = urljoin(url, location)
            if urlsplit(url).netloc != urlsplit(target).netloc:
                headers = {k: v for k, v in headers.items() if k.lower() != "cookie"}
                self.client.cookies.clear()
            url = target
            if response.status_code == 303 or (
                response.status_code in {301, 302} and method == "POST"
            ):
                method, content = "GET", None
        raise DownloadError("download_redirects")

    def close(self):
        self.client.close()


def check_status(response: httpx.Response) -> None:
    status = response.status_code
    if status == 429:
        delay = retry_after(response.headers.get("retry-after"), time.time()) or 60
        raise DownloadError("download_rate_limit", retryable=True, retry_after=delay)
    if status >= 500:
        raise DownloadError("download_server", retryable=True)
    if status != 200:
        raise DownloadError("source_unavailable")


def response_bytes(response, limit: int) -> bytes:
    result = bytearray()
    try:
        for block in response.iter_raw(64 * 1024):
            result.extend(block)
            if len(result) > limit:
                raise DownloadError("response_limit")
    except httpx.HTTPError:
        raise DownloadError("download_network", retryable=True) from None
    return bytes(result)

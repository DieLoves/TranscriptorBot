import socket
import ssl

import httpcore
import httpx
import pytest

from transcriptbot.downloaders.errors import DownloadError
from transcriptbot.downloaders.network import (
    PublicBackend,
    PublicTransport,
    SafeHTTP,
    public_address,
    response_bytes,
    validate_url,
)


@pytest.mark.parametrize(
    "value",
    [
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "100.64.0.1",
        "0.0.0.0",
        "192.0.2.1",
        "224.0.0.1",
        "255.255.255.255",
        "::1",
        "::",
        "fe80::1",
        "fc00::1",
        "ff02::1",
        "2001:db8::1",
        "::ffff:127.0.0.1",
        "2002:7f00:1::",
        "64:ff9b::a00:1",
        "168.63.129.16",
    ],
)
def test_non_public_addresses(value):
    assert not public_address(value)


@pytest.mark.parametrize("value", ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"])
def test_public_addresses(value):
    assert public_address(value)


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "ftp://example.com/a",
        "http://user:pass@example.com/a",
        "https://example.com:0/a",
        "http://[fe80::1%25eth0]/a",
        "https://a\\@example.com/a",
        "https://example.com/a\n",
        "http:///a",
    ],
)
def test_bad_urls(url):
    with pytest.raises(DownloadError, match="unsafe_url"):
        validate_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://cloud.mail.ru/a",
        "https://cloud.mail.ru.evil.test/a",
        "https://mail.ru/a",
        "https://cloclo.cloud.mail.ru:444/a",
        "https://8.8.8.8/a",
    ],
)
def test_mailru_domain_policy(url):
    with pytest.raises(DownloadError, match="unsafe_url"):
        validate_url(url, mailru=True)


def test_mailru_subdomain():
    assert validate_url("https://cloclo61.cloud.mail.ru/a", mailru=True) == "cloclo61.cloud.mail.ru"


def answers(*addresses):
    return [
        (
            socket.AF_INET6 if ":" in ip else socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
            "",
            (ip, 443),
        )
        for ip in addresses
    ]


@pytest.mark.parametrize("addresses", [(), ("8.8.8.8", "127.0.0.1"), ("::1", "2606:4700::1111")])
def test_mixed_or_empty_dns_rejected(addresses, monkeypatch):
    def no_socket(*args):
        pytest.fail("must reject before creating socket")

    monkeypatch.setattr(socket, "socket", no_socket)
    with pytest.raises(DownloadError, match="unsafe_address"):
        PublicBackend(lambda *a, **kw: answers(*addresses)).connect_tcp("example.com", 443)


@pytest.mark.parametrize("address", ["8.8.8.8", "2606:4700:4700::1111"])
def test_single_dns_resolution_numeric_connect(address, monkeypatch):
    resolved, connected = [], []

    class Socket:
        def settimeout(self, value):
            assert 0 < value <= 10

        def connect(self, value):
            connected.append(value)

        def close(self):
            pass

    def resolver(host, port, **kwargs):
        resolved.append(host)
        # A second lookup would return loopback (DNS rebinding).
        return answers(address if len(resolved) == 1 else "127.0.0.1")

    monkeypatch.setattr(socket, "socket", lambda *args: Socket())
    stream = PublicBackend(resolver).connect_tcp("example.com", 443, timeout=10)
    assert resolved == ["example.com"]
    assert connected == [(address, 443)]
    stream.close()


def test_original_host_and_tls_name(monkeypatch):
    writes, tls = [], []

    class Stream(httpcore.NetworkStream):
        def read(self, max_bytes, timeout=None):
            return b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

        def write(self, buffer, timeout=None):
            writes.append(buffer)

        def close(self):
            pass

        def start_tls(self, ssl_context, server_hostname=None, timeout=None):
            tls.append(server_hostname)
            assert ssl_context.verify_mode == ssl.CERT_REQUIRED
            assert ssl_context.check_hostname
            return self

        def get_extra_info(self, info):
            return None

    class Backend(httpcore.NetworkBackend):
        def connect_tcp(self, host, port, **kwargs):
            assert host == "example.com"
            return Stream()

    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    http = SafeHTTP(transport=PublicTransport(Backend()))
    try:
        response = http.open("https://example.com/test", headers={"Host": "evil.test"})
        assert response_bytes(response, 20) == b"ok"
        response.close()
        assert tls == ["example.com"]
        assert b"Host: example.com" in b"".join(writes)
    finally:
        http.close()


def test_each_redirect_checked_and_bounded():
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/private"})

    class CheckedTransport(httpx.BaseTransport):
        def handle_request(self, request):
            # Mock socket policy for this integration fixture.
            assert request.url.host != "127.0.0.1", "private redirect reached transport"
            return handler(request)

    http = SafeHTTP(transport=CheckedTransport())

    # SafeHTTP URL policy is syntactic; PublicTransport's backend does the IP check.
    # Use the production backend on the redirected request, without opening any socket.
    def dispatch(request):
        if request.url.host == "127.0.0.1":
            PublicBackend(lambda *a, **kw: answers("127.0.0.1")).connect_tcp("127.0.0.1", 80)
        return handler(request)

    http.close()
    http = SafeHTTP(transport=httpx.MockTransport(dispatch))
    with pytest.raises(DownloadError, match="unsafe_address"):
        http.open("https://example.com/a")
    assert seen == ["https://example.com/a"]
    http.close()
    http = SafeHTTP(
        redirects=2,
        transport=httpx.MockTransport(
            lambda r: httpx.Response(302, headers={"Location": "/again"})
        ),
    )
    with pytest.raises(DownloadError, match="download_redirects"):
        http.open("https://example.com/a")
    http.close()


def test_mailru_redirect_cannot_escape_allowlist():
    http = SafeHTTP(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(302, headers={"Location": "https://example.com/file"})
        )
    )
    with pytest.raises(DownloadError, match="unsafe_url"):
        http.open("https://cloud.mail.ru/a", mailru=True)
    http.close()


def test_encoded_body_rejected():
    http = SafeHTTP(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, headers={"Content-Encoding": "br"})
        )
    )
    with pytest.raises(DownloadError, match="unsupported_encoding"):
        http.open("https://example.com/a")
    http.close()


def test_dns_timeout_is_bounded():
    import time

    def delayed(*args, **kwargs):
        time.sleep(0.2)
        return answers("8.8.8.8")

    started = time.monotonic()
    with pytest.raises(DownloadError, match="download_network"):
        PublicBackend(delayed).connect_tcp("example.com", 443, timeout=0.02)
    assert time.monotonic() - started < 0.1


def test_invalid_tls_certificate_rejected(tmp_path):
    import subprocess
    import threading

    from httpcore._backends.sync import SyncStream

    certificate, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=example.com",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)

    def accept():
        sock, _ = listener.accept()
        try:
            with context.wrap_socket(sock, server_side=True):
                pass
        except ssl.SSLError:
            sock.close()

    thread = threading.Thread(target=accept)
    thread.start()

    class LocalTestBackend(httpcore.NetworkBackend):
        def connect_tcp(self, host, port, **kwargs):
            return SyncStream(socket.create_connection(listener.getsockname(), timeout=2))

    http = SafeHTTP(transport=PublicTransport(LocalTestBackend()))
    try:
        with pytest.raises(DownloadError, match="download_tls"):
            http.open("https://example.com/")
    finally:
        http.close()
        thread.join(timeout=3)
        listener.close()


@pytest.mark.parametrize(
    "value,delay",
    [
        ("7200", 7200),
        ("Thu, 08 Oct 2026 12:02:00 GMT", 120),
        ("broken", 60),
        ("nan", 60),
    ],
)
def test_retry_after_seconds_and_http_date(value, delay, monkeypatch):
    from datetime import UTC, datetime

    from transcriptbot.downloaders.network import check_status

    monkeypatch.setattr("time.time", lambda: datetime(2026, 10, 8, 12, tzinfo=UTC).timestamp())
    with pytest.raises(DownloadError) as error:
        check_status(httpx.Response(429, headers={"Retry-After": value}))
    assert error.value.retry_after == delay

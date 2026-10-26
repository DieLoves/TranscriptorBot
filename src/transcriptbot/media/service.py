"""Recoverable preparation, one-part transcription, assembly and delivery handoff."""

import asyncio
import hashlib
import itertools
import json
import shutil
from pathlib import Path

from transcriptbot.config import Settings
from transcriptbot.downloaders.files import job_directory, regular_path
from transcriptbot.media.artifacts import (
    RESULT_LIMIT,
    assemble,
    context_prompt,
    digest,
    publish_bytes,
    publish_checkpoint,
    read_json,
    validate_response,
)
from transcriptbot.media.errors import PipelineError
from transcriptbot.media.planning import RATE, ChunkPlan
from transcriptbot.media.tools import analyze, extract
from transcriptbot.storage.models import Job, JobStatus, Phase
from transcriptbot.storage.repository import Repository
from transcriptbot.telegram.store import TelegramStore


class MediaPipeline:
    def __init__(self, settings: Settings, repository: Repository, groq):
        self.settings, self.repository, self.groq = settings, repository, groq
        self.store = TelegramStore(repository)
        self._verified: dict[int, tuple] = {}
        # Acknowledged checkpoints already validated by this process. Rereading all
        # earlier responses on every step is quadratic; assembly rereads them anyway.
        self._checked: set[tuple] = set()

    async def alive(self, job: Job) -> bool:
        current = await self.repository.get_job(job.id, owner_id=job.owner_id)
        return (
            current is not None
            and current.status == JobStatus.ACTIVE
            and current.revision == job.revision
        )

    async def track(self, job: Job, path: Path) -> None:
        await self.repository.track_asset(job.id, kind="workspace", path=str(path))
        await self.repository.track_asset(
            job.id, kind="workspace", path=str(path.with_name(path.name + ".part"))
        )

    async def source(self, job: Job, directory: Path) -> dict:
        manifest = read_json(directory / "source.json", maximum=65536)
        try:
            root = self.settings.telegram_files_dir if job.source_kind == "telegram" else directory
            source = regular_path(Path(manifest["path"]), root)
            stat = source.stat()
            identity = (
                job.generation,
                manifest["sha256"],
                str(source),
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            )
            if (
                manifest["job_id"] != job.id
                or manifest["kind"] != job.source_kind
                or manifest["version"] != 1
                or stat.st_size != manifest["size_bytes"]
                or type(manifest["audio_stream"]) is not int
                or manifest["audio_stream"] < 0
            ):
                raise ValueError
            if self._verified.get(job.id) != identity:
                value = hashlib.sha256()
                with source.open("rb") as audio:
                    while block := audio.read(1024 * 1024):
                        value.update(block)
                        await asyncio.sleep(0)
                if value.hexdigest() != manifest["sha256"]:
                    raise ValueError
                self._verified.clear()
                self._verified[job.id] = identity
            return manifest
        except (KeyError, ValueError, TypeError):
            raise PipelineError("source_changed") from None

    def plan_identity(self, job: Job, manifest: dict) -> dict:
        return {
            "version": 1,
            "job_id": job.id,
            "model": job.model,
            "source_sha256": manifest["sha256"],
        }

    async def prepare(self, job: Job, directory: Path, manifest: dict) -> None:
        path = directory / "chunks.json"
        await self.track(job, path)
        identity = self.plan_identity(job, manifest)
        if path.exists() or path.is_symlink():
            value = read_json(path)
            if any(value.get(key) != field for key, field in identity.items()):
                raise PipelineError("checkpoint_invalid")
        else:
            chunks = await analyze(Path(manifest["path"]), manifest["audio_stream"], self.settings)
            if not await self.alive(job):
                return
            value = {**identity, "boundaries": [[c.start_sample, c.end_sample] for c in chunks]}
            publish_checkpoint(path, value)
        try:
            boundaries = value["boundaries"]
            if (
                not isinstance(boundaries, list)
                or not boundaries
                or boundaries[0][0] != 0
                or any(
                    len(pair) != 2
                    or any(type(x) is not int for x in pair)
                    or not 0 <= pair[0] < pair[1]
                    for pair in boundaries
                )
                or any(left[1] != right[0] for left, right in itertools.pairwise(boundaries))
                or boundaries[-1][1] > self.settings.max_duration_seconds * RATE
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError, IndexError):
            raise PipelineError("checkpoint_invalid") from None
        try:
            if await self.repository.plan_chunks(job, [tuple(pair) for pair in boundaries]):
                await self.repository.advance(job, Phase.TRANSCRIBE)
        except ValueError:
            raise PipelineError("checkpoint_invalid") from None

    def response(self, job: Job, manifest: dict, chunk, path: Path) -> dict:
        envelope = read_json(path)
        identity = {
            **self.plan_identity(job, manifest),
            "number": chunk.number,
            "start_sample": chunk.start_sample,
            "end_sample": chunk.end_sample,
        }
        if any(envelope.get(key) != value for key, value in identity.items()):
            raise PipelineError("checkpoint_invalid")
        if chunk.response_path is not None and (
            chunk.response_path != str(path) or digest(path) != chunk.response_sha256
        ):
            raise PipelineError("checkpoint_invalid")
        if "response" not in envelope:
            raise PipelineError("checkpoint_invalid")
        try:
            validate_response(envelope["response"])
        except PipelineError:
            raise PipelineError("checkpoint_invalid") from None
        return envelope["response"]

    async def transcribe(self, job: Job, directory: Path, manifest: dict) -> None:
        chunks = await self.repository.chunks(job.id)
        if not chunks:
            raise PipelineError("checkpoint_invalid")
        owner = (job.id, job.generation)
        self._checked = {key for key in self._checked if key[:2] == owner}
        for chunk in chunks:
            path = directory / f"response-{chunk.number:04d}.json"
            if chunk.response_path is not None:
                key = (*owner, chunk.number, chunk.response_sha256)
                if key not in self._checked:
                    self.response(job, manifest, chunk, path)
                    self._checked.add(key)
                    await asyncio.sleep(0)
                continue
            await self.track(job, path)
            if path.exists() or path.is_symlink():
                # Recover the crash between fsync/publication and SQLite acknowledgement.
                self.response(job, manifest, chunk, path)
                sha256 = digest(path)
                if await self.repository.save_chunk_response(
                    job, number=chunk.number, path=str(path), sha256=sha256
                ):
                    self._checked.add((*owner, chunk.number, sha256))
                    await self.repository.reset_chunk_attempts(job)
                return
            seconds = (chunk.end_sample - chunk.start_sample) / RATE
            if max(10, seconds) > min(
                self.settings.groq_audio_seconds_per_hour, self.settings.groq_audio_seconds_per_day
            ):
                raise PipelineError("groq_quota_configuration")
            # Waiting for account quota must not decode the source on every wakeup.
            _, until = await self.repository.reserve_groq(
                job,
                number=chunk.number,
                seconds=seconds,
                hourly=self.settings.groq_audio_seconds_per_hour,
                daily=self.settings.groq_audio_seconds_per_day,
                reserve=False,
            )
            if until is not None:
                raise PipelineError(
                    "groq_quota_local",
                    quota=True,
                    retry_after=max(1, until - self.repository.clock()),
                )
            if not await self.alive(job):
                return
            prompt = None
            if chunk.number:
                # Read before reserving quota; deterministic, so a retry sends the same context.
                previous = chunks[chunk.number - 1]
                before = directory / f"response-{previous.number:04d}.json"
                prompt = context_prompt(self.response(job, manifest, previous, before)["text"])
            upload = directory / "current.wav"
            await self.track(job, upload)
            if (
                shutil.disk_usage(directory).free
                < self.settings.wav_target_upload_bytes + 16_000_000
            ):
                raise PipelineError("disk_space")
            await extract(
                Path(manifest["path"]),
                manifest["audio_stream"],
                ChunkPlan(chunk.start_sample, chunk.end_sample),
                upload,
                self.settings,
            )
            if not await self.alive(job):
                return
            usage_id, until = await self.repository.reserve_groq(
                job,
                number=chunk.number,
                seconds=seconds,
                hourly=self.settings.groq_audio_seconds_per_hour,
                daily=self.settings.groq_audio_seconds_per_day,
            )
            if until is not None:
                raise PipelineError(
                    "groq_quota_local",
                    quota=True,
                    retry_after=max(1, until - self.repository.clock()),
                )
            if usage_id is None or not await self.alive(job):
                return
            try:
                response = await self.groq.transcribe(upload, prompt)
            except PipelineError as exc:
                if not exc.uncertain:
                    await self.repository.finish_groq_usage(usage_id, outcome="rejected")
                raise
            await self.repository.finish_groq_usage(usage_id, outcome="succeeded")
            if not await self.alive(job):
                return
            validate_response(response)
            envelope = {
                **self.plan_identity(job, manifest),
                "number": chunk.number,
                "start_sample": chunk.start_sample,
                "end_sample": chunk.end_sample,
                "response": response,
            }
            sha256 = publish_checkpoint(path, envelope)
            if await self.repository.save_chunk_response(
                job, number=chunk.number, path=str(path), sha256=sha256
            ):
                self._checked.add((*owner, chunk.number, sha256))
                await self.repository.reset_chunk_attempts(job)
            return
        await self.repository.advance(job, Phase.ASSEMBLE)

    async def assemble(self, job: Job, directory: Path, manifest: dict) -> None:
        chunks = await self.repository.chunks(job.id)
        if not chunks or any(chunk.response_path is None for chunk in chunks):
            raise PipelineError("checkpoint_invalid")
        responses = []
        response_bytes = 0
        for chunk in chunks:
            response = self.response(
                job, manifest, chunk, directory / f"response-{chunk.number:04d}.json"
            )
            # The final JSON retains every raw response. Reject an impossible
            # result before loading all 4 hours of individually bounded JSONs.
            response_bytes += len(
                json.dumps(response, ensure_ascii=False, allow_nan=False).encode("utf-8")
            )
            if response_bytes > RESULT_LIMIT:
                raise PipelineError("result_size_limit")
            responses.append(response)
            await asyncio.sleep(0)
        value = assemble(chunks, responses)
        txt, verbose_json = self.result_paths(directory)
        for path in (txt, verbose_json):
            await self.track(job, path)
        if not await self.alive(job):
            return
        text = value["text"]
        publish_bytes(
            txt, (text + ("\n" if text and not text.endswith("\n") else "")).encode("utf-8")
        )
        publish_bytes(
            verbose_json,
            (json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8"),
        )
        await self.repository.advance(job, Phase.DELIVER)

    @staticmethod
    def result_paths(directory: Path) -> tuple[Path, Path]:
        return directory / "result.txt", directory / "result.verbose.json"

    async def step(self, job: Job) -> None:
        directory = job_directory(self.settings.work_dir, job.id)
        if not await self.alive(job):
            return
        if job.phase == Phase.DELIVER:
            txt, verbose_json = self.result_paths(directory)
            # Delivery recovery needs only the published results; no new recognition.
            for path in (txt, verbose_json):
                regular_path(path, directory)
            await self.store.enqueue_results(job, txt=txt, verbose_json=verbose_json)
            return
        manifest = await self.source(job, directory)
        if job.phase == Phase.PREPARE:
            await self.prepare(job, directory, manifest)
        elif job.phase == Phase.TRANSCRIBE:
            await self.transcribe(job, directory, manifest)
        elif job.phase == Phase.ASSEMBLE:
            await self.assemble(job, directory, manifest)

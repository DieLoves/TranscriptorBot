import asyncio
import json
import wave

import pytest

from transcriptbot.config import Settings
from transcriptbot.downloaders.files import source_manifest
from transcriptbot.media.artifacts import read_json
from transcriptbot.media.errors import PipelineError
from transcriptbot.media.planning import RATE
from transcriptbot.media.service import MediaPipeline
from transcriptbot.storage.database import Database
from transcriptbot.storage.models import Phase, SourceKind, SourceRef
from transcriptbot.storage.repository import Repository
from transcriptbot.worker import process_job


@pytest.fixture
async def repository(settings):
    database = await Database(settings.sqlite_path).open(migrate=True)
    yield Repository(database, clock=lambda: 1000)
    await database.close()


class SimulatedCrash(BaseException):
    """Process death: unlike application errors, it must not be turned into a job state."""


class FakeGroq:
    def __init__(self):
        self.calls = 0
        self.responses = []
        self.prompts = []
        self.error = None

    async def transcribe(self, path, prompt=None):
        assert path.is_file()
        self.calls += 1
        self.prompts.append(prompt)
        if self.error:
            raise self.error
        response = {
            "text": f"Часть {self.calls}.",
            "language": "ru" if self.calls == 1 else "en",
            "segments": [{"id": 7, "start": 0.25, "end": 0.5, "text": "Речь."}],
            "extra": {"preserved": self.calls},
        }
        self.responses.append(response)
        return response


async def create(repository, settings, *, seconds=5, update_id=1):
    result = await repository.create_job(
        update_id=update_id,
        owner_id=123,
        chat_id=123,
        message_id=update_id,
        source=SourceRef(SourceKind.DIRECT, "https://example.com/audio", "Аудио"),
    )
    job = await repository.claim_next()
    assert job.id == result.job_id
    directory = settings.work_dir / str(job.id)
    directory.mkdir(parents=True)
    source = directory / "source.media"
    with wave.open(str(source), "wb") as audio:
        audio.setparams((1, 2, RATE, 0, "NONE", "none"))
        audio.writeframes(b"\x00\x10" * round(seconds * RATE))
    manifest = source_manifest(
        directory,
        {
            "job_id": job.id,
            "kind": "direct",
            "maximum": settings.max_input_bytes,
            "duration": settings.max_duration_seconds,
            "probe_timeout": 30,
            "title": "Аудио",
        },
        source,
    )
    return await repository.finish_download(job, manifest)


@pytest.fixture
def small_settings(settings):
    return Settings(
        _env_file=None,
        **{**settings.model_dump(), "chunk_seconds": 2, "silence_window_seconds": 0.5},
    )


async def step(repository, pipeline):
    job = await repository.claim_next()
    assert job is not None
    await process_job(job, pipeline, repository, asyncio.Event())
    return await repository.get_job(job.id, owner_id=123)


async def to_deliver(repository, pipeline):
    for _ in range(20):
        current = await step(repository, pipeline)
        assert current.status == "active"
        if current.phase == "deliver":
            await step(repository, pipeline)
            return current
    raise AssertionError("pipeline did not finish")


async def test_actual_pipeline_multi_and_delivery_recovery(repository, small_settings):
    groq = FakeGroq()
    job = await create(repository, small_settings)
    pipeline = MediaPipeline(small_settings, repository, groq)
    current = await to_deliver(repository, pipeline)
    assert groq.calls == 3
    directory = small_settings.work_dir / str(job.id)
    result = read_json(directory / "result.verbose.json")
    assert result["text"] == "Часть 1. Часть 2. Часть 3."
    assert result["duration"] == 5
    assert result["language"] == "ru"
    assert result["segments"][1]["start"] == 2.25
    assert result["segments"][2]["start"] == 4.25
    assert result["chunks"][1]["response"] == groq.responses[1]
    assert (directory / "result.txt").read_text() == result["text"] + "\n"
    # A restarted worker on deliver needs neither source nor chunk responses.
    (directory / "source.media").unlink()
    await MediaPipeline(small_settings, repository, groq).step(current)
    operations = await repository.database.call(
        lambda c: [
            tuple(row)
            for row in c.execute(
                "SELECT operation, status FROM outbox WHERE operation IN ('preview','txt','json')"
            )
        ]
    )
    assert sorted(operations) == [("json", "pending"), ("preview", "pending"), ("txt", "pending")]
    assert groq.calls == 3
    assets = await repository.database.call(
        lambda c: [r[0] for r in c.execute("SELECT path FROM assets")]
    )
    assert str(directory / "current.wav") in assets
    assert str(directory / "current.wav.part") in assets
    assert str(directory / "response-0002.json") in assets
    assert str(directory / "result.txt.part") in assets


@pytest.mark.parametrize("empty", [False, True])
async def test_single_raw_json_and_empty_result(repository, settings, empty):
    raw = {"text": "" if empty else "  Слова.  ", "language": "ru", "unknown": [1], "segments": []}

    class Groq:
        async def transcribe(self, path, prompt=None):
            return raw

    job = await create(repository, settings, seconds=1)
    await to_deliver(repository, MediaPipeline(settings, repository, Groq()))
    directory = settings.work_dir / str(job.id)
    assert read_json(directory / "result.verbose.json") == raw
    assert (directory / "result.txt").read_text() == (raw["text"] + "\n" if not empty else "")


async def test_assembly_bounds_total_responses_before_loading_all_parts(
    repository, small_settings, monkeypatch
):
    class Groq:
        calls = 0

        async def transcribe(self, path, prompt=None):
            self.calls += 1
            return {"text": "x" * 2048, "segments": []}

    job = await create(repository, small_settings)
    groq = Groq()
    pipeline = MediaPipeline(small_settings, repository, groq)
    for _ in range(10):
        current = await step(repository, pipeline)
        if current.phase == Phase.ASSEMBLE:
            break
    assert current.phase == Phase.ASSEMBLE and groq.calls == 3
    loaded = []
    response = pipeline.response

    def observe(job, manifest, chunk, path):
        loaded.append(chunk.number)
        return response(job, manifest, chunk, path)

    monkeypatch.setattr(pipeline, "response", observe)
    monkeypatch.setattr("transcriptbot.media.service.RESULT_LIMIT", 3500)
    await step(repository, pipeline)
    current = await repository.get_job(job.id, owner_id=123)
    assert current.status == "failed" and current.error_code == "result_size_limit"
    assert loaded == [0, 1]  # The third JSON is never read into the aggregate.
    assert groq.calls == 3
    directory = small_settings.work_dir / str(job.id)
    assert len(list(directory.glob("response-*.json"))) == 3
    assert not (directory / "result.txt").exists()


async def test_restart_saved_and_orphan_response_no_api_repeat(
    repository, small_settings, monkeypatch
):
    job = await create(repository, small_settings)
    groq = FakeGroq()
    pipeline = MediaPipeline(small_settings, repository, groq)
    await step(repository, pipeline)  # plan
    await step(repository, pipeline)  # first acknowledged response
    save = repository.save_chunk_response

    async def crash(*args, **kwargs):
        raise SimulatedCrash

    monkeypatch.setattr(repository, "save_chunk_response", crash)
    with pytest.raises(SimulatedCrash):
        await step(repository, pipeline)
    assert groq.calls == 2
    chunks = await repository.chunks(job.id)
    assert chunks[0].response_path is not None and chunks[1].response_path is None
    monkeypatch.setattr(repository, "save_chunk_response", save)
    pipeline = MediaPipeline(
        small_settings, Repository(repository.database, clock=repository.clock), groq
    )
    await step(repository, pipeline)
    assert groq.calls == 2
    assert (await repository.chunks(job.id))[1].response_path is not None
    await to_deliver(repository, pipeline)
    assert groq.calls == 3


async def test_prepare_checkpoint_survives_before_database_commit(
    repository, settings, monkeypatch
):
    job = await create(repository, settings, seconds=1)
    pipeline = MediaPipeline(settings, repository, FakeGroq())
    save = repository.plan_chunks

    async def crash(*args, **kwargs):
        raise SimulatedCrash

    monkeypatch.setattr(repository, "plan_chunks", crash)
    with pytest.raises(SimulatedCrash):
        await step(repository, pipeline)
    assert not await repository.chunks(job.id)
    assert (settings.work_dir / str(job.id) / "chunks.json").is_file()
    monkeypatch.setattr(repository, "plan_chunks", save)

    async def no_analyze(*args):
        raise AssertionError("published plan must be reused")

    monkeypatch.setattr("transcriptbot.media.service.analyze", no_analyze)
    assert (
        await step(repository, MediaPipeline(settings, repository, FakeGroq()))
    ).phase == "transcribe"


async def test_assembly_recovery_after_only_txt_publication(repository, settings, monkeypatch):
    job = await create(repository, settings, seconds=1)
    groq = FakeGroq()
    pipeline = MediaPipeline(settings, repository, groq)
    for _ in range(3):
        await step(repository, pipeline)
    assert (await repository.claim_next()).phase == "assemble"
    from transcriptbot.media.service import publish_bytes

    def crash(path, value):
        if path.name.endswith("json"):
            raise SimulatedCrash
        publish_bytes(path, value)

    monkeypatch.setattr("transcriptbot.media.service.publish_bytes", crash)
    with pytest.raises(SimulatedCrash):
        await step(repository, pipeline)
    assert (settings.work_dir / str(job.id) / "result.txt").exists()
    monkeypatch.setattr("transcriptbot.media.service.publish_bytes", publish_bytes)
    assert (await step(repository, pipeline)).phase == "deliver"
    assert groq.calls == 1


@pytest.mark.parametrize("corruption", ["hash", "missing", "identity", "json", "source"])
async def test_corrupted_checkpoint_never_retranscribed(repository, settings, corruption):
    job = await create(repository, settings, seconds=1)
    groq = FakeGroq()
    pipeline = MediaPipeline(settings, repository, groq)
    await step(repository, pipeline)
    await step(repository, pipeline)
    path = settings.work_dir / str(job.id) / "response-0000.json"
    value = read_json(path)
    if corruption == "missing":
        path.unlink()
    elif corruption == "json":
        path.write_text("bad json")
    elif corruption == "source":
        (path.parent / "source.media").write_bytes(b"changed")
    else:
        if corruption == "identity":
            value["source_sha256"] = "a" * 64
        else:
            value["response"]["text"] = "changed"
        path.write_text(json.dumps(value))
    current = await step(repository, MediaPipeline(settings, repository, groq))
    assert current.status == "failed"
    assert groq.calls == 1
    assert current.error_code in {"checkpoint_invalid", "source_changed", "unsafe_path"}


async def test_each_part_gets_the_previous_text_as_context(repository, small_settings):
    await create(repository, small_settings)
    groq = FakeGroq()
    pipeline = MediaPipeline(small_settings, repository, groq)
    await to_deliver(repository, pipeline)
    # Parts starting mid-sentence lose punctuation and words without this context.
    assert groq.prompts == [None, *[f"Часть {number}." for number in range(1, groq.calls)]]


async def test_transcription_validates_each_saved_checkpoint_once_per_process(
    repository, small_settings, monkeypatch
):
    job = await create(repository, small_settings)
    pipeline = MediaPipeline(small_settings, repository, FakeGroq())
    calls = []
    original = pipeline.response

    def response(job, manifest, chunk, path):
        calls.append(chunk.number)
        return original(job, manifest, chunk, path)

    monkeypatch.setattr(pipeline, "response", response)
    current = await step(repository, pipeline)
    while current.phase == Phase.TRANSCRIBE:
        current = await step(repository, pipeline)
    assert current.phase == Phase.ASSEMBLE
    assert len(await repository.chunks(job.id)) >= 3
    # Rereading every earlier response on each step is quadratic for long records.
    assert all(calls.count(number) <= 1 for number in calls)
    calls.clear()
    assert (await step(repository, pipeline)).phase == Phase.DELIVER
    assert sorted(calls) == list(range(len(await repository.chunks(job.id))))


async def test_unexpected_pipeline_error_retries_then_fails_resumably(repository, settings, caplog):
    job = await create(repository, settings, seconds=1)
    groq = FakeGroq()
    pipeline = MediaPipeline(settings, repository, groq)
    await step(repository, pipeline)
    groq.error = TypeError("raw https://api.example/?key=secret")
    with caplog.at_level("INFO"):
        for attempt in range(settings.max_retries + 1):
            current = await step(repository, pipeline)
            assert current.error_code == "internal_error"
            if attempt < settings.max_retries:
                assert current.status == "retry_wait"
                repository.clock = lambda deadline=current.next_attempt_at: deadline
    assert current.status == "failed" and current.resumable
    assert current.phase == Phase.TRANSCRIBE
    assert "TypeError" in caplog.text and "secret" not in caplog.text
    groq.error = None
    assert (await repository.chunks(job.id))[0].response_path is None


@pytest.mark.parametrize("quota", [False, True])
async def test_persisted_backoff_and_success_reset(repository, settings, quota):
    job = await create(repository, settings, seconds=1)
    groq = FakeGroq()
    pipeline = MediaPipeline(settings, repository, groq)
    await step(repository, pipeline)
    groq.error = PipelineError(
        "groq_quota" if quota else "groq_network",
        quota=quota,
        retryable=not quota,
        uncertain=not quota,
    )
    for index in range(7 if quota else 5):
        current = await step(repository, pipeline)
        expected = min(3600, 60 * 2**index) if quota else (5, 15, 45, 135, 300)[index]
        assert current.next_attempt_at == repository.clock() + expected
        assert current.attempts == (0 if quota else index + 1)
        assert current.quota_attempts == (index + 1 if quota else 0)
        assert await repository.claim_next() is None
        recovered = Repository(repository.database, clock=repository.clock)
        assert (await recovered.active_job()).next_attempt_at == current.next_attempt_at
        repository.clock = lambda timestamp=current.next_attempt_at: timestamp
    groq.error = None
    current = await step(repository, pipeline)
    assert current.status == "active" and current.attempts == current.quota_attempts == 0
    assert (await repository.chunks(job.id))[0].response_path is not None
    outcomes = await repository.database.call(
        lambda c: [r[0] for r in c.execute("SELECT outcome FROM groq_usage")]
    )
    assert outcomes == (["rejected"] * 7 if quota else ["uncertain"] * 5) + ["succeeded"]


async def test_retry_after_and_permanent_failure_release_fifo(repository, settings):
    job = await create(repository, settings, seconds=1)
    groq = FakeGroq()
    pipeline = MediaPipeline(settings, repository, groq)
    await step(repository, pipeline)
    await repository.create_job(
        update_id=2,
        owner_id=123,
        chat_id=123,
        message_id=2,
        source=SourceRef(SourceKind.DIRECT, "https://example.com/second"),
    )
    groq.error = PipelineError("groq_quota", quota=True, retry_after=123)
    current = await step(repository, pipeline)
    assert current.next_attempt_at == 1123
    assert await repository.claim_next() is None
    repository.clock = lambda: 1123
    groq.error = PipelineError("groq_auth")
    current = await step(repository, pipeline)
    assert current.status == "failed" and current.retention_until == 1123 + 7 * 86400
    assert (await repository.claim_next()).id != job.id


async def test_manual_resume_preserves_saved_parts(repository, small_settings):
    job = await create(repository, small_settings)
    groq = FakeGroq()
    pipeline = MediaPipeline(small_settings, repository, groq)
    await step(repository, pipeline)
    await step(repository, pipeline)
    groq.error = PipelineError("groq_request")
    failed = await step(repository, pipeline)
    assert failed.status == "failed"
    await repository.resume(update_id=2, job_id=job.id, owner_id=123, generation=0)
    groq.error = None
    await to_deliver(repository, MediaPipeline(small_settings, repository, groq))
    assert groq.calls == 4  # 3 successful requests and the one rejected request.


@pytest.mark.parametrize("shutdown_request", [False, True])
async def test_cancel_or_shutdown_during_groq(repository, settings, shutdown_request):
    job = await create(repository, settings, seconds=1)
    started, stopped, shutdown = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Groq:
        async def transcribe(self, path, prompt=None):
            started.set()
            try:
                await asyncio.Future()
            finally:
                stopped.set()

    pipeline = MediaPipeline(settings, repository, Groq())
    await step(repository, pipeline)
    job = await repository.claim_next()
    operation = asyncio.create_task(process_job(job, pipeline, repository, shutdown))
    await asyncio.wait_for(started.wait(), 5)
    if shutdown_request:
        shutdown.set()
    else:
        await repository.cancel(update_id=2, job_id=job.id, owner_id=123, generation=0)
    await asyncio.wait_for(operation, 3)
    assert stopped.is_set()
    assert not (settings.work_dir / str(job.id) / "response-0000.json").exists()
    assert (await repository.chunks(job.id))[0].response_path is None
    assert (await repository.get_job(job.id, owner_id=123)).status == (
        "active" if shutdown_request else "cancelled"
    )
    assert (
        await repository.database.call(
            lambda c: c.execute("SELECT outcome FROM groq_usage").fetchone()[0]
        )
        == "uncertain"
    )


async def test_late_response_after_cancel_is_not_published(repository, settings):
    job = await create(repository, settings, seconds=1)

    class Groq:
        async def transcribe(self, path, prompt=None):
            await repository.cancel(update_id=2, job_id=job.id, owner_id=123, generation=0)
            return {"text": "late", "segments": []}

    pipeline = MediaPipeline(settings, repository, Groq())
    await step(repository, pipeline)
    current = await step(repository, pipeline)
    assert current.status == "cancelled"
    assert not (settings.work_dir / str(job.id) / "response-0000.json").exists()


async def test_audio_quota_journal_hour_day_and_restart(repository, settings):
    job = await create(repository, settings, seconds=1)
    await repository.plan_chunks(job, [(0, 14400 * RATE)])
    job = await repository.advance(job, Phase.TRANSCRIBE)
    for number in range(12):
        usage, until = await repository.reserve_groq(
            job, number=number, seconds=600, hourly=7200, daily=28800
        )
        assert usage is not None and until is None
        if number % 2:
            await repository.finish_groq_usage(usage, outcome="succeeded")
    recovered = Repository(repository.database, clock=lambda: 1010)
    assert await recovered.reserve_groq(job, number=12, seconds=600, hourly=7200, daily=28800) == (
        None,
        4600,
    )
    for timestamp in (4600, 8200, 11800):
        repository.clock = lambda current=timestamp: current
        for number in range(12):
            usage, until = await repository.reserve_groq(
                job, number=number, seconds=600, hourly=7200, daily=28800
            )
            assert usage is not None and until is None
    repository.clock = lambda: 15400
    assert await repository.reserve_groq(job, number=12, seconds=600, hourly=7200, daily=28800) == (
        None,
        87400,
    )


async def test_rejected_requests_release_audio_and_stale_job_cannot_reserve(repository, settings):
    job = await create(repository, settings, seconds=1)
    await repository.plan_chunks(job, [(0, RATE)])
    job = await repository.advance(job, Phase.TRANSCRIBE)
    usage, until = await repository.reserve_groq(job, number=0, seconds=1, hourly=10, daily=10)
    assert usage is not None and until is None
    assert await repository.reserve_groq(job, number=0, seconds=1, hourly=10, daily=10) == (
        None,
        87400,
    )
    await repository.finish_groq_usage(usage, outcome="rejected")
    assert (await repository.reserve_groq(job, number=0, seconds=1, hourly=10, daily=10))[
        0
    ] is not None
    await repository.cancel(update_id=2, job_id=job.id, owner_id=123, generation=0)
    assert await repository.reserve_groq(job, number=0, seconds=1, hourly=10, daily=10) == (
        None,
        None,
    )


async def test_quota_wait_does_not_extract_audio(repository, settings, monkeypatch):
    job = await create(repository, settings, seconds=1)
    pipeline = MediaPipeline(settings, repository, FakeGroq())
    job = await step(repository, pipeline)
    # Fill the hourly account quota using conservative uncertain reservations.
    await repository.reserve_groq(job, number=0, seconds=7200, hourly=7200, daily=28800)

    async def forbidden(*args, **kwargs):
        raise AssertionError("quota wait must not run FFmpeg")

    monkeypatch.setattr("transcriptbot.media.service.extract", forbidden)
    job = await step(repository, pipeline)
    assert job.status == "quota_wait" and job.error_code == "groq_quota_local"
    assert not (settings.work_dir / str(job.id) / "current.wav").exists()

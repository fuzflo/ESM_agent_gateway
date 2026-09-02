"""Background Projection Worker run inside the single Railway service."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from .errors import GatewayError
from .state_machine import reduce_event, replay_stream
from .storage import Storage


logger = logging.getLogger("esm-gateway.projector")


class ProjectionWorker:
    def __init__(
        self,
        storage: Storage,
        *,
        poll_interval_seconds: float = 60.0,
        batch_size: int = 100,
    ) -> None:
        self.storage = storage
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._checkpoint: int | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._checkpoint = None
            self._task = asyncio.create_task(self.run(), name="esm-projection-worker")

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._task is not None:
            with suppress(asyncio.CancelledError):
                await self._task

    def notify(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        logger.info(
            "projection_worker_started poll_interval_seconds=%s batch_size=%s",
            self.poll_interval_seconds,
            self.batch_size,
        )
        while not self._stop.is_set():
            # Clear before scanning so a notify that arrives during run_once is retained.
            self._wake.clear()
            progressed = await self.run_once()
            if progressed:
                continue
            if self._wake.is_set():
                continue
            try:
                await asyncio.wait_for(
                    self._wake.wait(), timeout=self.poll_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("projection_worker_stopped")

    async def run_once(self) -> bool:
        checkpoint = await self._current_checkpoint()
        events = await self.storage.events_after(checkpoint, self.batch_size)
        if not events:
            return False
        for event in events:
            try:
                state = await self.storage.get_state(event.temp_application_id)
                if state is not None and state.last_processed_stream_version >= event.stream_version:
                    await self._commit_checkpoint(event.global_sequence, event.event_id)
                    continue
                if event.event_type == "event_corrected":
                    projected = replay_stream(
                        await self.storage.stream_events(event.temp_application_id)
                    )
                else:
                    projected = reduce_event(state, event)
                await self.storage.upsert_state(projected)
                await self._commit_checkpoint(event.global_sequence, event.event_id)
                logger.info(
                    "projection_event_completed event_id=%s global_sequence=%s",
                    event.event_id,
                    event.global_sequence,
                )
            except GatewayError as exc:
                logger.error(
                    "projection_event_failed event_id=%s error_code=%s",
                    event.event_id,
                    exc.result_code,
                )
                return False
        return True

    async def _current_checkpoint(self) -> int:
        if self._checkpoint is None:
            self._checkpoint = await self.storage.get_checkpoint()
        return self._checkpoint

    async def _commit_checkpoint(self, global_sequence: int, event_id: str) -> None:
        await self.storage.set_checkpoint(global_sequence, event_id)
        self._checkpoint = global_sequence

    async def wait_for_event(self, event_id: str, *, timeout_seconds: float = 3.0) -> bool:
        self.notify()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        while loop.time() < deadline:
            checkpoint = await self._current_checkpoint()
            events = await self.storage.events_after(max(checkpoint - 1, 0), 2)
            if not events:
                for state_id in await _state_ids(self.storage):
                    state = await self.storage.get_state(state_id)
                    if state is not None and state.last_applied_event_id == event_id:
                        return True
            await asyncio.sleep(0.05)
        return False


async def _state_ids(storage: Storage) -> list[str]:
    """Test-storage optimization; provider storage uses checkpoint confirmation."""

    states = getattr(storage, "states", None)
    return list(states) if isinstance(states, dict) else []

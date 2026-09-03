"""Background Projection Worker run inside the single Railway service."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .errors import GatewayError
from .state_machine import reduce_event, replay_stream
from .storage import Storage
from .util import utc_text


logger = logging.getLogger("esm-gateway.projector")


class ProjectionWorker:
    def __init__(
        self,
        storage: Storage,
        *,
        poll_interval_seconds: float = 60.0,
        batch_size: int = 100,
        retry_delay_seconds: float = 5.0,
    ) -> None:
        self.storage = storage
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size
        self.retry_delay_seconds = retry_delay_seconds
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._checkpoint: int | None = None
        self._iteration_error_code: str | None = None
        self._last_poll_at: str | None = None
        self._last_successful_poll_at: str | None = None
        self._last_error_at: str | None = None
        self._last_error_code: str | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._checkpoint = None
            self._task = asyncio.create_task(self.run(), name="esm-projection-worker")
            self._task.add_done_callback(self._task_finished)

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._task is not None:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("projection_worker_stop_observed_failure")

    def notify(self) -> None:
        self._wake.set()

    def health_status(self) -> dict[str, Any]:
        task = self._task
        return {
            "projection_worker_running": task is not None and not task.done(),
            "projection_worker_task_done": None if task is None else task.done(),
            "projection_last_poll_at": self._last_poll_at,
            "projection_last_successful_poll_at": self._last_successful_poll_at,
            "projection_last_error_at": self._last_error_at,
            "projection_last_error_code": self._last_error_code,
            "projection_checkpoint_cached": self._checkpoint,
        }

    async def run(self) -> None:
        logger.info(
            "projection_worker_started poll_interval_seconds=%s batch_size=%s",
            self.poll_interval_seconds,
            self.batch_size,
        )
        try:
            while not self._stop.is_set():
                # Clear before scanning so a notify that arrives during run_once is retained.
                self._wake.clear()
                self._last_poll_at = utc_text()
                self._iteration_error_code = None
                try:
                    progressed = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except GatewayError as exc:
                    self._record_iteration_failure(exc.result_code)
                    logger.exception(
                        "projection_worker_iteration_failed error_code=%s",
                        exc.result_code,
                    )
                    await self._wait_for_wake(self.retry_delay_seconds)
                    continue
                except Exception:
                    self._record_iteration_failure("PROJECTION_WORKER_INTERNAL_ERROR")
                    logger.exception(
                        "projection_worker_iteration_failed "
                        "error_code=PROJECTION_WORKER_INTERNAL_ERROR"
                    )
                    await self._wait_for_wake(self.retry_delay_seconds)
                    continue

                if self._iteration_error_code is not None:
                    self._record_iteration_failure(self._iteration_error_code)
                    await self._wait_for_wake(self.retry_delay_seconds)
                    continue

                recovered_from = self._last_error_code
                self._last_successful_poll_at = utc_text()
                self._last_error_code = None
                self._last_error_at = None
                if recovered_from is not None:
                    logger.info(
                        "projection_worker_recovered previous_error_code=%s",
                        recovered_from,
                    )
                logger.info(
                    "projection_poll_completed checkpoint=%s progressed=%s",
                    self._checkpoint,
                    progressed,
                )
                if progressed:
                    continue
                await self._wait_for_wake(self.poll_interval_seconds)
        finally:
            logger.info("projection_worker_stopped")

    async def run_once(self) -> bool:
        self._iteration_error_code = None
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
                self._iteration_error_code = exc.result_code
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

    async def _wait_for_wake(self, timeout_seconds: float) -> None:
        if self._stop.is_set() or self._wake.is_set():
            return
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=timeout_seconds)
        except TimeoutError:
            pass

    def _record_iteration_failure(self, error_code: str) -> None:
        self._checkpoint = None
        self._last_error_code = error_code
        self._last_error_at = utc_text()

    def _task_finished(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "projection_worker_terminated_unexpectedly",
                exc_info=(type(error), error, error.__traceback__),
            )
        elif not self._stop.is_set():
            logger.error("projection_worker_terminated_unexpectedly error_code=NO_ERROR")

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

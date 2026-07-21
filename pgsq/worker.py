"""Fair-queuing background worker for pgsq.

Polls ``pgsq_task`` for ``READY`` or ``FAILED`` tasks, respecting per-tenant
slot limits (``PgsqTaskSlot``).  Tasks are executed in a multi-process pool
(Pebble) and results are written back to the database.
"""

from __future__ import annotations

import importlib
import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from traceback import format_exception
from typing import Any

from django.db import DatabaseError, OperationalError, connection, transaction
from django.utils import timezone
from django.utils.crypto import get_random_string

from django_tasks import TaskResultStatus
from django_tasks.base import Task, TaskContext, TaskError, TaskResult
from django_tasks.signals import task_finished, task_started

logger = logging.getLogger("pgsq.worker")

DEFAULT_SLOTS = 3
POLL_INTERVAL = 1.0  # seconds between polls when queue is empty
TASK_TIMEOUT = 20  # seconds before a task is considered hung


# ---------------------------------------------------------------------------
#  Fair-queue picker  (raw SQL — Django ORM can't express CTEs + SKIP LOCKED)
# ---------------------------------------------------------------------------

def _claim_task() -> str | None:
    """Atomically claim the next eligible task, or return ``None``.

    Uses the same two-CTE pattern as the original pgsq:

      1. Count running tasks per tenant in the last 6 hours.
      2. Identify tenants at their slot capacity.
      3. Pick the oldest ``READY``/``FAILED`` task whose tenant still has
         room and whose ``retry_time`` has elapsed.

    The selected row is both locked (``FOR UPDATE SKIP LOCKED``) and
    updated to ``RUNNING`` in the same statement, preventing double-claims.
    """
    sql = """
    WITH running_jobs_per_queue AS (
        SELECT
            tenant_id,
            count(1) AS running_jobs
        FROM pgsq_task
        WHERE status IN ('RUNNING')
          AND created_at > NOW() - INTERVAL '6 hours'
        GROUP BY tenant_id
    ),
    full_queues AS (
        SELECT R.tenant_id
        FROM running_jobs_per_queue R
        LEFT JOIN pgsq_task_slot Q ON R.tenant_id = Q.tenant_id
        WHERE R.running_jobs >= COALESCE(Q.slots, %s)
    ),
    candidate AS (
        SELECT task_id
        FROM pgsq_task
        WHERE status IN ('READY', 'FAILED')
          AND tenant_id NOT IN (SELECT tenant_id FROM full_queues)
          AND retry_time <= NOW()
        ORDER BY priority DESC, created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ),
    updated AS (
        UPDATE pgsq_task
        SET status = 'RUNNING',
            started_at = NOW(),
            last_attempted_at = NOW()
        WHERE task_id IN (SELECT task_id FROM candidate)
        RETURNING task_id
    )
    SELECT task_id FROM updated
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, [DEFAULT_SLOTS])
        row = cursor.fetchone()
        if row is None:
            return None
        return row[0]


# ---------------------------------------------------------------------------
#  Task execution
# ---------------------------------------------------------------------------

def _import_func(func_path: str):
    """Import a module-level function from its dotted path."""
    module_name, _, func_name = func_path.rpartition(".")
    mod = importlib.import_module(module_name)
    return getattr(mod, func_name)


def _resolve_func(func_path: str):
    """Import the callable at *func_path*, unwrapping a ``Task`` if needed.

    ``@task()`` replaces the module-level function with a ``Task`` instance,
    so a plain ``importlib.import_module(name); getattr(mod, name)`` returns
    the ``Task`` wrapper.  This helper peels off the wrapper to get the
    underlying callable.
    """
    module_name, _, func_name = func_path.rpartition(".")
    mod = importlib.import_module(module_name)
    obj = getattr(mod, func_name)
    from django_tasks.base import Task as TaskClass

    if isinstance(obj, TaskClass):
        return obj.func  # unwrap the Task instance
    return obj


def _execute_task_row(row) -> str | None:
    """Run the task function and update the DB row.

    Returns the serialised return value (JSON) on success, or ``None`` if the
    function returned ``None``.  On failure the row status is set to ``FAILED``
    with error details written to the ``errors`` JSON field.
    """
    worker_id = os.environ.get("PGSQ_WORKER_ID", get_random_string(32))

    try:
        func = _resolve_func(row.func_path)
    except (ImportError, AttributeError) as e:
        _record_failure(row, e)
        return None

    task_obj = Task(
        func=func,
        priority=row.priority,
        queue_name=row.queue_name,
        run_after=row.run_after,
        takes_context=row.takes_context,
        backend=row.backend_alias,
    )
    task_result = TaskResult(
        task=task_obj,
        id=row.task_id,
        status=TaskResultStatus.RUNNING,
        enqueued_at=row.enqueued_at,
        started_at=row.started_at or timezone.now(),
        finished_at=None,
        last_attempted_at=row.last_attempted_at or timezone.now(),
        args=row.args or [],
        kwargs=row.kwargs or {},
        backend=row.backend_alias,
        errors=[],
        worker_ids=row.worker_ids or [],
    )
    task_started.send(sender=_PGSQ_SENDER, task_result=task_result)
    logger.info("Executing %s (%s)", row.func_path, row.task_id[:8])

    try:
        if row.takes_context:
            raw_return = func(TaskContext(task_result=task_result), *row.args, **row.kwargs)
        else:
            raw_return = func(*row.args, **row.kwargs)
    except BaseException as exc:
        exc_type = type(exc)
        error_entry = TaskError(
            exception_class_path=f"{exc_type.__module__}.{exc_type.__qualname__}",
            traceback="".join(format_exception(exc)),
        )
        now = timezone.now()
        one_sec = timedelta(seconds=1)
        if row.retry_time and row.created_at:
            retry_time = row.retry_time + (
                (row.retry_time - row.created_at + one_sec) * 2
            )
        else:
            retry_time = now + timedelta(seconds=10)
        delay = (retry_time - now).total_seconds()
        logger.info(
            "Task %s (%s) failed — retrying in %.0fs (ETA %s)",
            row.task_id[:8], row.func_path,
            delay, retry_time.strftime("%H:%M:%S"),
        )
        _update_row(
            row,
            status=TaskResultStatus.FAILED,
            finished_at=now,
            last_attempted_at=now,
            retry_time=retry_time,
            errors=[{"exception_class_path": error_entry.exception_class_path,
                     "traceback": error_entry.traceback}],
            return_value=None,
        )
        task_result = TaskResult(
            task=task_obj,
            id=row.task_id,
            status=TaskResultStatus.FAILED,
            enqueued_at=row.enqueued_at,
            started_at=row.started_at,
            finished_at=now,
            last_attempted_at=now,
            args=row.args or [],
            kwargs=row.kwargs or {},
            backend=row.backend_alias,
            errors=[error_entry],
            worker_ids=row.worker_ids or [],
        )
        task_finished.send(sender=_PGSQ_SENDER, task_result=task_result)
        return None

    now = timezone.now()
    _update_row(
        row,
        status=TaskResultStatus.SUCCESSFUL,
        finished_at=now,
        last_attempted_at=now,
        errors=[],
        return_value=raw_return,
    )
    task_result = TaskResult(
        task=task_obj,
        id=row.task_id,
        status=TaskResultStatus.SUCCESSFUL,
        enqueued_at=row.enqueued_at,
        started_at=row.started_at,
        finished_at=now,
        last_attempted_at=now,
        args=row.args or [],
        kwargs=row.kwargs or {},
        backend=row.backend_alias,
        errors=[],
        worker_ids=row.worker_ids or [],
    )
    task_finished.send(sender=_PGSQ_SENDER, task_result=task_result)
    return raw_return


def _update_row(row, **kwargs):
    """Update a single PgsqTask row."""
    from pgsq.models import PgsqTask

    PgsqTask.objects.filter(task_id=row.task_id).update(**kwargs)


def _record_failure(row, exc):
    """Set a task to FAILED when the function itself can't be loaded."""
    exc_type = type(exc)
    now = timezone.now()
    _update_row(
        row,
        status=TaskResultStatus.FAILED,
        finished_at=now,
        last_attempted_at=now,
        errors=[
            {
                "exception_class_path": f"{exc_type.__module__}.{exc_type.__qualname__}",
                "traceback": "".join(format_exception(exc)),
            }
        ],
        return_value=None,
    )


# ---------------------------------------------------------------------------
#  Worker sub-process entry point
# ---------------------------------------------------------------------------

def _worker_run(task_id: str) -> Any:
    """Execute a task in a worker thread."""
    from pgsq.models import PgsqTask  # noqa: F811

    row = PgsqTask.objects.get(task_id=task_id)
    return _execute_task_row(row)


# ---------------------------------------------------------------------------
#  Main loop (runs in the supervisor / management command)
# ---------------------------------------------------------------------------

def run_worker(*, num_workers: int = 1, install_signals: bool = True):
    """Enter the infinite worker loop.

    Parameters
    ----------
    num_workers:
        Number of concurrent worker threads.
    install_signals:
        Whether to install SIGTERM/SIGINT handlers.  Set to ``False``
        when running inside a non-main thread (``signal.signal`` only
        works from the main thread of the main interpreter).
    """
    worker_id = os.environ.get("PGSQ_WORKER_ID", get_random_string(32))
    logger.info(
        "pgsq worker %s starting (threads=%d)", worker_id, num_workers,
    )

    executor: ThreadPoolExecutor | None = None

    if install_signals:

        def _shutdown(*_args):
            logger.info("pgsq worker %s shutting down …", worker_id)
            if executor is not None:
                executor.shutdown(wait=False, cancel_futures=True)
            raise SystemExit(0)

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            logger.info("pgsq worker %s pool ready", worker_id)
            _poll_loop(executor, worker_id)
    except (SystemExit, KeyboardInterrupt):
        logger.info("pgsq worker %s exiting", worker_id)


def _poll_loop(executor: ThreadPoolExecutor, worker_id: str):
    """Continuously poll for tasks and submit them to the thread pool."""
    while True:
        try:
            task_id = _claim_task()
        except (DatabaseError, OperationalError) as exc:
            logger.warning("DB error in poll loop: %s — retrying in 3s", exc)
            time.sleep(3)
            continue
        finally:
            # Close the poll connection so threads don't compete on it.
            connection.close()

        if task_id is None:
            time.sleep(POLL_INTERVAL)
            continue

        logger.info("Claimed task %s", task_id)

        _submit_and_watch(executor, task_id)


def _submit_and_watch(executor: ThreadPoolExecutor, task_id: str):
    """Submit *task_id* to *executor* and attach a completion callback."""
    future = executor.submit(_worker_run, task_id)

    def _done(f, tid=task_id):
        try:
            result = f.result(timeout=TASK_TIMEOUT)
            logger.info("Task %s finished: %s", tid, result)
        except TimeoutError:
            logger.warning("Task %s timed out after %ss", tid, TASK_TIMEOUT)
            _handle_timeout(tid)
        except Exception as exc:
            logger.error("Task %s worker error: %s", tid, exc)

    future.add_done_callback(_done)


def _handle_timeout(task_id: str):
    """Mark a timed-out task as FAILED and allow retry."""
    from pgsq.models import PgsqTask

    try:
        row = PgsqTask.objects.get(task_id=task_id)
        now = timezone.now()
        one_sec = timedelta(seconds=1)
        if row.retry_time and row.created_at:
            retry_time = row.retry_time + (
                (row.retry_time - row.created_at + one_sec) * 2
            )
        else:
            retry_time = now + timedelta(seconds=10)
        _update_row(
            row,
            status=TaskResultStatus.FAILED,
            finished_at=now,
            retry_time=retry_time,
            errors=(
                row.errors or []
            )
            + [
                {
                    "exception_class_path": "builtins.TimeoutError",
                    "traceback": f"Task timed out after {TASK_TIMEOUT}s",
                }
            ],
        )
    except PgsqTask.DoesNotExist:
        logger.warning("Timeout handler: task %s already gone", task_id)


# Signal sender identifier — a dotted-path string works fine for signal
# filtering and avoids importing the backend class (which pulls in models).
_PGSQ_SENDER = "pgsq.worker"


# Entry point: users should run ``manage.py pgsq_worker``.
# This ``__main__`` block is intentionally omitted — pgsq is a reusable
# package and must not hardcode a project's settings module.

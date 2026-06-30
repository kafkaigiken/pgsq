import logging

from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.module_loading import import_string

from django_tasks import TaskResult, TaskResultStatus
from django_tasks.backends.base import BaseTaskBackend
from django_tasks.base import Task, TaskError
from django_tasks.exceptions import TaskResultDoesNotExist
from django_tasks.utils import normalize_json

from pgsq.models import PgsqTask

logger = logging.getLogger(__name__)


class PgsqBackend(BaseTaskBackend):
    """PostgreSQL-backed task queue backend for Django's Tasks API.

    Stores tasks in a ``pgsq_task`` table and supports fair-queuing via
    per-tenant slot limits (``PgsqTaskSlot``).  Enqueued tasks are picked
    up by a separate worker process (``manage.py pgsq_worker``).
    """

    supports_defer = True
    supports_get_result = True
    supports_priority = True
    supports_async_task = True
    supports_retry = True

    # ---- enqueue / aenqueue ------------------------------------------------

    def enqueue(self, task: Task, args, kwargs):
        self.validate_task(task)

        task_id = get_random_string(32)
        now = timezone.now()
        normalized_args = normalize_json(args)
        normalized_kwargs = normalize_json(kwargs)

        PgsqTask.objects.create(
            task_id=task_id,
            func_path=task.module_path,
            priority=task.priority,
            queue_name=task.queue_name,
            run_after=task.run_after,
            takes_context=task.takes_context,
            backend_alias=task.backend,
            username="default",
            name=task.name or "",
            status=TaskResultStatus.READY,
            args=normalized_args,
            kwargs=normalized_kwargs,
            enqueued_at=now,
            retry_time=task.run_after or now,
            errors=[],
            worker_ids=[],
        )

        return TaskResult(
            task=task,
            id=task_id,
            status=TaskResultStatus.READY,
            enqueued_at=now,
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=normalized_args,
            kwargs=normalized_kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

    # ---- get_result / aget_result ------------------------------------------

    def get_result(self, result_id: str) -> TaskResult:
        try:
            row = PgsqTask.objects.get(task_id=result_id)
        except PgsqTask.DoesNotExist:
            raise TaskResultDoesNotExist(
                f"Task result {result_id!r} does not exist."
            )

        task = self._reconstruct_task(row)
        errors = [TaskError(**e) for e in (row.errors or [])]

        result = TaskResult(
            task=task,
            id=row.task_id,
            status=row.status,
            enqueued_at=row.enqueued_at,
            started_at=row.started_at,
            finished_at=row.finished_at,
            last_attempted_at=row.last_attempted_at,
            args=row.args or [],
            kwargs=row.kwargs or {},
            backend=row.backend_alias or self.alias,
            errors=errors,
            worker_ids=row.worker_ids or [],
        )
        object.__setattr__(result, "_return_value", row.return_value)
        return result

    # ---- check -------------------------------------------------------------

    def check(self, **kwargs):
        from django.core.checks import Error

        messages = []
        try:
            from django.db import connection

            connection.introspection.get_table_list(connection.cursor())
        except Exception as e:
            messages.append(
                Error(
                    f"Cannot connect to database: {e}",
                    hint="Ensure the database is running and accessible.",
                    obj=self,
                    id="pgsq.E001",
                )
            )
        return messages

    # ---- internal helpers --------------------------------------------------

    def _reconstruct_task(self, row: PgsqTask) -> Task:
        try:
            func = import_string(row.func_path)
        except (ImportError, AttributeError) as e:
            raise TaskResultDoesNotExist(
                f"Cannot import task function {row.func_path!r}: {e}"
            ) from e

        # When ``@task()`` decorates the function, the module-level name
        # points to a ``Task`` instance.  Unwrap to get the raw callable.
        if isinstance(func, Task):
            func = func.func

        return Task(
            func=func,
            priority=row.priority,
            queue_name=row.queue_name,
            run_after=row.run_after,
            takes_context=row.takes_context,
            backend=row.backend_alias,
        )

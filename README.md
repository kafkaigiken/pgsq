# pgsq — PostgreSQL task queue for Django's Tasks API

A third-party backend for [Django's Tasks framework](https://docs.djangoproject.com/en/6.0/topics/tasks/) (introduced in Django 6.0), using PostgreSQL for queue storage and fair-queuing via per-tenant concurrency slots.

> **Why `django_tasks` and not `django.tasks`?**
> The Tasks API was added in Django 6.0. If you're on an older Django (5.x), you can use the [`django-tasks`](https://pypi.org/project/django-tasks/) backport package — it provides the same API under the `django_tasks` namespace. pgsq depends on `django-tasks` so it works on both Django 5.x and 6.x. When you upgrade to Django 6.0, swap the package and switch imports to `django.tasks`.

Features:
- **PostgreSQL-backed** — no Redis or other infra required
- **Fair queuing** — per-tenant slot limits prevent noisy neighbours from starving other tenants
- **Exponential backoff** — failed tasks retry with doubling delays (1s → 3s → 7s → …)
- **Async worker** — non-blocking enqueue; worker threads drain the queue asynchronously
- **Queue introspection** — query results via `get_result()` or inspect the `pgsq_task` table

## Quick start

```python
# settings.py
INSTALLED_APPS = [
    ...,
    "django_tasks",
    "pgsq",
]

TASKS = {
    "default": {
        "BACKEND": "pgsq.backend.PgsqBackend",
    },
}
```

```python
# myapp/tasks.py
from django_tasks import task

@task()
def send_welcome_email(user_email: str) -> str:
    # ...
    return f"sent to {user_email}"

# enqueue (returns immediately)
result = send_welcome_email.enqueue("user@example.com")
```

Run migrations, then start a worker:

```bash
./manage.py pgsq_worker --num=4
```

## Usage

### Enqueue tasks

```python
from django_tasks import task

@task()
def add(a: int, b: int) -> int:
    return a + b

result = add.enqueue(21, 21)
# result.id       — opaque tracking id
# result.status   — "READY" | "RUNNING" | "SUCCESSFUL" | "FAILED"
```

### Check results

```python
from django_tasks import default_task_backend

result = default_task_backend.get_result(result_id)
# result.return_value  — the task's return value (SUCCESSFUL only)
# result.errors[0].traceback  — exception traceback (FAILED only)
result.refresh()
```

### Per-tenant slots (fair queuing)

Each task carries a **tenant** (the `username` column of `pgsq_task`). The
worker's fair-queuing slots key off it, so one busy tenant can't starve the
rest. Set the tenant per-enqueue with `.using(username=...)`:

```python
result = send_welcome_email.using(username="user@example.com").enqueue(
    "user@example.com"
)
```

Tasks enqueued without a tenant land on the `"default"` tenant. Configure
per-tenant concurrency limits:

```python
from pgsq.models import PgsqTaskSlot

PgsqTaskSlot.objects.create(username="user@example.com", slots=5)
PgsqTaskSlot.objects.create(username="another@example.com", slots=2)
```

Tenants with no slot record default to 3 concurrent tasks.

### Run the worker

```bash
# 4 worker threads
./manage.py pgsq_worker --num=4

# Ctrl+C to stop
```

The worker logs lifecycle events for each task:

```
03:03:58 [INFO] Claimed task abc123...
03:03:58 [INFO] Executing myapp.tasks.send_welcome_email (abc123...)
03:03:58 [INFO] Task abc123... finished: sent to user@example.com
03:03:58 [INFO] Task abc123... (myapp.tasks.send_welcome_email) failed — retrying in 5s (ETA 03:04:06)
```

## Models

### `PgsqTask`

Stores one queued/executed task. Fields mirror Django's `TaskResult` dataclass. The table is created by running `migrate`.

### `PgsqTaskSlot`

Per-tenant concurrency limit. When a tenant reaches their slot limit, no new tasks for that tenant are picked up until one finishes.

## Development

```bash
git clone <repo>
cd pgsq
uv pip install -e .
```

Run tests:

```bash
uv run python test_pgsq.py
```

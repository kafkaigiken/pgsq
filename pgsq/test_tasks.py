"""Task functions for the pgsq end-to-end test.

Must be inside the pgsq package so Pebble ``spawn``-mode child processes
can import them by dotted path (``pgsq.test_tasks.add``).
"""

from django_tasks import task


@task()
def add(a: int, b: int) -> int:
    return a + b


@task()
def fail_always() -> int:
    raise ValueError("This task is designed to fail")

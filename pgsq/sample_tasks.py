"""Sample tasks to manually test pgsq.

Usage::

    cd akita_project
    .venv/bin/python -c "
    import django; django.setup()
    from pgsq.sample_tasks import hello, add
    r1 = hello.enqueue('world')
    r2 = add.enqueue(21, 21)
    print(f'Enqueued: {r1.id}, {r2.id}')
    "
    .venv/bin/python manage.py pgsq_worker --num 2
"""

import logging

from django_tasks import task

logger = logging.getLogger(__name__)


@task()
def hello(name: str) -> str:
    """Simple greeting task."""
    msg = f"Hello, {name}!"
    logger.info(msg)
    return msg


@task()
def add(a: int, b: int) -> int:
    """Add two numbers."""
    result = a + b
    logger.info("%s + %s = %s", a, b, result)
    return result

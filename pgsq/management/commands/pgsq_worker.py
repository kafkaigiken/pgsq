"""``manage.py pgsq_worker`` — start a pgsq worker pool."""

import logging

from django.core.management.base import BaseCommand

from pgsq.worker import run_worker


class Command(BaseCommand):
    help = "Start pgsq worker(s) to drain the task queue."

    def add_arguments(self, parser):
        parser.add_argument(
            "--num",
            type=int,
            default=1,
            help="Number of concurrent worker threads (default: 1).",
        )
        parser.add_argument(
            "--log-level",
            default="INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            help="Log level for pgsq worker output (default: INFO).",
        )

    def handle(self, **options):
        level = getattr(logging, options["log_level"].upper(), logging.INFO)
        logger = logging.getLogger("pgsq.worker")
        logger.setLevel(level)
        logger.propagate = False
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
            )
        )
        logger.addHandler(handler)

        num = options["num"]
        self.stdout.write(f"Starting pgsq worker pool ({num} thread(s)) …")
        run_worker(num_workers=num)

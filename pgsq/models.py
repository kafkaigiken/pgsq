from django.db import models


class PgsqTask(models.Model):
    """A task in the pgsq queue.

    Maps directly onto Django's ``Task`` and ``TaskResult`` dataclasses,
    while keeping the original pgsq fair-queuing concepts (tenant-aware
    slot limits via ``PgsqTaskSlot``).
    """

    # Primary key matches ``TaskResult.id`` (random 32-char string).
    task_id = models.CharField(max_length=32, unique=True, primary_key=True)

    # --- Task metadata (reconstructed from ``django.tasks.base.Task``) ---
    func_path = models.CharField(
        max_length=512,
        help_text="Dotted module path to the task function, e.g. 'myapp.tasks.send_email'",
    )
    priority = models.IntegerField(default=0)
    queue_name = models.CharField(max_length=255, default="default")
    run_after = models.DateTimeField(null=True, blank=True)
    takes_context = models.BooleanField(default=False)
    backend_alias = models.CharField(max_length=255, default="default")

    # --- Tenant identifier (original pgsq concept) ---
    username = models.CharField(
        max_length=255, db_index=True, default="default"
    )

    # --- Human-readable name ---
    name = models.CharField(max_length=255, blank=True, default="")

    # --- Lifecycle ---
    status = models.CharField(
        max_length=20,
        db_index=True,
        default="READY",
        choices=[
            ("READY", "READY"),
            ("RUNNING", "RUNNING"),
            ("SUCCESSFUL", "SUCCESSFUL"),
            ("FAILED", "FAILED"),
        ],
    )
    args = models.JSONField(default=list)
    kwargs = models.JSONField(default=dict)

    # --- Timing ---
    enqueued_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    retry_time = models.DateTimeField(
        null=True, blank=True, db_index=True,
        help_text="Earliest time this task may be picked up again (exponential backoff).",
    )

    # --- Result ---
    return_value = models.JSONField(null=True, blank=True)
    errors = models.JSONField(default=list)  # list[dict] with exception_class_path + traceback
    worker_ids = models.JSONField(default=list)

    class Meta:
        db_table = "pgsq_task"
        verbose_name = "pgsq task"
        verbose_name_plural = "pgsq tasks"
        indexes = [
            models.Index(fields=["status", "retry_time"]),
        ]

    def __str__(self):
        return f"[{self.status}] {self.name or self.func_path} ({self.username})"


class PgsqTaskSlot(models.Model):
    """Per-tenant concurrent-slot limit.

    When a tenant has *running* tasks >= ``slots``, the worker will skip
    picking up new tasks for that tenant until one finishes.  Tenants with
    no ``PgsqTaskSlot`` record default to a limit of 3.
    """

    username = models.CharField(max_length=255, unique=True)
    slots = models.IntegerField()

    class Meta:
        db_table = "pgsq_task_slot"
        verbose_name = "pgsq task slot"
        verbose_name_plural = "pgsq task slots"

    def __str__(self):
        return f"{self.username}: {self.slots} slots"

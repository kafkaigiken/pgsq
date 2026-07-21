from django.apps import AppConfig


class PgsqConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "pgsq"
    verbose_name = "pgsq — PostgreSQL Task Queue"

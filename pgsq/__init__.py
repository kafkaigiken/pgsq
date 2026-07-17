"""pgsq: PostgreSQL-backed multi-tenant task queue for Django's Tasks API."""

__version__ = "0.2.0"

__all__ = ["PgsqBackend", "PgsqTask", "PgsqTaskSlot", "TenantTask"]


def __getattr__(name):
    """Lazy imports so Django can populate the app registry at setup time."""
    if name in __all__:
        import importlib

        return importlib.import_module(f"pgsq.{__import_mapping[name]}").__dict__[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__import_mapping = {
    "PgsqBackend": "backend",
    "TenantTask": "backend",
    "PgsqTask": "models",
    "PgsqTaskSlot": "models",
}

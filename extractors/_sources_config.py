"""
Private source configuration loader.

The real portal URLs, endpoints and site-specific identifiers are covered by
an NDA, so they are NOT stored in this repository. Each extractor only knows
its anonymised alias (e.g. ``sa_source_01``) and asks this module for the
settings it needs at runtime.

Lookup order for ``get_source_setting(alias, key)``:

  1. Environment variable ``<ALIAS>_<KEY>`` in upper case
     e.g.  SA_SOURCE_01_API_URL
  2. ``TENDER_SOURCES_JSON`` environment variable holding the full JSON
     config as a string (handy for Databricks / CI secrets)
  3. JSON file at ``TENDER_SOURCES_CONFIG`` (path), defaulting to
     ``config/sources.local.json`` at the project root (git-ignored)

See ``config/sources.example.json`` for the expected structure.
"""

import json
import os
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_FILE = PROJECT_ROOT / "config" / "sources.local.json"

_MISSING = object()


class SourceConfigError(RuntimeError):
    """Raised when a required private source setting is not configured."""


@lru_cache(maxsize=1)
def _load_config():
    raw_json = os.environ.get("TENDER_SOURCES_JSON", "").strip()
    if raw_json:
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise SourceConfigError(
                f"TENDER_SOURCES_JSON is not valid JSON: {exc}"
            ) from exc
        return data if isinstance(data, dict) else {}

    config_path = Path(
        os.environ.get("TENDER_SOURCES_CONFIG", "") or DEFAULT_CONFIG_FILE
    )
    if not config_path.exists():
        return {}

    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SourceConfigError(
            f"Could not read source config {config_path}: {exc}"
        ) from exc
    return data if isinstance(data, dict) else {}


def get_source_setting(alias, key, default=_MISSING):
    """Return one private setting for a source alias."""
    env_name = f"{alias}_{key}".upper()
    env_value = os.environ.get(env_name, "").strip()
    if env_value:
        return env_value

    value = (_load_config().get(alias) or {}).get(key)
    if value not in (None, ""):
        return value

    if default is not _MISSING:
        return default

    raise SourceConfigError(
        f"Missing private setting '{key}' for source '{alias}'. "
        f"Set {env_name}, or add it to config/sources.local.json "
        f"(copy config/sources.example.json)."
    )

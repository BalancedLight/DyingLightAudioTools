from __future__ import annotations

import json
from pathlib import Path

from dyingaudio.models import AudioEntry


def load_manifest(path: str | Path) -> list[AudioEntry]:
    manifest_path = Path(path).resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_entries = payload.get("entries", [])
    entries: list[AudioEntry] = []

    for item in raw_entries:
        item_path = Path(item["path"])
        if not item_path.is_absolute():
            item_path = (manifest_path.parent / item_path).resolve()

        entries.append(
            AudioEntry(
                entry_name=item["name"],
                source_path=str(item_path),
                source_mode="fsb",
                fsb_path=str(item_path),
                entry_type=int(item.get("type", 2)),
                sample_count=int(item.get("sampleCount", 0)),
                duration_ms=int(item.get("durationMs", 0)),
                reserved=int(item.get("reserved", 0)),
                notes="Imported from manifest.",
            )
        )

    return entries


def write_manifest(path: str | Path, entries: list[AudioEntry]) -> Path:
    manifest_path = Path(path).resolve()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    serialized_entries = []
    for entry in entries:
        fsb_path = entry.resolved_fsb_path()
        if fsb_path is None:
            continue

        try:
            relative = fsb_path.resolve().relative_to(manifest_path.parent.resolve())
            stored_path = str(relative)
        except ValueError:
            stored_path = str(fsb_path.resolve())

        serialized_entries.append(
            {
                "name": entry.entry_name,
                "path": stored_path,
                "type": int(entry.entry_type),
                "sampleCount": int(entry.sample_count),
                "durationMs": int(entry.duration_ms),
                "reserved": int(entry.reserved),
            }
        )

    manifest_path.write_text(json.dumps({"entries": serialized_entries}, indent=2), encoding="utf-8")
    return manifest_path

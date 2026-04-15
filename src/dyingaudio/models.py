from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def entry_type_from_channel_count(channel_count: int) -> int:
    return channel_count if channel_count > 0 else 2


def format_entry_type(entry_type: int) -> str:
    if entry_type == 1:
        return "Mono"
    if entry_type == 2:
        return "Stereo"
    if entry_type > 0:
        return f"{entry_type} channels"
    return str(entry_type)


@dataclass(slots=True)
class AudioEntry:
    entry_name: str
    source_path: str = ""
    source_mode: str = "raw"
    fsb_path: str = ""
    entry_type: int = 2
    sample_count: int = 0
    duration_ms: int = 0
    reserved: int = 0
    notes: str = ""

    def resolved_source_path(self) -> Path | None:
        if not self.source_path:
            return None
        return Path(self.source_path)

    def resolved_fsb_path(self) -> Path | None:
        candidate = self.fsb_path or self.source_path
        if not candidate:
            return None
        return Path(candidate)

    def display_source(self) -> str:
        if self.source_mode == "fsb":
            return self.fsb_path or self.source_path
        return self.source_path

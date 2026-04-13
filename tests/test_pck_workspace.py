from __future__ import annotations

import struct
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dyingaudio.audio_info import AudioMetadata  # noqa: E402
from dyingaudio.core.pck_workspace import (  # noqa: E402
    AKPK_SOURCE_TYPE,
    load_pck_pack_rows,
    parse_pck_header,
    scan_pck_root,
)


def _riff_bytes(frame_count: int = 4_800, sample_rate: int = 48_000) -> bytes:
    channels = 1
    bits_per_sample = 16
    block_align = channels * (bits_per_sample // 8)
    byte_rate = sample_rate * block_align
    pcm_bytes = b"\x00" * (frame_count * block_align)
    fmt_chunk = b"fmt " + struct.pack("<IHHIIHH", 16, 1, channels, sample_rate, byte_rate, block_align, bits_per_sample)
    data_chunk = b"data" + struct.pack("<I", len(pcm_bytes)) + pcm_bytes
    riff_size = 4 + len(fmt_chunk) + len(data_chunk)
    return b"RIFF" + struct.pack("<I", riff_size) + b"WAVE" + fmt_chunk + data_chunk


def _chunk(chunk_id: bytes, payload: bytes) -> bytes:
    return chunk_id + struct.pack("<I", len(payload)) + payload


def _build_language_sector(languages: dict[int, str] | None = None) -> bytes:
    values = languages or {0: "sfx"}
    strings = bytearray()
    entries = bytearray()
    string_offset = 4 + (len(values) * 8)
    for language_id, name in values.items():
        encoded = name.encode("utf-16-le") + b"\x00\x00"
        entries.extend(struct.pack("<II", string_offset, language_id))
        strings.extend(encoded)
        string_offset += len(encoded)
    return struct.pack("<I", len(values)) + entries + strings


def _build_bank_with_didx(bank_id: int, media_items: list[tuple[int, bytes]]) -> bytes:
    data_payload = bytearray()
    didx_payload = bytearray()
    relative_offset = 0
    for media_id, media_bytes in media_items:
        didx_payload.extend(struct.pack("<III", media_id, relative_offset, len(media_bytes)))
        data_payload.extend(media_bytes)
        relative_offset += len(media_bytes)
    return (
        _chunk(b"BKHD", struct.pack("<II", 0x86, bank_id))
        + _chunk(b"DIDX", bytes(didx_payload))
        + _chunk(b"DATA", bytes(data_payload))
    )


def _build_bank_with_hirc(bank_id: int, objects: list[tuple[int, int, bytes]]) -> bytes:
    hirc_payload = bytearray(struct.pack("<I", len(objects)))
    for object_type, object_id, payload in objects:
        hirc_payload.extend(struct.pack("<B", object_type))
        hirc_payload.extend(struct.pack("<I", 4 + len(payload)))
        hirc_payload.extend(struct.pack("<I", object_id))
        hirc_payload.extend(payload)
    return _chunk(b"BKHD", struct.pack("<II", 0x86, bank_id)) + _chunk(b"HIRC", bytes(hirc_payload))


def _build_pck(
    path: Path,
    *,
    bank_items: list[tuple[int, bytes]] | None = None,
    sound_items: list[tuple[int, bytes]] | None = None,
    external_items: list[tuple[int, bytes]] | None = None,
    languages: dict[int, str] | None = None,
) -> Path:
    bank_items = list(bank_items or [])
    sound_items = list(sound_items or [])
    external_items = list(external_items or [])
    language_sector = _build_language_sector(languages)
    bank_sector_size = 4 + (len(bank_items) * 20)
    sound_sector_size = 4 + (len(sound_items) * 20)
    external_sector_size = 4 + (len(external_items) * 24)
    header_size = 20 + len(language_sector) + bank_sector_size + sound_sector_size + external_sector_size

    current_offset = 8 + header_size
    bank_sector = bytearray(struct.pack("<I", len(bank_items)))
    sound_sector = bytearray(struct.pack("<I", len(sound_items)))
    external_sector = bytearray(struct.pack("<I", len(external_items)))
    payload_parts: list[bytes] = []

    for file_id, payload in bank_items:
        bank_sector.extend(struct.pack("<IIIII", file_id, 1, len(payload), current_offset, 0))
        payload_parts.append(payload)
        current_offset += len(payload)
    for file_id, payload in sound_items:
        sound_sector.extend(struct.pack("<IIIII", file_id, 1, len(payload), current_offset, 0))
        payload_parts.append(payload)
        current_offset += len(payload)
    for file_id, payload in external_items:
        external_sector.extend(struct.pack("<QIIII", file_id, 1, len(payload), current_offset, 0))
        payload_parts.append(payload)
        current_offset += len(payload)

    header = (
        struct.pack("<IIIII", 1, len(language_sector), len(bank_sector), len(sound_sector), len(external_sector))
        + language_sector
        + bytes(bank_sector)
        + bytes(sound_sector)
        + bytes(external_sector)
    )
    assert len(header) == header_size
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"AKPK" + struct.pack("<I", header_size) + header + b"".join(payload_parts))
    return path


def _fake_metadata(path: str | Path) -> AudioMetadata:
    name = Path(path).stem
    seed = sum(ord(character) for character in name) % 1000
    return AudioMetadata(duration_ms=1_000 + seed, sample_count_48k=48_000 + seed)


class PckWorkspaceTests(unittest.TestCase):
    def test_parse_pck_header_handles_sound_external_bank_and_musicgame_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            sound_path = _build_pck(root / "Music1.pck", sound_items=[(101, _riff_bytes())])
            musicgame_path = _build_pck(root / "MusicGame" / "MusicGame1.pck", sound_items=[(202, _riff_bytes())])
            external_path = _build_pck(root / "English(US)" / "1001.pck", external_items=[(2**40 + 123, _riff_bytes())])
            bank_path = _build_pck(root / "Banks0.pck", bank_items=[(303, _build_bank_with_didx(303, [(401, _riff_bytes())]))])

            sound_header = parse_pck_header(sound_path)
            self.assertEqual(len(sound_header.sound_entries), 1)
            self.assertEqual(len(sound_header.bank_entries), 0)
            self.assertEqual(len(sound_header.external_entries), 0)

            musicgame_header = parse_pck_header(musicgame_path)
            self.assertEqual(len(musicgame_header.sound_entries), 1)
            self.assertEqual(len(musicgame_header.bank_entries), 0)

            external_header = parse_pck_header(external_path)
            self.assertEqual(len(external_header.external_entries), 1)
            self.assertGreater(external_header.external_entries[0].file_id, 2**32)

            bank_header = parse_pck_header(bank_path)
            self.assertEqual(len(bank_header.bank_entries), 1)
            self.assertEqual(len(bank_header.sound_entries), 0)

    def test_load_pck_pack_rows_builds_direct_sound_and_external_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "AudioAssets"
            cache_root = Path(temp_dir) / "cache"
            _build_pck(root / "Music1.pck", sound_items=[(101, _riff_bytes())])
            _build_pck(root / "English(US)" / "1001.pck", external_items=[(2**40 + 123, _riff_bytes())])

            with patch("dyingaudio.core.pck_workspace.probe_audio_metadata", side_effect=_fake_metadata):
                index = scan_pck_root(AKPK_SOURCE_TYPE, root, cache_root, lambda _message: None)
                sound_descriptor = next(item for item in index.packs if item.relative_path == "Music1.pck")
                sound_rows = load_pck_pack_rows(index, sound_descriptor, lambda _message: None)
                self.assertEqual([row.row_kind for row in sound_rows.rows], ["direct_sound"])
                self.assertEqual(sound_rows.rows[0].file_id, 101)
                self.assertGreater(sound_rows.rows[0].duration_ms, 0)

                external_descriptor = next(item for item in index.packs if item.relative_path == "English(US)/1001.pck")
                external_rows = load_pck_pack_rows(index, external_descriptor, lambda _message: None)
                self.assertEqual([row.row_kind for row in external_rows.rows], ["external"])
                self.assertGreater(external_rows.rows[0].file_id, 2**32)

    def test_load_pck_pack_rows_builds_embedded_bank_media_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "AudioAssets"
            cache_root = Path(temp_dir) / "cache"
            _build_pck(
                root / "Banks0.pck",
                bank_items=[(303, _build_bank_with_didx(303, [(401, _riff_bytes()), (402, _riff_bytes())]))],
            )

            with patch("dyingaudio.core.pck_workspace.probe_audio_metadata", side_effect=_fake_metadata):
                index = scan_pck_root(AKPK_SOURCE_TYPE, root, cache_root, lambda _message: None)
                descriptor = next(item for item in index.packs if item.relative_path == "Banks0.pck")
                pack_rows = load_pck_pack_rows(index, descriptor, lambda _message: None)
                self.assertEqual([row.row_kind for row in pack_rows.rows], ["embedded_bank_media", "embedded_bank_media"])
                self.assertEqual([row.file_id for row in pack_rows.rows], [401, 402])

    def test_load_pck_pack_rows_links_hirc_only_banks_to_direct_sound(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "AudioAssets"
            cache_root = Path(temp_dir) / "cache"
            _build_pck(root / "Music1.pck", sound_items=[(111, _riff_bytes())])
            object_payload = b"\x00" * 5 + struct.pack("<I", 111)
            _build_pck(root / "Banks0.pck", bank_items=[(500, _build_bank_with_hirc(500, [(2, 9001, object_payload)]))])

            with patch("dyingaudio.core.pck_workspace.probe_audio_metadata", side_effect=_fake_metadata):
                index = scan_pck_root(AKPK_SOURCE_TYPE, root, cache_root, lambda _message: None)
                descriptor = next(item for item in index.packs if item.relative_path == "Banks0.pck")
                pack_rows = load_pck_pack_rows(index, descriptor, lambda _message: None)
                self.assertEqual(len(pack_rows.rows), 1)
                self.assertEqual(pack_rows.rows[0].row_kind, "linked_bank_media")
                self.assertEqual(pack_rows.rows[0].source_pack, "Music1.pck")
                self.assertEqual(pack_rows.rows[0].file_id, 111)

    def test_load_pck_pack_rows_reports_unresolved_hirc_banks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "AudioAssets"
            cache_root = Path(temp_dir) / "cache"
            object_payload = b"\x00" * 5 + struct.pack("<I", 999)
            _build_pck(root / "Banks0.pck", bank_items=[(500, _build_bank_with_hirc(500, [(2, 9001, object_payload)]))])

            with patch("dyingaudio.core.pck_workspace.probe_audio_metadata", side_effect=_fake_metadata):
                index = scan_pck_root(AKPK_SOURCE_TYPE, root, cache_root, lambda _message: None)
                descriptor = next(item for item in index.packs if item.relative_path == "Banks0.pck")
                pack_rows = load_pck_pack_rows(index, descriptor, lambda _message: None)
                self.assertEqual(pack_rows.rows, [])
                self.assertTrue(pack_rows.unresolved)
                self.assertIn("no media ids resolved", pack_rows.unresolved[0].note.lower())

    def test_load_pck_pack_rows_collapses_duplicate_linked_targets_and_aggregates_origins(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "AudioAssets"
            cache_root = Path(temp_dir) / "cache"
            _build_pck(root / "Music1.pck", sound_items=[(111, _riff_bytes())])
            object_payload = b"\x00" * 5 + struct.pack("<I", 111)
            _build_pck(
                root / "Banks0.pck",
                bank_items=[
                    (
                        500,
                        _build_bank_with_hirc(
                            500,
                            [
                                (2, 9001, object_payload),
                                (2, 9002, object_payload),
                            ],
                        ),
                    )
                ],
            )

            with patch("dyingaudio.core.pck_workspace.probe_audio_metadata", side_effect=_fake_metadata):
                index = scan_pck_root(AKPK_SOURCE_TYPE, root, cache_root, lambda _message: None)
                descriptor = next(item for item in index.packs if item.relative_path == "Banks0.pck")
                pack_rows = load_pck_pack_rows(index, descriptor, lambda _message: None)
                self.assertEqual(len(pack_rows.rows), 1)
                self.assertEqual(pack_rows.rows[0].row_kind, "linked_bank_media")
                self.assertEqual(len(pack_rows.rows[0].origins), 2)


if __name__ == "__main__":
    unittest.main()

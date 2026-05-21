from __future__ import annotations

import csv
import struct
from collections.abc import Iterable
from pathlib import Path


TextCatalog = dict[str, str]


def _decode_script_bytes(data: bytes) -> str:
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16")
    for encoding in ("utf-8-sig", "cp1250", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _strip_comments(text: str) -> str:
    output: list[str] = []
    index = 0
    in_string = False
    while index < len(text):
        character = text[index]
        if in_string:
            output.append(character)
            if character == "\\" and index + 1 < len(text):
                output.append(text[index + 1])
                index += 2
                continue
            if character == '"':
                in_string = False
            index += 1
            continue

        if character == '"':
            in_string = True
            output.append(character)
            index += 1
            continue
        if character == "/" and index + 1 < len(text) and text[index + 1] == "/":
            output.extend("  ")
            index += 2
            while index < len(text) and text[index] not in "\r\n":
                output.append(" ")
                index += 1
            continue
        if character == "/" and index + 1 < len(text) and text[index + 1] == "*":
            output.extend("  ")
            index += 2
            while index + 1 < len(text) and not (text[index] == "*" and text[index + 1] == "/"):
                output.append(text[index] if text[index] in "\r\n" else " ")
                index += 1
            if index + 1 < len(text):
                output.extend("  ")
                index += 2
            continue
        output.append(character)
        index += 1
    return "".join(output)


def _skip_ws(text: str, index: int) -> int:
    while index < len(text) and text[index].isspace():
        index += 1
    return index


def _parse_hex_escape(text: str, index: int, size: int) -> tuple[str, int]:
    payload = text[index : index + size]
    if len(payload) != size or any(character not in "0123456789abcdefABCDEF" for character in payload):
        return "", index
    return chr(int(payload, 16)), index + size


def _parse_string(text: str, index: int) -> tuple[str, int] | None:
    index = _skip_ws(text, index)
    if index >= len(text) or text[index] != '"':
        return None
    index += 1
    output: list[str] = []
    while index < len(text):
        character = text[index]
        if character == '"':
            return "".join(output), index + 1
        if character == "\\" and index + 1 < len(text):
            escape = text[index + 1]
            if escape == "n":
                output.append("\n")
                index += 2
            elif escape == "r":
                output.append("\r")
                index += 2
            elif escape == "t":
                output.append("\t")
                index += 2
            elif escape == "0":
                output.append("\0")
                index += 2
            elif escape in {'"', "\\"}:
                output.append(escape)
                index += 2
            elif escape == "x":
                character, index = _parse_hex_escape(text, index + 2, 2)
                output.append(character)
            elif escape == "u":
                character, index = _parse_hex_escape(text, index + 2, 4)
                output.append(character)
            elif escape == "U":
                character, index = _parse_hex_escape(text, index + 2, 8)
                output.append(character)
            else:
                output.append("\\")
                output.append(escape)
                index += 2
            continue
        output.append(character)
        index += 1
    return None


def _skip_call_tail(text: str, index: int) -> int:
    depth = 1
    while index < len(text):
        character = text[index]
        if character == '"':
            parsed = _parse_string(text, index)
            if parsed is None:
                return len(text)
            _, index = parsed
            continue
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return len(text)


def _starts_word(text: str, index: int, word: str) -> bool:
    identifier = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"
    if not text.startswith(word, index):
        return False
    if index > 0 and (text[index - 1] in identifier or text[index - 1] == "!"):
        return False
    end = index + len(word)
    return end >= len(text) or text[end] not in identifier


def _parse_one_string_call(text: str, index: int) -> tuple[str, int] | None:
    index = _skip_ws(text, index)
    if index >= len(text) or text[index] != "(":
        return None
    parsed = _parse_string(text, index + 1)
    if parsed is None:
        return None
    value, index = parsed
    return value, _skip_call_tail(text, index)


def _read_scr_rows(path: Path, *, use_includes: bool = True, stack: list[Path] | None = None) -> list[tuple[str, str]]:
    stack = [] if stack is None else stack
    resolved = path.resolve()
    if resolved in stack:
        return []

    text = _strip_comments(_decode_script_bytes(path.read_bytes()))
    rows: list[tuple[str, str]] = []
    index = 0
    stack.append(resolved)
    while index < len(text):
        if text[index] == '"':
            parsed = _parse_string(text, index)
            if parsed is None:
                break
            _, index = parsed
            continue
        if _starts_word(text, index, "!include"):
            parsed = _parse_one_string_call(text, index + len("!include"))
            if parsed is None:
                index += len("!include")
                continue
            include_name, index = parsed
            include_path = (path.parent / include_name).resolve()
            if use_includes and include_path.exists():
                rows.extend(_read_scr_rows(include_path, use_includes=use_includes, stack=stack))
            continue
        if _starts_word(text, index, "String"):
            call_start = _skip_ws(text, index + len("String"))
            if call_start >= len(text) or text[call_start] != "(":
                index += len("String")
                continue
            key_parsed = _parse_string(text, call_start + 1)
            if key_parsed is None:
                index += len("String")
                continue
            key, after_key = key_parsed
            after_key = _skip_ws(text, after_key)
            if after_key >= len(text) or text[after_key] != ",":
                index = _skip_call_tail(text, after_key)
                continue
            value_parsed = _parse_string(text, after_key + 1)
            if value_parsed is None:
                index = _skip_call_tail(text, after_key)
                continue
            value, after_value = value_parsed
            rows.append((key, value))
            index = _skip_call_tail(text, after_value)
            continue
        index += 1
    stack.pop()
    return rows


def read_scr_texts(path: str | Path) -> TextCatalog:
    catalog: TextCatalog = {}
    for key, value in _read_scr_rows(Path(path).expanduser().resolve()):
        catalog[key.casefold()] = value
    return catalog


def read_bin_texts(path: str | Path) -> TextCatalog:
    data = Path(path).expanduser().resolve().read_bytes()
    if len(data) < 8:
        return {}
    _version, count = struct.unpack_from("<II", data, 0)
    offset = 8
    catalog: TextCatalog = {}
    for _index in range(count):
        if offset + 2 > len(data):
            break
        key_len = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        if offset + key_len > len(data):
            break
        key = data[offset : offset + key_len].decode("ascii", errors="replace")
        offset += key_len
        if offset + 2 > len(data):
            break
        value_units = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        value_size = value_units * 2
        if offset + value_size > len(data):
            break
        value = data[offset : offset + value_size].decode("utf-16le", errors="replace")
        offset += value_size
        catalog[key.casefold()] = value
    return catalog


def read_tsv_texts(path: str | Path) -> TextCatalog:
    catalog: TextCatalog = {}
    with Path(path).expanduser().resolve().open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if len(row) < 2:
                continue
            key = row[0].strip()
            value = row[1].strip()
            if not key or key.startswith("#"):
                continue
            if key.casefold() in {"id", "key", "string_id", "speech_id"}:
                continue
            catalog[key.casefold()] = value
    return catalog


def read_text_catalog(path: str | Path) -> TextCatalog:
    resolved = Path(path).expanduser().resolve()
    suffix = resolved.suffix.lower()
    if suffix == ".scr":
        return read_scr_texts(resolved)
    if suffix == ".bin":
        return read_bin_texts(resolved)
    if suffix in {".tsv", ".txt", ".csv"}:
        return read_tsv_texts(resolved)
    return {}


def _iter_text_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    for suffix in ("*.scr", "*.bin", "*.tsv", "*.txt", "*.csv"):
        yield from path.rglob(suffix)


def _coerce_paths(sources: str | Path | Iterable[str | Path] | None) -> list[Path]:
    if sources is None:
        return []
    if isinstance(sources, (str, Path)):
        raw_sources: Iterable[str | Path] = [sources]
    else:
        raw_sources = sources
    paths: list[Path] = []
    for source in raw_sources:
        if source is None:
            continue
        text = str(source).strip()
        if not text:
            continue
        paths.append(Path(text).expanduser())
    return paths


def auto_text_candidates(root: str | Path | None) -> list[Path]:
    if root is None:
        return []
    start = Path(root).expanduser().resolve()
    if start.is_file():
        start = start.parent

    candidates: list[Path] = []
    for parent in (start, *start.parents):
        candidates.append(parent / "data" / "texts_steam_workshop.scr")
        candidates.append(parent / "texts_steam_workshop.scr")
    return [candidate for candidate in candidates if candidate.exists()]


def load_text_catalog(
    sources: str | Path | Iterable[str | Path] | None = None,
    *,
    auto_root: str | Path | None = None,
) -> TextCatalog:
    catalog: TextCatalog = {}
    paths = auto_text_candidates(auto_root)
    paths.extend(_coerce_paths(sources))

    seen: set[str] = set()
    for source in paths:
        try:
            resolved = source.resolve()
        except OSError:
            continue
        key = str(resolved).casefold()
        if key in seen or not resolved.exists():
            continue
        seen.add(key)
        for text_file in _iter_text_files(resolved):
            try:
                catalog.update(read_text_catalog(text_file))
            except OSError:
                continue
    return catalog

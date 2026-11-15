#!/usr/bin/env python3
"""Back up the Piggy files that sit on hosts the team does not control (PIG-217).

Five file sets, stored under the keys the rehost stories (PIG-198, PIG-199)
will use, so that a rehost is a prefix copy:

    piggy-sol-gang/json/{mint}.json    <- r2.dev  old/metadata/{mint}.json
    piggy-sol-gang/images/{mint}.png   <- r2.dev  old/images/{mint}.png
    piggy-gang/json/{id}.json          <- r2.dev  metadata/{id}.json
    piggy-gang/images/{id}.png         <- S3      metadata/{id}.png
    piggy-gang/json-s3/{id}.json       <- S3      metadata/{id}.json

{mint} is every Piggy SOL Gang mint in config/seeds/piggy-sol-gang.mints.json;
{id} is every art id in the migration app's mint map (piggygang/migrate,
lib/mint-map.json — `mint_map_prod.json` in the old piggy-upgrades code).

The last set is not in the issue. The S3 bucket holds the original 2024 Piggy
Gang documents beside the images, and the Core collection account's own URI
points at one of them; the r2.dev documents are the corrected copies that every
live asset uses. Neither host can be listed, so a file nobody has a key for
cannot be found, let alone backed up.

    download   fetch whatever is not already in --dest (resumable, throttled)
    manifest   re-hash every file from disk, check it, write manifests/
    verify     check any copy of the backup against the committed manifests
    upload     copy --dest to an S3-compatible bucket and read every file back

Standard library only, except `upload`, which needs boto3. Run it with
`python3 -I` so Python imports nothing out of the working directory.

A file counts as downloaded only when its length equals the origin's
Content-Length *and* its MD5 equals the origin's ETag (both hosts publish the
MD5 of a single-part object as its ETag). The journal in --dest/state records
what the origin said about every key, so a re-run fetches only what is missing
and a 404 is a recorded answer rather than a silent gap.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import http.client
import json
import os
import random
import re
import signal
import ssl
import struct
import sys
import threading
import time
import zlib
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

R2 = "https://pub-b1a45763f0a64d8fa271f66f5514a561.r2.dev"
S3 = "https://piggysolgang.s3.amazonaws.com"

# Keys become file names and URL path segments, so they are validated rather
# than trusted: a base58 mint address, or the 20 hex digits of an art id.
MINT = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}\Z")
ART_ID = re.compile(r"[0-9a-f]{20}\Z")
MD5_HEX = re.compile(r"[0-9a-f]{32}\Z")

REPO = Path(__file__).resolve().parent.parent
DEFAULT_MINTS = REPO / "config" / "seeds" / "piggy-sol-gang.mints.json"
DEFAULT_MINT_MAP = REPO.parent / "migrate" / "lib" / "mint-map.json"
MANIFESTS = Path(__file__).resolve().parent / "manifests"

HEADERS = {
    # identity: the bytes on the wire must be the stored object, or neither the
    # Content-Length nor the ETag check below would mean anything.
    "Accept-Encoding": "identity",
    "Accept": "*/*",
    "User-Agent": "asset-backup/1 (python http.client)",
}


@dataclass(frozen=True)
class FileSet:
    name: str  # path of the set under the backup root, e.g. "piggy-gang/images"
    source: str  # URL template, {key} substituted
    ext: str
    keys: str  # which input list feeds it: "mints" | "art-ids"

    @property
    def host(self) -> str:
        return urlsplit(self.source).netloc

    @property
    def manifest(self) -> str:
        return self.name.replace("/", ".") + ".sha256"

    def url(self, key: str) -> str:
        return self.source.format(key=key)

    def relpath(self, key: str) -> str:
        return f"{self.name}/{key}.{self.ext}"


SETS = (
    FileSet("piggy-sol-gang/json", R2 + "/old/metadata/{key}.json", "json", "mints"),
    FileSet("piggy-gang/json", R2 + "/metadata/{key}.json", "json", "art-ids"),
    FileSet("piggy-sol-gang/images", R2 + "/old/images/{key}.png", "png", "mints"),
    FileSet("piggy-gang/images", S3 + "/metadata/{key}.png", "png", "art-ids"),
    FileSet("piggy-gang/json-s3", S3 + "/metadata/{key}.json", "json", "art-ids"),
)


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str) -> None:
    print(f"{now()} {message}", flush=True)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def load_keys(mints_path: Path, mint_map_path: Path) -> dict[str, list[str]]:
    """The two input lists, validated. Order is the files' own; duplicates are an error."""
    mints = json.loads(mints_path.read_text())
    mint_map = json.loads(mint_map_path.read_text())
    if not isinstance(mints, list) or not isinstance(mint_map, dict):
        raise SystemExit("expected a JSON array of mints and a JSON object mint -> art id")
    art_ids = list(mint_map.values())
    for label, keys, pattern in (("mint", mints, MINT), ("art id", art_ids, ART_ID)):
        bad = [k for k in keys if not isinstance(k, str) or not pattern.match(k)]
        if bad:
            raise SystemExit(f"{len(bad)} malformed {label}s, e.g. {bad[0]!r}")
        if len(set(keys)) != len(keys):
            raise SystemExit(f"duplicate {label}s in the input list")
    return {"mints": mints, "art-ids": art_ids}


class Journal:
    """Append-only record of what the origin answered for each key; the last line per key wins."""

    def __init__(self, path: Path, writable: bool = True):
        self.path = path
        self.latest: dict[str, dict] = {}
        self._lock = threading.Lock()
        if path.exists():
            with open(path) as handle:
                for number, line in enumerate(handle, 1):
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        # Only a line torn by a crash can be unparseable; the key
                        # it was for has no verdict and is simply fetched again.
                        log(f"{path.name}: ignoring unparseable line {number}")
                        continue
                    self.latest[record["key"]] = record
        torn = False
        if path.exists() and path.stat().st_size:
            with open(path, "rb") as handle:
                handle.seek(-1, os.SEEK_END)
                torn = handle.read(1) != b"\n"
        self._handle = open(path, "a", buffering=1) if writable else None
        if torn and self._handle:
            # A torn line has no newline, and append mode would glue the next
            # record onto the fragment and lose that one too. End the fragment.
            self._handle.write("\n")

    def record(self, record: dict) -> None:
        line = json.dumps(record, separators=(",", ":"), sort_keys=True)
        with self._lock:
            self._handle.write(line + "\n")
            self.latest[record["key"]] = record

    def close(self) -> None:
        if self._handle:
            self._handle.flush()
            os.fsync(self._handle.fileno())
            self._handle.close()


def journal_path(dest: Path, file_set: FileSet) -> Path:
    return dest / "state" / (file_set.name.replace("/", ".") + ".jsonl")


class Throttle:
    """Evenly spaced requests to one host, backing off when the host says so.

    No burst allowance: r2.dev's limit is undocumented, so the only safe shape
    is a steady rate well under it. A 429 or 5xx pauses every worker on the host
    and halves the rate; the rate creeps back only after a long clean streak and
    never past the configured ceiling.
    """

    def __init__(self, rps: float):
        self.base = 1.0 / rps
        self.interval = self.base
        self.penalties = 0
        self._next = time.monotonic()
        self._paused_until = 0.0
        self._streak = 0
        self._lock = threading.Lock()

    def _reserve(self) -> float:
        slot = max(self._next, time.monotonic())
        self._next = slot + self.interval
        return slot

    def wait(self) -> None:
        with self._lock:
            slot = self._reserve()
        while True:
            delay = slot - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            with self._lock:
                # A back-off imposed while this worker slept outranks the slot
                # it had already reserved: queue again, behind the pause.
                if self._paused_until <= slot:
                    return
                slot = self._reserve()

    def penalize(self, pause: float) -> None:
        with self._lock:
            self.penalties += 1
            self._streak = 0
            self.interval = min(self.interval * 2, 2.0)
            self._paused_until = max(self._paused_until, time.monotonic() + pause)
            self._next = max(self._next, self._paused_until)

    def ok(self) -> None:
        with self._lock:
            self._streak += 1
            if self._streak >= 500 and self.interval > self.base:
                self._streak = 0
                self.interval = max(self.base, self.interval * 0.8)

    @property
    def rps(self) -> float:
        return 1.0 / self.interval


class Retry(Exception):
    """The response could not be trusted; fetch the file again."""


_thread = threading.local()
_tls = ssl.create_default_context()
_stop = threading.Event()


def _connection(host: str) -> http.client.HTTPSConnection:
    pool = _thread.__dict__.setdefault("pool", {})
    if host not in pool:
        pool[host] = http.client.HTTPSConnection(host, timeout=60, context=_tls)
    return pool[host]


def _drop(host: str) -> None:
    connection = _thread.__dict__.get("pool", {}).pop(host, None)
    if connection is not None:
        try:
            connection.close()
        except OSError:
            pass


def fetch(file_set: FileSet, key: str, dest: Path, throttle: Throttle, attempts: int = 6) -> dict:
    """Download one file and return its journal record. Never raises for an origin error."""
    url = urlsplit(file_set.url(key))
    final = dest / "files" / file_set.relpath(key)
    part = final.with_name(final.name + ".part")
    record = {"key": key, "attempts": 0}
    error = "not attempted"
    for attempt in range(1, attempts + 1):
        if _stop.is_set():
            error = "interrupted"
            break
        throttle.wait()
        record["attempts"] = attempt
        pause = min(60.0, 2.0**attempt) * random.uniform(0.5, 1.0)
        try:
            connection = _connection(url.netloc)
            connection.request("GET", url.path, headers=HEADERS)
            response = connection.getresponse()
            status = response.status
            record["http"] = status
            if status == 200:
                sha256, md5, size = hashlib.sha256(), hashlib.md5(), 0
                with open(part, "wb") as out:
                    while chunk := response.read(1 << 20):
                        out.write(chunk)
                        sha256.update(chunk)
                        md5.update(chunk)
                        size += len(chunk)
                encoding = (response.getheader("Content-Encoding") or "identity").lower()
                if encoding != "identity":
                    raise Retry(f"Content-Encoding {encoding}: not the stored bytes")
                # Both proofs or the file is not kept: a response that cannot be
                # checked proves nothing, whatever it contains. http.client
                # returns a short body without complaint, so the length has to
                # be compared by hand.
                declared = response.getheader("Content-Length") or ""
                if not declared.isdigit():
                    raise Retry("the origin sent no Content-Length")
                if int(declared) != size:
                    raise Retry(f"got {size} of {declared} bytes")
                etag = (response.getheader("ETag") or "").removeprefix("W/").strip('"')
                if not MD5_HEX.match(etag):
                    raise Retry(f"the origin's ETag {etag!r} is not an MD5")
                if etag != md5.hexdigest():
                    raise Retry(f"MD5 {md5.hexdigest()} is not the origin's ETag {etag}")
                if size == 0:
                    raise Retry("empty body")
                os.replace(part, final)
                throttle.ok()
                record.update(
                    status="ok",
                    size=size,
                    sha256=sha256.hexdigest(),
                    md5=md5.hexdigest(),
                    etag=etag,
                    # The two proofs above, on the record: `manifest` refuses a
                    # line whose journal entry does not carry both.
                    etag_md5_match=True,
                    length_checked=True,
                    last_modified=response.getheader("Last-Modified"),
                    content_type=response.getheader("Content-Type"),
                    version_id=response.getheader("x-amz-version-id"),
                    fetched_at=now(),
                )
                return record
            response.read()  # drain, so the connection can be reused
            error = f"HTTP {status}"
            if status == 404:
                record.update(status="missing", fetched_at=now())
                return record
            if status == 403:
                # S3 answers 403 for a key that does not exist when the bucket
                # does not allow listing, and also when it has gone private.
                # Recorded apart from "missing" so a re-run asks again.
                record.update(status="denied", fetched_at=now())
                return record
            if status == 429 or status >= 500:
                retry_after = response.getheader("Retry-After") or ""
                pause = max(pause, float(retry_after)) if retry_after.isdigit() else max(pause, 10.0)
                throttle.penalize(pause)
            elif attempt >= 2:
                break  # an unexpected 3xx/4xx does not improve with repetition
        except (OSError, http.client.HTTPException, Retry) as exception:
            error = f"{type(exception).__name__}: {exception}"
            _drop(url.netloc)
        try:
            part.unlink()
        except FileNotFoundError:
            pass
        if attempt < attempts and not _stop.is_set():
            time.sleep(pause)
    record.update(status="failed", error=error, fetched_at=now())
    return record


def download(args: argparse.Namespace) -> int:
    dest: Path = args.dest
    keys = load_keys(args.mints, args.mint_map)
    selected = [s for s in SETS if not args.only or s.name in args.only]
    if args.only and len(selected) != len(args.only):
        raise SystemExit(f"--only takes any of: {', '.join(s.name for s in SETS)}")
    (dest / "state").mkdir(parents=True, exist_ok=True)

    throttles = {urlsplit(R2).netloc: Throttle(args.r2_rps), urlsplit(S3).netloc: Throttle(args.s3_rps)}
    workers = {urlsplit(R2).netloc: args.r2_workers, urlsplit(S3).netloc: args.s3_workers}
    journals = {s.name: Journal(journal_path(dest, s)) for s in selected}
    tally = {s.name: dict(total=0, have=0, ok=0, missing=0, denied=0, failed=0, bytes=0) for s in selected}
    tally_lock = threading.Lock()

    queues: dict[str, list[tuple[FileSet, str]]] = {host: [] for host in throttles}
    for file_set in selected:
        (dest / "files" / file_set.name).mkdir(parents=True, exist_ok=True)
        wanted = keys[file_set.keys][: args.limit] if args.limit else keys[file_set.keys]
        counts = tally[file_set.name]
        counts["total"] = len(wanted)
        for key in wanted:
            previous = journals[file_set.name].latest.get(key)
            if previous and previous["status"] == "ok":
                path = dest / "files" / file_set.relpath(key)
                if path.exists() and path.stat().st_size == previous["size"]:
                    counts["have"] += 1
                    continue
            elif previous and previous["status"] == "missing" and not args.retry_missing:
                counts["missing"] += 1
                continue
            queues[file_set.host].append((file_set, key))

    def work(job: tuple[FileSet, str]) -> None:
        file_set, key = job
        if _stop.is_set():
            return
        record = fetch(file_set, key, dest, throttles[file_set.host])
        if record.get("error") == "interrupted":
            return
        journals[file_set.name].record(record)
        with tally_lock:
            counts = tally[file_set.name]
            counts[record["status"]] += 1
            counts["bytes"] += record.get("size", 0)

    def report() -> None:
        with tally_lock:
            for name, c in tally.items():
                done = c["have"] + c["ok"] + c["missing"] + c["denied"] + c["failed"]
                log(
                    f"{name:<22} {done:>6}/{c['total']:<6} new={c['ok']} have={c['have']} "
                    f"missing={c['missing']} denied={c['denied']} failed={c['failed']} "
                    f"fetched={c['bytes'] / 1e6:,.0f} MB"
                )
        for host, throttle in throttles.items():
            log(f"  {host}: {throttle.rps:.1f} req/s ceiling, {throttle.penalties} back-offs")

    for name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(name, lambda *_: _stop.set())
    log(f"queued: " + ", ".join(f"{host}={len(jobs)}" for host, jobs in queues.items()))

    pools = [ThreadPoolExecutor(max_workers=workers[host], thread_name_prefix=host) for host in queues]
    futures = [pool.submit(work, job) for pool, jobs in zip(pools, queues.values()) for job in jobs]
    try:
        waited = 0
        while not all(f.done() for f in futures) and not _stop.is_set():
            time.sleep(1)
            waited += 1
            if waited % args.report_every == 0:
                report()
        if _stop.is_set():
            log("stopping: letting in-flight files finish")
    finally:
        for pool in pools:
            pool.shutdown(wait=True, cancel_futures=True)
        for journal in journals.values():
            journal.close()
        os.sync()
    for future in futures:
        if future.done() and not future.cancelled() and future.exception():
            raise future.exception()
    report()
    unfinished = sum(c["failed"] + c["denied"] for c in tally.values())
    if _stop.is_set():
        log("interrupted; run again to resume")
        return 130
    if unfinished:
        log(f"{unfinished} files failed or were denied; run again to retry them")
        return 1
    log("download complete: every key has a verdict (ok, or a recorded 404)")
    return 0


PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
PNG_CHANNELS = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}
# Each Adam7 pass of an interlaced PNG: x offset, y offset, x step, y step.
ADAM7 = ((0, 0, 8, 8), (4, 0, 8, 8), (0, 4, 4, 8), (2, 0, 4, 4), (0, 2, 2, 4), (1, 0, 2, 2), (0, 1, 1, 2))


def png_pixel_bytes(width: int, height: int, bits_per_pixel: int, interlaced: bool) -> int:
    """What a PNG's pixel stream inflates to: per scanline, a filter byte and the packed pixels.

    An interlaced image is seven smaller images, and a pass that has no column
    or no row at this size is simply absent.
    """
    total = 0
    for x0, y0, dx, dy in ADAM7 if interlaced else ((0, 0, 1, 1),):
        columns, rows = -(-(width - x0) // dx), -(-(height - y0) // dy)
        if columns > 0 and rows > 0:
            total += rows * (1 + (columns * bits_per_pixel + 7) // 8)
    return total


def check_png(path: Path) -> tuple[str | None, dict]:
    """Walk a PNG without an image library: (what is wrong with it or None, its header).

    An ETag match proves the copy equals the origin's object, not that the
    object is a whole image — an upload cut short at the origin has a perfectly
    good ETag. So every chunk's CRC is checked, IEND must be the last byte, and
    the pixel stream must inflate to exactly the size the header implies.
    """
    facts: dict = {}
    with open(path, "rb") as handle:
        if handle.read(8) != PNG_SIGNATURE:
            return "not a PNG (bad signature)", facts
        inflater = zlib.decompressobj()
        pixels, ended, expected = 0, False, 0
        while head := handle.read(8):
            if ended:
                return "data after IEND", facts
            if len(head) < 8:
                return "truncated chunk header", facts
            length, kind = struct.unpack(">I4s", head)
            body, crc = handle.read(length), handle.read(4)
            if len(body) != length or len(crc) != 4:
                return f"truncated {kind.decode('latin-1')} chunk", facts
            if zlib.crc32(body, zlib.crc32(kind)) != int.from_bytes(crc, "big"):
                return f"bad CRC in {kind.decode('latin-1')} chunk", facts
            if not facts:
                if kind != b"IHDR" or length != 13:
                    return "first chunk is not IHDR", facts
                width, height, depth, color, _, _, interlace = struct.unpack(">IIBBBBB", body)
                if color not in PNG_CHANNELS or interlace not in (0, 1) or not width or not height:
                    return "invalid IHDR", facts
                facts = {"width": width, "height": height, "bit_depth": depth, "color_type": color}
                expected = png_pixel_bytes(width, height, PNG_CHANNELS[color] * depth, interlace == 1)
            elif kind == b"IDAT":
                try:
                    pixels += len(inflater.decompress(body))
                except zlib.error as exception:
                    return f"pixel data does not inflate: {exception}", facts
            elif kind == b"IEND":
                ended = True
    if not ended:
        return "no IEND chunk", facts
    if not inflater.eof:
        return "pixel data ends early", facts
    if pixels != expected:
        return f"pixel data is {pixels} bytes, the header implies {expected}", facts
    return None, facts


def check_json(path: Path) -> tuple[str | None, dict]:
    """(what is wrong or None, the URLs the document points at)."""
    try:
        document = json.loads(path.read_bytes())
    except ValueError as exception:  # UnicodeDecodeError is a ValueError
        return f"not valid JSON: {exception}", {}
    if not isinstance(document, dict):
        return "not a JSON object", {}
    references = [document[field] for field in ("image", "animation_url") if isinstance(document.get(field), str)]
    properties = document.get("properties")
    for entry in (properties.get("files") if isinstance(properties, dict) else None) or []:
        if isinstance(entry, dict) and isinstance(entry.get("uri"), str):
            references.append(entry["uri"])
    return None, {"image": document.get("image"), "references": references}


def inspect(job: tuple[str, str]) -> tuple[str, int, str, str | None, dict]:
    """Hash one file from disk and check its content. Runs in a worker process."""
    root, relpath = job
    path = Path(root) / relpath
    problem, facts = check_png(path) if relpath.endswith(".png") else check_json(path)
    return relpath, path.stat().st_size, sha256_of(path), problem, facts


def inspect_all(root: Path, relpaths: list[str], label: str) -> dict[str, tuple[int, str, str | None, dict]]:
    results = {}
    started = time.monotonic()
    with ProcessPoolExecutor() as pool:
        jobs = ((str(root), relpath) for relpath in relpaths)
        for count, (relpath, *rest) in enumerate(pool.map(inspect, jobs, chunksize=64), 1):
            results[relpath] = tuple(rest)
            if count % 5000 == 0:
                log(f"{label}: {count}/{len(relpaths)} files")
    log(f"{label}: {len(relpaths)} files in {time.monotonic() - started:.0f} s")
    return results


def resolver():
    """Maps a source URL back to the backup path that holds it, or None."""
    patterns = []
    for file_set in SETS:
        prefix, suffix = file_set.source.split("{key}")
        key = MINT if file_set.keys == "mints" else ART_ID
        pattern = re.compile(re.escape(prefix) + "(" + key.pattern.removesuffix(r"\Z") + ")" + re.escape(suffix) + r"\Z")
        patterns.append((pattern, file_set))

    def resolve(url: str) -> str | None:
        for pattern, file_set in patterns:
            if match := pattern.match(url):
                return file_set.relpath(match.group(1))
        return None

    return resolve


def manifest(args: argparse.Namespace) -> int:
    """Write the sha256 manifests from the bytes on disk — never from the journal alone."""
    dest: Path = args.dest
    root = dest / "files"
    keys = load_keys(args.mints, args.mint_map)
    mint_of = {art_id: mint for mint, art_id in json.loads(args.mint_map.read_text()).items()}
    journals = {s.name: Journal(journal_path(dest, s), writable=False) for s in SETS}

    present: dict[str, list[str]] = {s.name: [] for s in SETS}
    absent: list[dict] = []
    problems: list[str] = []
    for file_set in SETS:
        expected = set(keys[file_set.keys])
        for key in keys[file_set.keys]:
            record = journals[file_set.name].latest.get(key, {})
            if record.get("status") == "ok":
                present[file_set.name].append(file_set.relpath(key))
            else:
                absent.append(
                    {
                        "set": file_set.name,
                        "key": key,
                        "url": file_set.url(key),
                        "verdict": record.get("status", "never fetched"),
                        "http": record.get("http"),
                    }
                )
        on_disk = {p.name for p in (root / file_set.name).iterdir()} if (root / file_set.name).is_dir() else set()
        strays = sorted(on_disk - {f"{key}.{file_set.ext}" for key in expected})
        if strays:
            problems.append(f"{file_set.name}: {len(strays)} files on disk are not in the key list, e.g. {strays[0]}")

    everything = [relpath for paths in present.values() for relpath in paths]
    gone = [relpath for relpath in everything if not (root / relpath).is_file()]
    if gone:
        raise SystemExit(f"{len(gone)} files the journal calls downloaded are not on disk, e.g. {gone[0]}")
    inspected = inspect_all(root, everything, "hash + content check")

    resolve = resolver()
    args.out.mkdir(parents=True, exist_ok=True)
    summary_sets = {}
    for file_set in SETS:
        journal = journals[file_set.name].latest
        lines, sizes, modified, fetched, shapes = [], [], [], [], {}
        md5_proven = content_bad = wrong_image = map_agrees = map_disagrees = 0
        uncovered: dict[str, int] = {}
        samples: list[str] = []
        for relpath in sorted(present[file_set.name]):
            key = relpath.rsplit("/", 1)[1].rsplit(".", 1)[0]
            record = journal[key]
            size, digest, problem, facts = inspected[relpath]
            if (size, digest) != (record["size"], record["sha256"]):
                problems.append(f"{relpath}: on disk {size} B {digest}, downloaded as {record['size']} B {record['sha256']}")
                continue
            if record.get("etag_md5_match") is not True or record.get("length_checked") is not True:
                problems.append(f"{relpath}: downloaded without both origin proofs (Content-Length and an MD5 ETag)")
                continue
            lines.append(f"{digest}  {relpath}\n")
            sizes.append(size)
            md5_proven += record["etag_md5_match"] is True
            fetched.append(record["fetched_at"])
            if record.get("last_modified"):
                modified.append(parsedate_to_datetime(record["last_modified"]).strftime("%Y-%m-%dT%H:%M:%SZ"))
            if problem:
                # It stays in the manifest, being a faithful copy of what the
                # origin serves, but a broken file is never a clean run.
                content_bad += 1
                problems.append(f"{relpath}: {problem}")
                continue
            if file_set.ext == "png":
                shape = f"{facts['width']}x{facts['height']}"
                shapes[shape] = shapes.get(shape, 0) + 1
                continue
            # A document is only backed up if everything it points at is too.
            image_set = next(s for s in SETS if s.keys == file_set.keys and s.ext == "png")
            if facts["image"] != image_set.url(key):
                wrong_image += 1
                samples.append(f"{relpath}: image is {facts['image']!r}")
            targets = [resolve(url) for url in facts["references"]]
            for url, target in zip(facts["references"], targets):
                if target is None or target not in inspected:
                    host = urlsplit(url).netloc or "(not a URL)"
                    uncovered[host] = uncovered.get(host, 0) + 1
                    if uncovered[host] <= 3:
                        samples.append(f"{relpath}: points at {url}, which is not in the backup")
            if file_set.keys == "art-ids":
                # Informational: when a Piggy Gang document names the old image
                # it replaces, that should be the mint the migration app maps
                # to this art id. Two independent records of one pairing.
                old = [t for t in targets if t and t.startswith("piggy-sol-gang/images/")]
                if old:
                    agrees = f"piggy-sol-gang/images/{mint_of[key]}.png" in old
                    map_agrees += agrees
                    map_disagrees += not agrees
                    if not agrees and map_disagrees <= 3:
                        samples.append(f"{relpath}: names {old[0]}, the mint map says {mint_of[key]}")

        (args.out / file_set.manifest).write_text("".join(lines))
        entry = {
            "source": file_set.source,
            "keys": len(keys[file_set.keys]),
            "files": len(lines),
            "absent": sum(1 for a in absent if a["set"] == file_set.name),
            "bytes": sum(sizes),
            "smallest_bytes": min(sizes, default=0),
            "largest_bytes": max(sizes, default=0),
            "md5_equals_origin_etag": md5_proven,
            "origin_last_modified": [min(modified, default=None), max(modified, default=None)],
            "fetched": [min(fetched, default=None), max(fetched, default=None)],
            "manifest": file_set.manifest,
            "manifest_sha256": hashlib.sha256("".join(lines).encode()).hexdigest(),
            "content_problems": content_bad,
        }
        if file_set.ext == "png":
            entry["dimensions"] = dict(sorted(shapes.items(), key=lambda item: -item[1]))
        else:
            entry["image_field_is_not_its_own_image"] = wrong_image
            entry["references_outside_the_backup_by_host"] = uncovered
            if file_set.keys == "art-ids":
                entry["old_image_vs_mint_map"] = {"agree": map_agrees, "disagree": map_disagrees}
        if samples:
            entry["examples"] = samples[:20]
        summary_sets[file_set.name] = entry

    summary = {
        "issue": "PIG-217",
        "generated_at": now(),
        "layout": "{set}/{key}.{ext}, relative to the backup root; one sha256sum-format manifest per set",
        "inputs": {
            "mints": {"file": args.mints.name, "sha256": sha256_of(args.mints), "count": len(keys["mints"])},
            "mint_map": {"file": args.mint_map.name, "sha256": sha256_of(args.mint_map), "count": len(keys["art-ids"])},
        },
        "totals": {
            "files": sum(s["files"] for s in summary_sets.values()),
            "bytes": sum(s["bytes"] for s in summary_sets.values()),
            "absent": len(absent),
        },
        "sets": summary_sets,
        "absent": absent,
        "problems": problems,
    }
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    for name, entry in summary_sets.items():
        log(
            f"{name:<22} {entry['files']:>6}/{entry['keys']:<6} {entry['bytes'] / 1e9:8.3f} GB  "
            f"absent={entry['absent']} md5==etag={entry['md5_equals_origin_etag']} "
            f"content_problems={entry['content_problems']}"
        )
    for problem in problems[:20]:
        log(f"PROBLEM {problem}")
    for item in absent[:20]:
        log(f"ABSENT  {item['set']}/{item['key']} ({item['verdict']}, HTTP {item['http']})")
    undecided = [a for a in absent if a["verdict"] != "missing"]
    if problems or undecided:
        log(f"not clean: {len(problems)} problems, {len(undecided)} keys without a definite answer from the origin")
        return 1
    log(f"manifests written to {args.out}: {summary['totals']['files']} files, {len(absent)} confirmed absent at the origin")
    return 0


def read_manifests(directory: Path) -> dict[str, str]:
    """relpath -> sha256 across every *.sha256 in the directory."""
    entries: dict[str, str] = {}
    for path in sorted(directory.glob("*.sha256")):
        for line in path.read_text().splitlines():
            digest, relpath = line.split("  ", 1)
            if relpath in entries:
                raise SystemExit(f"{relpath} is listed twice")
            entries[relpath] = digest
    if not entries:
        raise SystemExit(f"no manifests in {directory}")
    return entries


def verify(args: argparse.Namespace) -> int:
    """Check a copy of the backup against the manifests: same files, same bytes, nothing extra."""
    entries = read_manifests(args.manifests)
    on_disk = {p.relative_to(args.root).as_posix() for p in args.root.rglob("*") if p.is_file()}
    lost = sorted(set(entries) - on_disk)
    extra = sorted(on_disk - set(entries))
    inspected = inspect_all(args.root, sorted(set(entries) & on_disk), "verify")
    changed = sorted(relpath for relpath, result in inspected.items() if result[1] != entries[relpath])
    broken = sorted(relpath for relpath, result in inspected.items() if result[2])
    for label, paths in (("MISSING", lost), ("CHANGED", changed), ("EXTRA", extra), ("UNREADABLE", broken)):
        for relpath in paths[:20]:
            log(f"{label} {relpath}")
    log(
        f"{len(entries)} in the manifests: {len(inspected) - len(changed)} match, {len(changed)} changed, "
        f"{len(lost)} missing, {len(extra)} extra, {len(broken)} not a whole PNG/JSON"
    )
    return 1 if lost or changed or extra or broken else 0


CONTENT_TYPES = {"png": "image/png", "json": "application/json", "sha256": "text/plain", "gz": "application/gzip"}


class Bucket:
    """An S3-compatible bucket, opened from the JSON `railway bucket credentials --json` prints."""

    def __init__(self, credentials: Path, prefix: str, workers: int):
        import boto3  # only this command needs it
        from botocore.config import Config

        secret = json.loads(credentials.read_text())
        self.name = secret["bucketName"]
        self.prefix = prefix
        self.workers = workers
        self.client = boto3.client(
            "s3",
            endpoint_url=secret["endpoint"],
            region_name=secret["region"],
            aws_access_key_id=secret["accessKeyId"],
            aws_secret_access_key=secret["secretAccessKey"],
            config=Config(
                s3={"addressing_style": "virtual" if secret.get("urlStyle") == "virtual-host" else "path"},
                retries={"max_attempts": 8, "mode": "standard"},
                max_pool_connections=workers,
                # Measured against the Railway bucket on 2026-11-15: a wrong
                # Content-MD5 is refused (BadDigest), but an object sent with a
                # wrong x-amz-checksum-sha256 is stored without complaint. So
                # the newer checksum headers are switched off rather than
                # trusted, and every put carries a Content-MD5.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )

    def listing(self) -> dict[str, tuple[int, str]]:
        """key (without the prefix) -> (size, ETag) for everything under the prefix."""
        found = {}
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.name, Prefix=self.prefix):
            for item in page.get("Contents", []):
                found[item["Key"][len(self.prefix) :]] = (item["Size"], item["ETag"].strip('"'))
        return found

    def put(self, key: str, body: bytes) -> None:
        self.client.put_object(
            Bucket=self.name,
            Key=self.prefix + key,
            Body=body,
            # The bucket refuses a body that does not hash to this, so a byte
            # flipped in transit is an error here rather than a stored file.
            ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode(),
            ContentType=CONTENT_TYPES[key.rsplit(".", 1)[1]],
            Metadata={"sha256": hashlib.sha256(body).hexdigest()},
        )

    def sha256(self, key: str) -> str:
        digest = hashlib.sha256()
        for chunk in self.client.get_object(Bucket=self.name, Key=self.prefix + key)["Body"].iter_chunks(1 << 20):
            digest.update(chunk)
        return digest.hexdigest()


def is_metadata(key: str) -> bool:
    """Manifests and journals stored beside the files; they describe the backup, they are not in it."""
    return key.startswith("_")


def check_bucket(bucket: Bucket, entries: dict[str, str]) -> int:
    """Read every file the manifests list back from the bucket: the only check that counts.

    The manifests and journals stored beside them are skipped, not checked.
    """
    listed = {key: value for key, value in bucket.listing().items() if not is_metadata(key)}
    lost = sorted(set(entries) - set(listed))
    extra = sorted(set(listed) - set(entries))
    wanted = sorted(set(entries) & set(listed))
    changed, done, started = [], 0, time.monotonic()
    with ThreadPoolExecutor(max_workers=bucket.workers) as pool:
        for key, digest in zip(wanted, pool.map(bucket.sha256, wanted)):
            done += 1
            if digest != entries[key]:
                changed.append(key)
            if done % 5000 == 0:
                log(f"read back {done}/{len(wanted)}")
    for label, keys in (("MISSING", lost), ("CHANGED", changed), ("EXTRA", extra)):
        for key in keys[:20]:
            log(f"{label} {bucket.prefix}{key}")
    stored = sum(listed[key][0] for key in wanted)
    log(
        f"s3://{bucket.name}/{bucket.prefix}: {len(entries)} in the manifests, {len(wanted) - len(changed)} read back "
        f"identical ({stored / 1e9:.3f} GB in {time.monotonic() - started:.0f} s), {len(changed)} changed, "
        f"{len(lost)} missing, {len(extra)} extra"
    )
    return 1 if lost or changed or extra else 0


def upload(args: argparse.Namespace) -> int:
    """Copy the local backup into the bucket, then read every file back."""
    entries = read_manifests(args.manifests)
    bucket = Bucket(args.credentials, args.prefix, args.workers)
    if args.check_only:
        return check_bucket(bucket, entries)
    if args.dest is None:
        raise SystemExit("upload needs --dest (only --check-only works without it)")
    root: Path = args.dest / "files"
    existing = bucket.listing()
    log(f"s3://{bucket.name}/{bucket.prefix}: {len(existing)} objects already there")

    def send(item: tuple[str, str]) -> str:
        key, digest = item
        body = (root / key).read_bytes()
        if hashlib.sha256(body).hexdigest() != digest:
            return "local"  # never upload a file that is not what its manifest says
        if existing.get(key) == (len(body), hashlib.md5(body).hexdigest()):
            return "have"
        bucket.put(key, body)
        return "sent"

    tally = {"sent": 0, "have": 0, "local": 0}
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for count, ((key, _), outcome) in enumerate(zip(entries.items(), pool.map(send, entries.items())), 1):
            tally[outcome] += 1
            if outcome == "local":
                log(f"NOT SENT {key}: the local file does not match its manifest")
            if count % 5000 == 0:
                log(f"uploaded {count}/{len(entries)}")
    log(f"{tally['sent']} sent, {tally['have']} already identical, {tally['local']} refused in {time.monotonic() - started:.0f} s")

    # The backup describes itself: the manifests, and what the origin said about
    # every key (ETag, Last-Modified, S3 version id) when it was fetched.
    for path in sorted(args.manifests.iterdir()):
        if path.suffix in (".sha256", ".json"):
            bucket.put(f"_manifests/{path.name}", path.read_bytes())
    for path in sorted((args.dest / "state").glob("*.jsonl")):
        bucket.put(f"_journal/{path.name}.gz", gzip.compress(path.read_bytes(), mtime=0))
    return check_bucket(bucket, entries) or (1 if tally["local"] else 0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    def common(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--dest", type=Path, required=True, help="local staging directory (files/ and state/)")
        sub.add_argument("--mints", type=Path, default=DEFAULT_MINTS, help="Piggy SOL Gang mint list")
        sub.add_argument("--mint-map", type=Path, default=DEFAULT_MINT_MAP, help="mint -> art id map")

    sub = commands.add_parser("download", help="fetch whatever is not already in --dest")
    common(sub)
    sub.add_argument("--only", action="append", help="limit to one set (repeatable), e.g. piggy-gang/json")
    sub.add_argument("--limit", type=int, default=0, help="first N keys of each set (smoke test)")
    sub.add_argument("--r2-rps", type=float, default=10.0, help="r2.dev request ceiling per second")
    sub.add_argument("--s3-rps", type=float, default=25.0, help="S3 request ceiling per second")
    sub.add_argument("--r2-workers", type=int, default=8)
    sub.add_argument("--s3-workers", type=int, default=12)
    sub.add_argument("--retry-missing", action="store_true", help="ask again for keys that answered 404")
    sub.add_argument("--report-every", type=int, default=30, help="seconds between progress lines")
    sub.set_defaults(run=download)

    sub = commands.add_parser("manifest", help="re-hash every file from disk, check it, write the manifests")
    common(sub)
    sub.add_argument("--out", type=Path, default=MANIFESTS, help="where the manifests go")
    sub.set_defaults(run=manifest)

    sub = commands.add_parser("verify", help="check a copy of the backup against the manifests")
    sub.add_argument("--root", type=Path, required=True, help="directory holding piggy-sol-gang/ and piggy-gang/")
    sub.add_argument("--manifests", type=Path, default=MANIFESTS)
    sub.set_defaults(run=verify)

    sub = commands.add_parser("upload", help="copy --dest to an S3-compatible bucket and read every file back")
    sub.add_argument("--dest", type=Path, help="local staging directory (files/ and state/)")
    sub.add_argument("--manifests", type=Path, default=MANIFESTS)
    sub.add_argument("--credentials", type=Path, required=True, help="JSON from `railway bucket credentials --json`")
    sub.add_argument("--prefix", default="backup/", help="key prefix inside the bucket")
    sub.add_argument("--workers", type=int, default=16)
    sub.add_argument("--check-only", action="store_true", help="upload nothing; only read the bucket back")
    sub.set_defaults(run=upload)

    args = parser.parse_args()
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())

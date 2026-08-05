#!/usr/bin/env python3
"""Block local-private values and credentials from entering Git."""

from __future__ import annotations

import argparse
import getpass
import ipaddress
import os
from pathlib import Path
import platform
import re
import subprocess
import sys


SCANNER_PATH = ".githooks/check_local_privacy.py"
PRIVATE_KEY = re.compile(
    r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"
)
TOKEN_PREFIX = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"gh[pousr]_[A-Za-z0-9]{20,}|"
    r"sk-(?:proj-)?[A-Za-z0-9_-]{20,}|"
    r"AKIA[0-9A-Z]{16}|"
    r"xox[baprs]-[A-Za-z0-9-]{20,}|"
    r"(?:rk|sk)_(?:live|test)_[A-Za-z0-9]{20,}"
    r")(?![A-Za-z0-9])"
)
CREDENTIAL_URL = re.compile(
    r"(?i)https?://[^\s/:@]+:[^\s/@]+@[^\s/]+"
)
SECRET_LITERAL = re.compile(
    r"(?i)\b(?:api[_-]?(?:key|secret)|client[_-]?secret|password|"
    r"private[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"authorization|cookie|session[_-]?(?:id|token))\b"
    r"\s*[:=]\s*[\"']([^\"']{8,})[\"']"
)
EMAIL = re.compile(
    r"(?i)\b[A-Z0-9._%+-]+@([A-Z0-9.-]+\.[A-Z]{2,})\b"
)
SID = re.compile(r"\bS-1-5-21-((?:\d+-){3}\d+)\b")
MAC_ADDRESS = re.compile(
    r"(?i)(?<![0-9a-f])[0-9a-f]{2}([:-])"
    r"(?:[0-9a-f]{2}\1){4}[0-9a-f]{2}(?![0-9a-f])"
)
IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
IPV6_CANDIDATE = re.compile(
    r"(?<![A-Za-z0-9_:])\[?([A-Fa-f0-9]{0,4}(?::[A-Fa-f0-9]{0,4}){2,7})\]?"
    r"(?![A-Za-z0-9_:])"
)
QUOTED_WINDOWS_PATH = re.compile(
    r"(?i)[\"']([A-Z]:[\\/][^\"'\r\n]+)[\"']"
)
BARE_WINDOWS_PATH = re.compile(
    r"(?i)(?<![A-Za-z0-9])([A-Z]:[\\/]+[^\s\"'<>|]+)"
)
UNC_PATH = re.compile(
    r"(?<![.A-Za-z0-9}:\\])\\\\[A-Za-z0-9][^\s\\/]*[\\/][^\s\"'<>|]+|"
    r"(?<![A-Za-z0-9+/:=])//[A-Za-z0-9][^\s\\/]*[\\/][^\s\"'<>|]+"
)
POSIX_HOME = re.compile(r"(?i)/(?:Users|home)/[^/\s\"']+|/root(?:/|\b)")
PROXY_LITERAL = re.compile(
    r"(?i)\b(?:https?_proxy|all_proxy|proxy_url)\b\s*[:=]\s*[\"']([^\"']+)[\"']"
)

EXAMPLE_EMAIL_DOMAINS = {
    "example.com",
    "example.net",
    "example.org",
    "example.test",
    "example.invalid",
    "users.noreply.github.com",
}
DOCUMENTATION_IPV4 = tuple(
    ipaddress.ip_network(value)
    for value in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24")
)
DOCUMENTATION_IPV6 = ipaddress.ip_network("2001:db8::/32")
SAFE_LITERAL_MARKERS = (
    "example",
    "placeholder",
    "synthetic",
    "test-only",
    "sensitive-",
    "dummy",
    "fake",
    "qualification-",
    "named-",
)
SAFE_ABSOLUTE_PREFIXES = (
    "c:/halpharuntime/",
    "c:/program files/",
    "c:/program",
    "c:/example/",
    "c:/temp/",
    "c:/tmp/",
    "d:/wrong",
)
SENSITIVE_FILE_SUFFIXES = {
    ".key",
    ".pem",
    ".p12",
    ".pfx",
    ".sqlite",
    ".sqlite3",
    ".db",
    ".dump",
    ".log",
    ".bak",
    ".orig",
}
SENSITIVE_FILE_NAMES = {
    ".env",
    ".pgpass",
    "pgpass.conf",
    "credentials.json",
    "credentials.toml",
    "secrets.json",
    "secrets.toml",
    "id_rsa",
    "id_ed25519",
}


def _git(*args: str) -> bytes:
    return subprocess.check_output(
        ["git", *args],
        stderr=subprocess.DEVNULL,
    )


def _repository_root() -> Path:
    return Path(_git("rev-parse", "--show-toplevel").decode().strip()).resolve()


def _paths(mode: str) -> list[str]:
    if mode == "staged":
        raw = _git(
            "diff",
            "--cached",
            "--name-only",
            "-z",
            "--diff-filter=ACMR",
        )
    else:
        raw = _git("ls-files", "-z", "--cached", "--others", "--exclude-standard")
    return [
        value
        for value in raw.decode("utf-8", errors="surrogateescape").split("\0")
        if value
    ]


def _content(path: str, mode: str, root: Path) -> bytes | None:
    try:
        if mode == "staged":
            return _git("show", f":{path}")
        candidate = root / path
        return candidate.read_bytes() if candidate.is_file() else None
    except (OSError, subprocess.CalledProcessError):
        return None


def _normalize_path(value: str) -> str:
    return re.sub(r"[\\/]+", "/", value).rstrip("),.;:").casefold()


def _synthetic_sid(value: str) -> bool:
    machine = value.split("-")[:3]
    return all(len(part) <= 3 or len(set(part)) == 1 for part in machine)


def _safe_ip(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return True
    if address.is_loopback or address.is_unspecified:
        return True
    if isinstance(address, ipaddress.IPv4Address):
        return any(address in network for network in DOCUMENTATION_IPV4)
    return address in DOCUMENTATION_IPV6


def _safe_proxy(value: str) -> bool:
    folded = value.casefold()
    return (
        not folded
        or any(marker in folded for marker in SAFE_LITERAL_MARKERS)
        or folded.startswith(("http://127.0.0.1", "http://localhost"))
        or "${" in value
        or "%" in value
    )


def _path_categories(path: str) -> set[str]:
    normalized = path.replace("\\", "/").casefold()
    name = normalized.rsplit("/", 1)[-1]
    suffix = Path(name).suffix
    categories: set[str] = set()
    if name in SENSITIVE_FILE_NAMES or suffix in SENSITIVE_FILE_SUFFIXES:
        categories.add("sensitive-file")
    if (
        suffix in {".txt", ".json", ".toml", ".yaml", ".yml", ".ini"}
        and re.search(r"(?:api[-_]?key|copytrading[-_]?api|credentials?|secrets?)", name)
        and "example" not in name
    ):
        categories.add("sensitive-file")
    if suffix in {".zip", ".7z", ".rar", ".tar", ".gz"}:
        categories.add("opaque-archive")
    if name.startswith(".env.") and not name.endswith(".example"):
        categories.add("sensitive-file")
    if ".local." in name or ".private." in name:
        categories.add("local-config-file")
    if any(part in {"credentials", "secrets", ".secrets"} for part in normalized.split("/")):
        categories.add("sensitive-directory")
    if (
        normalized.startswith("config/")
        and name.endswith(".toml")
        and ".example." not in name
    ):
        categories.add("enabled-local-config")
    return categories


def _content_categories(
    line: str,
    private_roots: tuple[str, ...],
    identities: tuple[str, ...],
) -> set[str]:
    categories: set[str] = set()
    if PRIVATE_KEY.search(line):
        categories.add("private-key")
    if TOKEN_PREFIX.search(line):
        categories.add("token-prefix")
    credential_url = CREDENTIAL_URL.search(line)
    if credential_url and not any(
        marker in credential_url.group(0).casefold()
        for marker in (*SAFE_LITERAL_MARKERS, "user:secret@")
    ):
        categories.add("credential-url")
    for match in SECRET_LITERAL.finditer(line):
        value = match.group(1).casefold()
        if not any(marker in value for marker in SAFE_LITERAL_MARKERS):
            categories.add("literal-secret")
    for match in EMAIL.finditer(line):
        if match.group(1).casefold() not in EXAMPLE_EMAIL_DOMAINS:
            categories.add("email")
    for match in SID.finditer(line):
        if not _synthetic_sid(match.group(1)):
            categories.add("windows-sid")
    for match in MAC_ADDRESS.finditer(line):
        compact = re.sub(r"[:-]", "", match.group(0)).casefold()
        if compact not in {"000000000000", "ffffffffffff", "001122334455"}:
            categories.add("mac-address")
    for match in IPV4.finditer(line):
        if not _safe_ip(match.group(0)):
            categories.add("non-loopback-ip")
    for match in IPV6_CANDIDATE.finditer(line):
        if not _safe_ip(match.group(1)):
            categories.add("non-loopback-ip")

    path_values = [match.group(1) for match in QUOTED_WINDOWS_PATH.finditer(line)]
    path_values.extend(match.group(1) for match in BARE_WINDOWS_PATH.finditer(line))
    path_values.extend(match.group(0) for match in POSIX_HOME.finditer(line))
    path_values.extend(match.group(0) for match in UNC_PATH.finditer(line))
    for value in path_values:
        normalized = _normalize_path(value)
        if any(normalized.startswith(root) for root in private_roots):
            categories.add("host-path")
        elif normalized.startswith("//"):
            categories.add("unc-path")
        elif not any(normalized.startswith(prefix) for prefix in SAFE_ABSOLUTE_PREFIXES):
            categories.add("absolute-host-path")

    for match in PROXY_LITERAL.finditer(line):
        if not _safe_proxy(match.group(1)):
            categories.add("proxy-config")
    folded = line.casefold()
    if any(identity and identity in folded for identity in identities):
        categories.add("host-identity")
    return categories


def _privacy_context(root: Path) -> tuple[tuple[str, ...], tuple[str, ...]]:
    private_roots = tuple(
        _normalize_path(str(path)) + "/"
        for path in {Path.home().resolve(), root.parent.resolve()}
    )
    identities = tuple(
        value.casefold()
        for value in {
            getpass.getuser(),
            platform.node(),
            os.environ.get("USERNAME", ""),
        }
        if len(value.strip()) >= 4
    )
    return private_roots, identities


def _scan_blob(
    path: str,
    raw: bytes | None,
    private_roots: tuple[str, ...],
    identities: tuple[str, ...],
) -> list[tuple[str, int, str]]:
    findings: list[tuple[str, int, str]] = []
    for category in sorted(_path_categories(path)):
        findings.append((path, 0, category))
    for category in sorted(_content_categories(path, private_roots, identities)):
        if category in {
            "email",
            "host-identity",
            "mac-address",
            "non-loopback-ip",
            "windows-sid",
        }:
            findings.append((path, 0, f"path-{category}"))
    if path.replace("\\", "/") == SCANNER_PATH or raw is None:
        return findings
    if b"\0" in raw:
        text = "\n".join(
            value.decode("ascii", errors="ignore")
            for value in re.findall(rb"[\x20-\x7e]{8,}", raw)
        )
    else:
        text = raw.decode("utf-8", errors="replace")
    for line_number, line in enumerate(text.splitlines(), 1):
        for category in sorted(_content_categories(line, private_roots, identities)):
            findings.append((path, line_number, category))
    return findings


def _scan(mode: str) -> list[tuple[str, int, str]]:
    root = _repository_root()
    private_roots, identities = _privacy_context(root)
    findings: list[tuple[str, int, str]] = []
    for path in _paths(mode):
        if mode != "staged" and not (root / path).is_file():
            continue
        raw = _content(path, mode, root)
        findings.extend(_scan_blob(path, raw, private_roots, identities))
    return findings


def _outgoing_commits(remote: str) -> list[str]:
    zero_sha = "0" * 40
    commits: list[str] = []
    seen: set[str] = set()
    remote_refs = [
        value
        for value in _git(
            "for-each-ref",
            "--format=%(refname)",
            f"refs/remotes/{remote}/",
        )
        .decode("utf-8", errors="replace")
        .splitlines()
        if value
    ]
    for line in sys.stdin:
        parts = line.split()
        if len(parts) != 4:
            continue
        _, local_sha, _, remote_sha = parts
        if local_sha == zero_sha:
            continue
        args = ["rev-list", local_sha]
        if remote_sha != zero_sha:
            args.append(f"^{remote_sha}")
        elif remote_refs:
            args.extend(["--not", *remote_refs])
        for commit in _git(*args).decode("ascii").splitlines():
            if commit not in seen:
                seen.add(commit)
                commits.append(commit)
    return commits


def _scan_commit(commit: str) -> list[tuple[str, int, str]]:
    root = _repository_root()
    private_roots, identities = _privacy_context(root)
    short_commit = commit[:12]
    findings: list[tuple[str, int, str]] = []
    metadata = _git("show", "-s", "--format=%an%n%cn", commit).decode(
        "utf-8", errors="replace"
    )
    metadata_lines = metadata.splitlines()
    for value in metadata_lines:
        if any(identity and identity in value.casefold() for identity in identities):
            findings.append((f"commit:{short_commit}", 0, "author-host-identity"))
            break
    message = _git("show", "-s", "--format=%B", commit).decode(
        "utf-8", errors="replace"
    )
    for line_number, line in enumerate(message.splitlines(), 1):
        for category in sorted(_content_categories(line, private_roots, identities)):
            findings.append(
                (f"commit:{short_commit}:message", line_number, category)
            )
    raw_paths = _git(
        "diff-tree",
        "--root",
        "--no-commit-id",
        "--name-only",
        "-r",
        "-m",
        "-z",
        "--diff-filter=ACMR",
        commit,
    )
    paths = {
        value
        for value in raw_paths.decode("utf-8", errors="surrogateescape").split("\0")
        if value
    }
    for path in sorted(paths):
        try:
            raw = _git("show", f"{commit}:{path}")
        except subprocess.CalledProcessError:
            raw = None
        for finding_path, line_number, category in _scan_blob(
            path,
            raw,
            private_roots,
            identities,
        ):
            findings.append(
                (f"commit:{short_commit}:{finding_path}", line_number, category)
            )
    return findings


def _scan_pre_push(remote: str) -> list[tuple[str, int, str]]:
    findings: list[tuple[str, int, str]] = []
    for commit in _outgoing_commits(remote):
        findings.extend(_scan_commit(commit))
    return findings


def _self_test() -> None:
    root = _normalize_path(str(_repository_root().parent)) + "/"
    private_roots = (root, _normalize_path(str(Path.home())) + "/")
    unsafe = {
        'api_key = "sk-proj-abcdefghijklmnopqrstuvwxyz123456"': "token-prefix",
        "endpoint = '10.22.33.44'": "non-loopback-ip",
        f'path = "{_repository_root().parent / "private"}"': "host-path",
        "owner = 'person@personalmail.tested'": "email",
        "sid = 'S-1-5-21-1234567890-2345678901-3456789012-1001'": "windows-sid",
        "proxy_url = 'https://private-proxy.internal:8443'": "proxy-config",
    }
    for line, category in unsafe.items():
        if category not in _content_categories(line, private_roots, ()):
            raise AssertionError(f"self-test failed for {category}")
    safe = (
        "bind = '127.0.0.1'",
        "owner = 'owner@example.invalid'",
        "path = 'C:/HalphaRuntime/gate.json'",
        "sid = 'S-1-5-21-0-0-0-1001'",
        "proxy_url = 'http://127.0.0.1:7897'",
    )
    for line in safe:
        if _content_categories(line, private_roots, ()):
            raise AssertionError("self-test rejected a safe example")
    if "sensitive-file" not in _path_categories("credentials.json"):
        raise AssertionError("self-test failed for sensitive-file")
    if "sensitive-file" not in _path_categories("copytrading-api.txt"):
        raise AssertionError("self-test failed for credential filename")
    if "enabled-local-config" not in _path_categories("config/runtime.toml"):
        raise AssertionError("self-test failed for enabled-local-config")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all",
        "--working-tree",
        dest="working_tree",
        action="store_true",
        help="scan tracked and non-ignored untracked files instead of the staged snapshot",
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--pre-push", action="store_true")
    parser.add_argument("--remote", default="origin")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        print("privacy-check self-test passed")
        return 0
    if args.pre_push:
        findings = _scan_pre_push(args.remote)
    else:
        mode = "working" if args.working_tree else "staged"
        findings = _scan(mode)
    for path, line_number, category in findings:
        location = f"{path}:{line_number}" if line_number else path
        print(f"privacy-check: {location}: {category}", file=sys.stderr)
    if findings:
        print(
            "privacy-check: blocked; move local values outside Git or use synthetic examples",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

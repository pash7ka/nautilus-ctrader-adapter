"""Regenerate the protobuf bindings from the vendored `.proto` files.

Run: uv run python scripts/gen_protobuf.py

The vendored protos are kept byte-identical to Spotware's upstream, and they use bare
imports (`import "OpenApiModelMessages.proto";`). protoc copies those into the generated code
as bare `import OpenApiModelMessages_pb2`, which fails inside a package. This script rewrites
exactly those lines to package-absolute imports, and refuses to finish if the number rewritten
differs from the number the schema declares - a change in protoc's output format must stop
generation, not ship broken bindings.

The toolchain version matters too. The bundled protoc of the pinned `grpcio-tools` stamps
its protobuf version into the generated files, and protobuf refuses to load generated code
newer than the runtime. This script refuses to finish if the stamp is newer than the
`protobuf` floor in pyproject.toml - raise the floor with the pin.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MESSAGES_DIR = ROOT / "src" / "nautilus_ctrader" / "messages"
PACKAGE = "nautilus_ctrader.messages"

_PROTO_IMPORT = re.compile(r'^import "(OpenApi\w+)\.proto";', re.MULTILINE)
_BARE_PYTHON_IMPORT = re.compile(r"^import (OpenApi\w+_pb2) as (\w+)$", re.MULTILINE)
_RUNTIME_CHECK = re.compile(
    r"ValidateProtobufRuntimeVersion\(\s*_runtime_version\.Domain\.PUBLIC,\s*(\d+),\s*(\d+),\s*(\d+),"
)
_PROTOBUF_REQUIREMENT = re.compile(r"protobuf>=([\d.]+),<(\d+)")


def generated_version(generated: Path) -> tuple[int, int, int] | None:
    """The protobuf version a generated module requires of the runtime, or None if unreadable."""
    found = _RUNTIME_CHECK.search(generated.read_text(encoding="utf-8"))
    return tuple(int(part) for part in found.groups()) if found else None


def declared_protobuf() -> tuple[tuple[int, ...], int]:
    """The `protobuf` floor and major-version ceiling (exclusive) declared in pyproject.toml."""
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    for requirement in pyproject["project"]["dependencies"]:
        found = _PROTOBUF_REQUIREMENT.fullmatch(requirement)
        if found:
            floor, ceiling = found.groups()
            return tuple(int(part) for part in floor.split(".")), int(ceiling)
    raise ValueError("pyproject.toml declares no `protobuf>=X.Y.Z,<N` requirement")


def _check_versions() -> str | None:
    floor, _ = declared_protobuf()
    for generated in sorted(MESSAGES_DIR.glob("*_pb2.py")):
        version = generated_version(generated)
        if version is None:
            return f"{generated.name}: no runtime version check; protoc's output may have changed"
        if version > floor:
            wanted = ".".join(str(part) for part in version)
            return f"{generated.name} requires protobuf {wanted}; raise the floor in pyproject.toml"
    return None


def _declared_imports(protos: list[Path]) -> int:
    return sum(len(_PROTO_IMPORT.findall(p.read_text(encoding="utf-8"))) for p in protos)


def _rewrite_imports() -> int:
    rewritten = 0
    for generated in sorted(MESSAGES_DIR.glob("*_pb2.py")):
        text = generated.read_text(encoding="utf-8")
        text, count = _BARE_PYTHON_IMPORT.subn(rf"from {PACKAGE} import \1 as \2", text)
        if count:
            generated.write_text(text, encoding="utf-8")
        rewritten += count
    return rewritten


def main() -> int:
    protos = sorted(MESSAGES_DIR.glob("*.proto"))
    if not protos:
        print(f"no .proto files found in {MESSAGES_DIR}", file=sys.stderr)
        return 1

    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={MESSAGES_DIR}",
        f"--python_out={MESSAGES_DIR}",
        *(str(p) for p in protos),
    ]
    print(" ".join(command))
    status = subprocess.call(command)
    if status:
        return status

    declared = _declared_imports(protos)
    rewritten = _rewrite_imports()
    if rewritten != declared:
        print(
            f"rewrote {rewritten} import(s) but the schema declares {declared}; "
            "protoc's output format may have changed - inspect before committing",
            file=sys.stderr,
        )
        return 1
    print(f"rewrote {rewritten} import(s) to {PACKAGE}")

    problem = _check_versions()
    if problem:
        print(problem, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

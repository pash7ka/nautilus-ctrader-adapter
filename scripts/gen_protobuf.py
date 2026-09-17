"""Regenerate the protobuf bindings from the vendored `.proto` files.

Run: uv run python scripts/gen_protobuf.py

The vendored protos are kept byte-identical to Spotware's upstream, and they use bare
imports (`import "OpenApiModelMessages.proto";`). protoc copies those into the generated code
as bare `import OpenApiModelMessages_pb2`, which fails inside a package. This script rewrites
exactly those lines to package-absolute imports, and refuses to finish if the number rewritten
differs from the number the schema declares - a change in protoc's output format must stop
generation, not ship broken bindings.

The toolchain version matters too. Protobuf refuses generated code newer than the runtime,
and nautilus-trader pins protobuf==5.29.6 under its `ib` extra, so `grpcio-tools` is pinned
to the 1.68 line in pyproject.toml. This script refuses to run against anything else.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MESSAGES_DIR = ROOT / "src" / "nautilus_ctrader" / "messages"
PACKAGE = "nautilus_ctrader.messages"
# Changes together with the grpcio-tools pin in pyproject.toml.
REQUIRED_PROTOBUF_MAJOR_MINOR = (5, 29)

_PROTO_IMPORT = re.compile(r'^import "(OpenApi\w+)\.proto";', re.MULTILINE)
_BARE_PYTHON_IMPORT = re.compile(r"^import (OpenApi\w+_pb2) as (\w+)$", re.MULTILINE)


def _check_toolchain() -> str | None:
    from google.protobuf import __version__ as runtime_version

    parts = tuple(int(p) for p in runtime_version.split(".")[:2])
    if parts != REQUIRED_PROTOBUF_MAJOR_MINOR:
        wanted = ".".join(str(p) for p in REQUIRED_PROTOBUF_MAJOR_MINOR)
        return (
            f"protobuf runtime is {runtime_version}, expected {wanted}.x. "
            "Generated code must not be newer than the runtime any user will have; "
            "check the grpcio-tools pin in pyproject.toml."
        )
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
    problem = _check_toolchain()
    if problem:
        print(problem, file=sys.stderr)
        return 1

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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

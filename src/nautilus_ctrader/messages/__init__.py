"""Protobuf bindings for the cTrader Open API.

The `.proto` files here are vendored byte-identical from Spotware's MIT-licensed
`openapi-proto-messages` repository; `LICENSE.spotware` is their licence. The `*_pb2.py`
files beside them are generated build output - regenerate with `scripts/gen_protobuf.py`
rather than editing them.

The generated files are not raw protoc output. The vendored protos use bare cross-file
imports, so protoc emits bare `import OpenApi..._pb2` lines that fail inside a package; the
generator rewrites exactly those lines to package-absolute imports.
"""

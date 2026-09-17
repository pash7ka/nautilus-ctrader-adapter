"""A stand-in for the Nautilus `Logger` that records what was logged.

The Nautilus logger writes from Rust, so its output is invisible to pytest's caplog: asserting
against caplog would pass without checking anything.
"""

from __future__ import annotations


class RecordingLogger:
    def __init__(self) -> None:
        self.lines: list[tuple[str, str]] = []

    def debug(self, message: str) -> None:
        self.lines.append(("debug", message))

    def info(self, message: str) -> None:
        self.lines.append(("info", message))

    def warning(self, message: str) -> None:
        self.lines.append(("warning", message))

    def error(self, message: str) -> None:
        self.lines.append(("error", message))

    def exception(self, message: str, ex: BaseException) -> None:
        self.lines.append(("error", message))

    def errors(self) -> list[str]:
        return [message for level, message in self.lines if level == "error"]

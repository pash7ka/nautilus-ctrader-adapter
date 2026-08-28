"""Shared pytest fixtures.

Tests run offline against recorded protobuf fixtures and the fake server. Anything needing a
real broker connection must carry the ``broker`` marker.
"""

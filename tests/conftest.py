"""Pytest configuration for async tests"""
import pytest
import asyncio
from typing import Generator

pytest_plugins = ('pytest_asyncio',)

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

def pytest_configure(config):
    """Configure pytest-asyncio"""
    config.option.asyncio_mode = "auto"

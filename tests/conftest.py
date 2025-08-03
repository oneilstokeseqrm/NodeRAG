"""Pytest configuration for async tests"""
import pytest

pytest_plugins = ('pytest_asyncio',)

def pytest_configure(config):
    """Configure pytest-asyncio"""
    config.option.asyncio_mode = "auto"

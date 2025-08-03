#!/usr/bin/env python3
"""Investigate asyncio test failure"""

import sys
import asyncio
import platform
import json
from datetime import datetime

def gather_environment_info():
    """Gather relevant environment information"""
    info = {
        "timestamp": datetime.now().isoformat(),
        "python_version": sys.version,
        "platform": platform.platform(),
        "asyncio_version": asyncio.__version__ if hasattr(asyncio, '__version__') else "builtin",
        "event_loop_policy": str(asyncio.get_event_loop_policy()),
    }
    
    try:
        import pytest_asyncio
        info["pytest_asyncio_version"] = pytest_asyncio.__version__
    except ImportError:
        info["pytest_asyncio_version"] = "Not installed"
    
    try:
        loop = asyncio.get_event_loop()
        info["event_loop_running"] = loop.is_running()
        info["event_loop_closed"] = loop.is_closed()
    except RuntimeError as e:
        info["event_loop_error"] = str(e)
    
    return info

def check_for_event_loop_conflicts():
    """Check for common event loop conflicts"""
    conflicts = []
    
    try:
        import IPython
        conflicts.append("IPython detected - may have existing event loop")
    except ImportError:
        pass
    
    try:
        import nest_asyncio
        conflicts.append("nest_asyncio installed - may affect event loop behavior")
    except ImportError:
        pass
    
    try:
        import uvloop
        conflicts.append("uvloop installed - alternative event loop implementation")
    except ImportError:
        pass
    
    return conflicts

if __name__ == "__main__":
    report = {
        "environment": gather_environment_info(),
        "conflicts": check_for_event_loop_conflicts(),
    }
    
    with open("asyncio_investigation_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    print("Environment investigation complete. See asyncio_investigation_report.json")
    print("\nKey findings:")
    print(f"Python: {report['environment']['python_version'].split()[0]}")
    print(f"Platform: {report['environment']['platform']}")
    if report['conflicts']:
        print(f"Potential conflicts: {', '.join(report['conflicts'])}")

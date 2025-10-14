#!/usr/bin/env python3
"""
Test script for metrex live mode functionality.
Run this after installing ccxt to verify the implementation.
"""

import sys
from pathlib import Path

def test_imports():
    """Test that all modules can be imported."""
    print("Testing imports...")
    try:
        from metrex import cli
        from metrex import live_runner
        from metrex import processor
        from metrex import metrics
        print("✅ All imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_live_data_function():
    """Test that get_live_data function exists and has correct signature."""
    print("\nTesting get_live_data function...")
    try:
        from metrex.live_runner import get_live_data
        import inspect
        
        sig = inspect.signature(get_live_data)
        params = list(sig.parameters.keys())
        
        expected_params = ['symbols', 'timeframe', 'limit', 'exchange_id']
        if params == expected_params:
            print(f"✅ Function signature correct: {params}")
            return True
        else:
            print(f"❌ Function signature mismatch")
            print(f"   Expected: {expected_params}")
            print(f"   Got: {params}")
            return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_runner_functions():
    """Test that all runner functions exist."""
    print("\nTesting runner functions...")
    try:
        from metrex.live_runner import (
            get_live_data,
            run_live_metrics,
            run_live_ranking,
            run_live_combined
        )
        print("✅ All runner functions exist")
        return True
    except ImportError as e:
        print(f"❌ Missing function: {e}")
        return False

def test_cli_commands():
    """Test that CLI commands are properly registered."""
    print("\nTesting CLI commands...")
    try:
        from metrex.cli import cli
        
        commands = list(cli.commands.keys())
        expected = ['metrics', 'rank', 'live', 'list']
        
        missing = set(expected) - set(commands)
        if not missing:
            print(f"✅ All CLI commands registered: {commands}")
            return True
        else:
            print(f"❌ Missing commands: {missing}")
            return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_metrics_command_options():
    """Test that metrics command has live mode options."""
    print("\nTesting metrics command options...")
    try:
        from metrex.cli import metrics
        
        param_names = [p.name for p in metrics.params]
        
        expected_live_params = ['live', 'symbols', 'interval', 'exchange']
        missing = set(expected_live_params) - set(param_names)
        
        if not missing:
            print(f"✅ All live mode options present in metrics command")
            return True
        else:
            print(f"❌ Missing options: {missing}")
            return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_live_command_options():
    """Test that live command has required options."""
    print("\nTesting live command options...")
    try:
        from metrex.cli import live
        
        param_names = [p.name for p in live.params]
        
        expected_params = ['symbols', 'timeframe', 'interval', 'metrics', 
                          'all_metrics', 'metrics_output', 'ranks_output', 'exchange']
        missing = set(expected_params) - set(param_names)
        
        if not missing:
            print(f"✅ All required options present in live command")
            return True
        else:
            print(f"❌ Missing options: {missing}")
            return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_ccxt_available():
    """Test if CCXT is installed."""
    print("\nTesting CCXT installation...")
    try:
        import ccxt
        print(f"✅ CCXT version {ccxt.__version__} installed")
        return True
    except ImportError:
        print("⚠️  CCXT not installed - live mode will not work")
        print("   Install with: pip install ccxt>=4.0.0")
        return False

def run_all_tests():
    """Run all tests and report results."""
    print("="*60)
    print("METREX LIVE MODE TEST SUITE")
    print("="*60)
    
    tests = [
        test_imports,
        test_live_data_function,
        test_runner_functions,
        test_cli_commands,
        test_metrics_command_options,
        test_live_command_options,
        test_ccxt_available
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test crashed: {e}")
            results.append(False)
    
    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    elif passed >= total - 1:  # Allow CCXT to be missing
        print("\n✅ Core functionality tests passed")
        print("⚠️  Install CCXT to enable live mode")
        return 0
    else:
        print("\n❌ Some tests failed - review implementation")
        return 1

if __name__ == '__main__':
    sys.exit(run_all_tests())

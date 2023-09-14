import sys
from pathlib import Path
sys.path[0] = str(Path(sys.path[0]).parent)

import pytest
from emissions_cost import cost

class TestCost:

    # Test with a positive float value for emissions.
    def test_positive_float_value(self):
        assert cost(5000) == 42.5

    # Test with a zero value for emissions.
    def test_zero_value(self):
        assert cost(0) == 0.0

    # Test with a large float value for emissions.
    def test_large_float_value(self):
        assert cost(100000) == 850.0

    # Test with the minimum possible float value for emissions.
    def test_minimum_float_value(self):
        assert cost(2.2250738585072014e-308) == 0.0

    
    # Test with a negative float value for emissions.
    def test_negative_float_value(self):
        assert cost(-5000) == -42.5
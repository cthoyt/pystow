"""Tests for itertools."""

import unittest
from collections.abc import Iterable

from pystow.utils import reyield


class TestItertools(unittest.TestCase):
    """Tests for itertools."""

    def test_reyield(self) -> None:
        """Test making a function re-iterable."""
        sum_value = 0
        square_sum_value = 0

        def _accumulate(inner_elements: Iterable[int]) -> None:
            nonlocal sum_value
            for inner_element in inner_elements:
                sum_value += inner_element

        for element in reyield(_accumulate, range(5)):
            square_sum_value += element**2

        self.assertEqual(10, sum_value)
        self.assertEqual(30, square_sum_value)

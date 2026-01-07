"""Test git utilities."""

import unittest

from pystow.git import temporary_github_clone


class TestGitUtils(unittest.TestCase):
    """Test git utilities."""

    def test_temporary_github_clone(self) -> None:
        """Test temporarily cloning a GitHub repository."""
        with temporary_github_clone("cthoyt", "cthoyt") as directory:
            self.assertTrue(directory.joinpath("README.md").is_file())

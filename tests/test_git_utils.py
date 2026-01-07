"""Test git utilities."""

import unittest

from pystow.git import create_branch, get_current_branch, has_local_branch, temporary_github_clone


class TestGitUtils(unittest.TestCase):
    """Test git utilities."""

    def test_temporary_github_clone(self) -> None:
        """Test temporarily cloning a GitHub repository."""
        name = "test-branch-name"

        with temporary_github_clone("cthoyt", "cthoyt") as directory:
            self.assertTrue(directory.joinpath("README.md").is_file())
            self.assertEqual("master", get_current_branch(directory))

            self.assertFalse(has_local_branch(directory, name))
            create_branch(directory, name)
            self.assertTrue(has_local_branch(directory, name))

        # Test if we do this again, the branch didn't persist
        with temporary_github_clone("cthoyt", "cthoyt") as directory:
            self.assertFalse(has_local_branch(directory, name))

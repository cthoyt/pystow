"""Test git utilities."""

import unittest

from pystow.git import (
    clone_github_tempdir,
    create_branch,
    get_current_branch,
    guess_if_default_branch,
    has_local_branch,
)


class TestGitUtils(unittest.TestCase):
    """Test git utilities."""

    def test_temporary_github_clone(self) -> None:
        """Test temporarily cloning a GitHub repository."""
        name = "test-branch-name"

        with clone_github_tempdir("cthoyt", "cthoyt") as directory:
            self.assertTrue(directory.joinpath("README.md").is_file())
            self.assertEqual("master", get_current_branch(directory))
            self.assertTrue(guess_if_default_branch(directory))

            self.assertFalse(has_local_branch(directory, name))
            create_branch(directory, name)
            self.assertEqual(name, get_current_branch(directory))
            self.assertTrue(has_local_branch(directory, name))
            self.assertFalse(guess_if_default_branch(directory))

        # Test if we do this again, the branch didn't persist
        with clone_github_tempdir("cthoyt", "cthoyt") as directory:
            self.assertFalse(has_local_branch(directory, name))

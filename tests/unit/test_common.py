import unittest
from dbx.utils.common import update_json, ContextLockFile
from .utils import DbxTest


class CommonTest(DbxTest):
    def test_update_json(self):
        self.assertRaises(FileNotFoundError, update_json, {"key": "value"}, "/absolutely/not/existent/file/path.json")

    def test_context_lock_file(self):
        self.assertIsNone(ContextLockFile.get_context())

    def update_project_file(self):
        pass


if __name__ == "__main__":
    unittest.main()

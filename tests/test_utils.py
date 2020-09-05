import logging

from path import Path

from dbx.cli.utils import InfoFile, read_json, INFO_FILE_NAME
from .utils import DbxTest


class UtilsTest(DbxTest):
    def test_default_initialize(self):
        with self.project_dir:
            InfoFile.initialize()
            init_content = read_json(INFO_FILE_NAME)
            self.assertEqual(init_content.get("environments"), {})

    def test_non_empty_behaviour(self):
        with self.project_dir:
            logging.info(self.project_dir)
            InfoFile.initialize()
            self.assertTrue(Path(".dbx").exists())
            InfoFile.update({"environments": {"new-env": "payload"}})
            InfoFile.initialize()

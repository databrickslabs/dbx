from dbx.utils.dependency_manager import DependencyManager
from pathlib import Path
from tests.unit.sync.utils import temporary_directory
import unittest


class DependencyManagerTest(unittest.TestCase):
    def test_simple_requirements_file(self):

        with temporary_directory() as tmp:
            requirements_txt = Path(tmp) / "requirements"
            requirements_txt.write_text(
                """tqdm
rstcheck
prospector>=1.3.1,<1.7.0"""
            )

            dm = DependencyManager(
                no_rebuild="true",
                global_no_package=True,
                requirements_file=requirements_txt.resolve(),
            )
            self.assertEqual(
                dm._requirements_references,
                [
                    {"pypi": {"package": "tqdm"}},
                    {"pypi": {"package": "rstcheck"}},
                    {"pypi": {"package": "prospector<1.7.0,>=1.3.1"}},
                ],
            )

    def test_requirements_with_comments(self):
        with temporary_directory() as tmp:
            requirements_txt = Path(tmp) / "requirements"
            requirements_txt.write_text(
                """# simple comment
tqdm
rstcheck # use this library
prospector>=1.3.1,<1.7.0"""
            )

            dm = DependencyManager(
                no_rebuild="true",
                global_no_package=True,
                requirements_file=requirements_txt.resolve(),
            )
            self.assertEqual(
                dm._requirements_references,
                [
                    {"pypi": {"package": "tqdm"}},
                    {"pypi": {"package": "rstcheck"}},
                    {"pypi": {"package": "prospector<1.7.0,>=1.3.1"}},
                ],
            )

    def test_requirements_with_empty_line(self):
        with temporary_directory() as tmp:
            requirements_txt = Path(tmp) / "requirements"
            requirements_txt.write_text(
                """tqdm

rstcheck
prospector>=1.3.1,<1.7.0"""
            )

            dm = DependencyManager(
                no_rebuild="true",
                global_no_package=True,
                requirements_file=requirements_txt.resolve(),
            )
            self.assertEqual(
                dm._requirements_references,
                [
                    {"pypi": {"package": "tqdm"}},
                    {"pypi": {"package": "rstcheck"}},
                    {"pypi": {"package": "prospector<1.7.0,>=1.3.1"}},
                ],
            )

    def test_requirements_with_filtered_pyspark(self):
        with temporary_directory() as tmp:
            requirements_txt = Path(tmp) / "requirements"
            requirements_txt.write_text(
                """tqdm
pyspark==1.2.3
rstcheck
prospector>=1.3.1,<1.7.0"""
            )

            dm = DependencyManager(
                no_rebuild="true",
                global_no_package=True,
                requirements_file=requirements_txt.resolve(),
            )
            self.assertEqual(
                dm._requirements_references,
                [
                    {"pypi": {"package": "tqdm"}},
                    {"pypi": {"package": "rstcheck"}},
                    {"pypi": {"package": "prospector<1.7.0,>=1.3.1"}},
                ],
            )


if __name__ == "__main__":
    unittest.main()

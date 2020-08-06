import unittest

from dotenv import load_dotenv

load_dotenv()


class TestMlflowDeployments(unittest.TestCase):
    def smoke_test(self):
        self.assertGreater(0, 1)


if __name__ == '__main__':
    unittest.main()

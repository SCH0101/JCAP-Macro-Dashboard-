import os
import unittest
from pathlib import Path

import server_bbg


class PoolEnvLoadingTest(unittest.TestCase):
    def test_loads_pool_settings_from_project_local_env(self):
        variable_names = (
            "JCAP_POOL_URL",
            "JCAP_POOL_API_KEY",
            "JCAP_POOL_TIMEOUT",
        )
        original_values = {name: os.environ.get(name) for name in variable_names}
        original_web_dir = server_bbg.WEB_DIR

        try:
            for name in variable_names:
                os.environ.pop(name, None)
            server_bbg.WEB_DIR = str(Path(__file__).parent / "test_fixtures" / "local_env")

            server_bbg._load_pool_env()

            self.assertEqual(os.environ.get("JCAP_POOL_URL"), "https://pool.example")
            self.assertEqual(os.environ.get("JCAP_POOL_API_KEY"), "test-key")
            self.assertEqual(os.environ.get("JCAP_POOL_TIMEOUT"), "17")
        finally:
            server_bbg.WEB_DIR = original_web_dir
            for name, value in original_values.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


if __name__ == "__main__":
    unittest.main()

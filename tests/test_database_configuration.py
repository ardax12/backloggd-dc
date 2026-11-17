"""Provider-neutral configuration keeps existing deployments compatible."""

import json
import os
import subprocess
import sys
import unittest


class DatabaseConfigurationTests(unittest.TestCase):
    def read_configuration(self, **values):
        environment = dict(os.environ, PYTHON_DOTENV_DISABLED="1")
        for name in ("LIBSQL_DATABASE_URL", "LIBSQL_AUTH_TOKEN", "TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN"):
            environment.pop(name, None)
        environment.update(values)
        result = subprocess.run(
            [sys.executable, "-c", "import json; from bot.db import Database; "
             "db = Database(); print(json.dumps([db.turso_url, db.turso_token]))"],
            env=environment, capture_output=True, text=True, check=True,
        )
        return json.loads(result.stdout)

    def test_neutral_names_configure_remote_database(self):
        self.assertEqual(
            self.read_configuration(LIBSQL_DATABASE_URL="libsql://example.invalid", LIBSQL_AUTH_TOKEN="example-token"),
            ["https://example.invalid", "example-token"],
        )

    def test_old_names_remain_compatible(self):
        self.assertEqual(
            self.read_configuration(TURSO_DATABASE_URL="https://legacy.invalid", TURSO_AUTH_TOKEN="old-token"),
            ["https://legacy.invalid", "old-token"],
        )

    def test_explicit_neutral_configuration_overrides_old_names(self):
        self.assertEqual(
            self.read_configuration(LIBSQL_DATABASE_URL="", LIBSQL_AUTH_TOKEN="", TURSO_DATABASE_URL="https://legacy.invalid", TURSO_AUTH_TOKEN="old-token"),
            ["", ""],
        )

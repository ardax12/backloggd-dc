"""Startup must reject missing/example cookies before opening any resources."""

import os
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import main


class StartupCredentialTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_or_placeholder_cookie_stops_before_bot_and_health_server(self):
        for cookie in (None, "", "   ", "your_backloggd_session_cookie_here"):
            with (
                self.subTest(cookie=cookie),
                patch.object(main, "DISCORD_TOKEN", "configured-token"),
                patch.dict(os.environ, {"PORT": "8080"}),
                patch.object(main, "BackloggdBot") as bot,
                patch.object(main, "start_health_server", new_callable=AsyncMock) as health,
            ):
                if cookie is None:
                    os.environ.pop("BACKLOGGD_SESSION", None)
                else:
                    os.environ["BACKLOGGD_SESSION"] = cookie
                bot.return_value.start = AsyncMock()
                with self.assertLogs("backloggd.main", level="ERROR") as logs:
                    with self.assertRaises(SystemExit) as error:
                        await main.main()
                self.assertEqual(error.exception.code, 1)
                self.assertIn("BACKLOGGD_SESSION", " ".join(logs.output))
                bot.assert_not_called()
                health.assert_not_awaited()

    async def test_configured_cookie_allows_startup_without_logging_its_value(self):
        instance = MagicMock()
        instance.__aenter__ = AsyncMock(return_value=instance)
        instance.__aexit__ = AsyncMock(return_value=False)
        instance.start = AsyncMock()
        secret = "a-private-session-value"
        with (
            patch.object(main, "DISCORD_TOKEN", "configured-token"),
            patch.dict(os.environ, {"BACKLOGGD_SESSION": secret, "PORT": ""}),
            patch.object(main, "BackloggdBot", return_value=instance),
            patch.object(main.logger, "error") as log_error,
        ):
            await main.main()
        instance.start.assert_awaited_once_with("configured-token")
        log_error.assert_not_called()

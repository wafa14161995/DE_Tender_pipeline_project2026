import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from extractors._browser_fallback import open_with_fallback
from extractors import qatar_finance


class BrowserFallbackTests(unittest.TestCase):
    def setUp(self):
        self.engines = {}
        self.browsers = {}
        for name in ('chromium', 'webkit', 'firefox'):
            browser = MagicMock()
            browser.new_context.return_value.new_page.return_value.engine_name = name
            self.browsers[name] = browser
            self.engines[name] = MagicMock()
            self.engines[name].launch.return_value = browser
        self.playwright = SimpleNamespace(**self.engines)

    def open(self, prepare, **kwargs):
        return open_with_fallback(
            self.playwright, source='test', prepare=prepare,
            context_options={'locale': 'ar', 'user_agent': 'Chrome test'},
            timeout=100, **kwargs,
        )

    def test_primary_success_does_not_launch_alternatives(self):
        self.open(lambda page: [{'id': 1}])
        self.engines['webkit'].launch.assert_not_called()
        self.engines['firefox'].launch.assert_not_called()

    def test_navigation_failure_switches_to_webkit_and_cleans_up(self):
        def prepare(page):
            if page.engine_name == 'chromium':
                raise RuntimeError('net::ERR_TIMED_OUT')
            return [{'id': 1}]
        browser, _, _, rows = self.open(
            prepare, launch_options={'headless': True, 'args': ['--no-sandbox']}
        )
        self.assertIs(browser, self.browsers['webkit'])
        self.browsers['chromium'].close.assert_called_once()
        self.assertEqual(rows, [{'id': 1}])
        self.assertNotIn('args', self.engines['webkit'].launch.call_args.kwargs)
        self.assertNotIn('user_agent', browser.new_context.call_args.kwargs)

    def test_uninstalled_primary_falls_back(self):
        self.engines['chromium'].launch.side_effect = RuntimeError('Executable missing')
        self.assertIs(self.open(lambda page: [1])[0], self.browsers['webkit'])

    def test_all_empty_reports_failure_and_closes_browsers(self):
        with self.assertRaisesRegex(RuntimeError, 'No browser could load'):
            self.open(lambda page: [])
        for browser in self.browsers.values():
            browser.close.assert_called_once()

    def test_chrome_remains_primary_for_chrome_sources(self):
        self.open(lambda page: [1], preferred='chrome')
        self.assertEqual(self.engines['chromium'].launch.call_args.kwargs['channel'], 'chrome')
        self.engines['webkit'].launch.assert_not_called()

    def test_finance_fallback_reuses_first_page_then_continues(self):
        calls = []
        def extract(page, number):
            calls.append((page.engine_name, number))
            if page.engine_name == 'chromium':
                raise RuntimeError('net::ERR_TIMED_OUT')
            return [{'id': 'new'}] if number == 1 else []
        manager = MagicMock()
        manager.__enter__.return_value = self.playwright
        with patch.object(qatar_finance, 'sync_playwright', return_value=manager), \
             patch.object(qatar_finance, 'extract_page', side_effect=extract), \
             patch.object(qatar_finance, 'record_key', side_effect=lambda r: r['id']):
            rows, pages = qatar_finance.scrape_all_pages(set())
        self.assertEqual(rows, [{'id': 'new'}])
        self.assertEqual(pages, 1)
        self.assertEqual(calls, [('chromium', 1), ('webkit', 1), ('webkit', 2)])
        self.browsers['webkit'].close.assert_called_once()


if __name__ == '__main__':
    unittest.main()

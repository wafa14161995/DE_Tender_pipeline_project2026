"""Open a source with its preferred browser, then installed alternatives."""


def open_with_fallback(playwright, *, source, prepare, context_options,
                       timeout, preferred="chromium", launch_options=None):
    order = list(dict.fromkeys([preferred, "chromium", "webkit", "firefox"]))
    errors = []
    for name in order:
        browser = None
        try:
            print(f"[{source}] Trying browser: {name}")
            options = dict(launch_options or {"headless": True})
            options.pop("channel", None)
            if name not in {"chrome", "chromium"}:
                options.pop("args", None)
            if name == "chrome":
                options["channel"] = "chrome"
            engine = playwright.chromium if name == "chrome" else getattr(playwright, name)
            browser = engine.launch(**options)
            settings = dict(context_options)
            # Let alternate engines report their own user agent.
            if name not in {"chrome", "chromium"}:
                settings.pop("user_agent", None)
            context = browser.new_context(**settings)
            page = context.new_page()
            page.set_default_timeout(timeout)
            result = prepare(page)
            if result == []:
                raise RuntimeError("No tender records found on the first page")
            print(f"[{source}] Using browser: {name}")
            return browser, context, page, result
        except Exception as exc:
            errors.append(f"{name}: {exc}")
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            print(f"[{source}] Browser {name} failed: {exc}")
    raise RuntimeError("No browser could load the tender page:\n" + "\n".join(errors))

import os
import zipfile

from fake_useragent import UserAgent
from selenium import webdriver

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def proxy_extension(proxy_host: str, proxy_port: int, proxy_user: str, proxy_password: str):
    manifest_json = """
                {
                    "version": "1.0.0",
                    "manifest_version": 2,
                    "name": "Chrome Proxy",
                    "permissions": [
                        "proxy",
                        "tabs",
                        "unlimitedStorage",
                        "storage",
                        "<all_urls>",
                        "webRequest",
                        "webRequestBlocking"
                    ],
                    "background": {
                        "scripts": ["background.js"]
                    },
                    "minimum_chrome_version":"22.0.0"
                }
                """

    background_js = """
        var config = {
                mode: "fixed_servers",
                rules: {
                  singleProxy: {
                    scheme: "http",
                    host: "%(host)s",
                    port: parseInt(%(port)d)
                  },
                  bypassList: ["foobar.com"]
                }
              };

        chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

        function callbackFn(details) {
            return {
                authCredentials: {
                    username: "%(user)s",
                    password: "%(pass)s"
                }
            };
        }

        chrome.webRequest.onAuthRequired.addListener(
                    callbackFn,
                    {urls: ["<all_urls>"]},
                    ['blocking']
        );
            """ % {
        "host": proxy_host,
        "port": proxy_port,
        "user": proxy_user,
        "pass": proxy_password,
    }

    plugin_path = os.path.join(BASE_DIR, f"proxy_auth_plugin_{proxy_host}.zip")

    with zipfile.ZipFile(plugin_path, 'w') as zp:
        zp.writestr("manifest.json", manifest_json)
        zp.writestr("background.js", background_js)

    return plugin_path


def driver_init(proxy_host: str, proxy_port: int, proxy_user: str, proxy_password: str):
    proxy_extension_path = proxy_extension(proxy_host, proxy_port, proxy_user, proxy_password)
    ua = UserAgent()
    driver_options = webdriver.ChromeOptions()
    driver_options.add_argument("--start-maximized")
    driver_options.add_argument('--no-default-browser-check')
    driver_options.add_argument('--no-first-run')
    driver_options.add_argument('--disable-gpu')
    driver_options.add_argument("--disable-blink-features")
    driver_options.add_argument("--disable-blink-features=AutomationControlled")
    driver_options.add_experimental_option('useAutomationExtension', False)
    driver_options.add_experimental_option("excludeSwitches", ['enable-automation'])
    driver_options.add_argument(f'user-agent={ua.random}')
    driver_options.add_argument('--disable-infobars')
    driver_options.add_extension(proxy_extension_path)

    driver = webdriver.Chrome(os.path.join(BASE_DIR, "webdriver\\chromedriver.exe"), options=driver_options)

    os.remove(proxy_extension_path)

    return driver

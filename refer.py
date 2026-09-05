#!/usr/bin/env python3
"""
StockGro Multi-Threaded Referral Runner with Telegram Bot
Uses Firebase panel devices for phone numbers + OTP
"""

import sys
import os
import time
import random
import json
import base64
import re
import uuid
import urllib.parse
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from threading import Lock, Event
from collections import Counter
from typing import Optional, List, Dict, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    import requests
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError as err:
    print(f"Missing required library: {err}")
    print("Please install requirements: pip install requests cryptography")
    sys.exit(1)

# ===================== TELEGRAM BOT CONFIG =====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_ADMIN_ID = os.environ.get("TELEGRAM_ADMIN_ID", "")

# ===================== RSA KEY =====================
RSA_PEM = """-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAzznL21S0K5qWULt8XKHA
gPmvCptGqg3n38ANqhe1ZZbbSi8o0mfNtQfAviwMNJWrf4N6gu1bPhva8pegEIGk
qGXEe2FW11y1i1Ak/ngmtsTW4438oIQ5bvMDFmAEf/1L2Atkz5jyxAJVIVSSUog1
h6CWfX/0ShBC+ApLPauKxv4K7qebp8DyOdHhATA7x6nWOBp57k3Ph97oyF0wxXOQ
Qskfbz9RBmRSBQ/gVWl22UAfjUzmSLmiaYR0BHU9W1MmRdjz+0Fplm6fOUd/Zn74
9Jt8qSBzzQZHd3Jl1rJBlP3bVN5V9o16aaTRYWwXAPNNZhjdZUEoK2ZlJTKc+esF
l7zze23SvmzfxmeoF5sThcptUXEqrNrr2zW2wnE+tKLWUDV98DR5STei0jfEegOB
FV7SrjHNyZrCmaA9jhzJurR2HRaq50lVm1Xqcs9Z7RVIE+N+Azq8PYIydsCHJ+Bq
LZP+zV4LAvRo0tGvQN5JRIAZyjxV/NRB39xUPuI20vWjF92IBqFDCe5nVRB1XrrS
RW7KqaBPEQm1rHMCjWRfbgSdi87t43hc4F0Q0hlmB+5IUhQw1PDHgU32Umg/Jldgl
ARwjQpK+5ogGXL6ev1bFDv62G+tpFxGTtDJ9BFUN2shwfvDbk/C8AJiU+6xf9qZ0
0IEoYVcAvHSol+43rXPhnrsCAwEAAQ==
-----END PUBLIC KEY-----"""

# ===================== SHARED STATE =====================
print_lock = Lock()
stats_lock = Lock()
success_count = 0
failed_count = 0
attempt_count = 0
target_count = 0
stop_event = Event()
in_flight = 0
last_fail_reason = ""
is_running = False
task_start_time = None
firebase_url = ""
referral_code = ""
num_workers = 3
config_state = {}

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def _set_fail(reason: str):
    global failed_count, last_fail_reason
    failed_count += 1
    clean = " ".join(str(reason).split())
    if len(clean) > 90:
        clean = clean[:87] + "..."
    last_fail_reason = clean

# ===================== CRYPTO =====================
class CryptoLayer:
    def __init__(self, pem_str: str):
        self.public_key = serialization.load_pem_public_key(pem_str.encode("utf-8"))

    def encrypt_payload(self, payload: dict) -> dict:
        plaintext = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        aes_key = os.urandom(32)
        nonce = os.urandom(12)
        aesgcm = AESGCM(aes_key)
        ciphertext_tag = aesgcm.encrypt(nonce, plaintext, None)
        aes_b64 = base64.b64encode(aes_key).decode("utf-8")
        timestamp_sec = str(int(time.time()))
        rsa_plaintext = f"{aes_b64}-{timestamp_sec}".encode("utf-8")
        encrypted_aes_key = self.public_key.encrypt(
            rsa_plaintext,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )
        return {
            "encrypted_aes_key": base64.b64encode(encrypted_aes_key).decode(),
            "encrypted_data": base64.b64encode(ciphertext_tag).decode(),
            "nonce": base64.b64encode(nonce).decode(),
        }

# ===================== HEADERS =====================
class HeaderProfile:
    def __init__(self):
        self.luid = str(uuid.uuid4())
        self.session_id = str(uuid.uuid4())
        epoch_ms = int(time.time() * 1000) - random.randint(0, 86400000)
        rand_suffix = random.randint(1000000000000000000, 9999999999999999999)
        self.appflyer_uid = f"{epoch_ms}-{rand_suffix}"
        self.posthog_user_id = str(uuid.uuid4())
        we_suffix = uuid.uuid4().hex
        self.we_anonymous_id = (
            f"000001a0-{we_suffix[0:4]}-{we_suffix[4:8]}-"
            f"{we_suffix[8:12]}-{we_suffix[12:24]}"
        )

    def base_headers(self, login_id: str = "") -> dict:
        return {
            "platform": "android",
            "loginid": login_id,
            "sessionid": self.session_id,
            "stockgro-luid": self.luid,
            "user-agent": "StockGro_Android_325",
            "version_code": "325",
            "version-code": "325",
            "x-appflyer-uid": self.appflyer_uid,
            "posthog-user-id": self.posthog_user_id,
            "we-anonymous-id": self.we_anonymous_id,
            "content-type": "application/json; charset=UTF-8",
            "accept-encoding": "gzip",
        }

# ===================== FIREBASE PANEL =====================
def parse_panel_link(link: str) -> Optional[str]:
    if not link:
        return None
    link = link.strip()
    if link.startswith("https://") and (
        "firebaseio.com" in link or "firebasedatabase.app" in link
    ):
        firebase_url = link
        if not firebase_url.endswith("/"):
            firebase_url += "/"
        return firebase_url
    parsed_url = urllib.parse.urlparse(link)
    qs = urllib.parse.parse_qs(parsed_url.query)
    if "s" not in qs:
        return None
    s_param = qs["s"][0] + "=" * ((4 - len(qs["s"][0]) % 4) % 4)
    try:
        decoded = base64.b64decode(s_param).decode("utf-8").split("|")[0].strip()
        if not decoded.endswith("/"):
            decoded += "/"
        return decoded
    except Exception:
        return None

def extract_phone_from_messages(messages) -> Optional[str]:
    if not messages or not isinstance(messages, dict):
        return None
    
    patterns = [
        re.compile(r"\b(?:\+91|91|0)?([6-9]\d{9})\b"),
        re.compile(r"\b(?:phone|mobile|number)[\s:]*([6-9]\d{9})\b", re.IGNORECASE),
        re.compile(r"[^0-9]([6-9]\d{9})[^0-9]"),
    ]
    counts = Counter()
    for msg in messages.values():
        if not isinstance(msg, dict):
            continue
        text = str(msg.get("body") or msg.get("message") or msg.get("text") or "")
        for pat in patterns:
            for num in pat.findall(text):
                counts[num] += 1
    if not counts:
        return None
    return counts.most_common(1)[0][0]

def extract_phones_and_clients(firebase_url: str) -> List[Dict]:
    try:
        c_resp = requests.get(firebase_url + "clients.json", timeout=30)
        c_resp.raise_for_status()
        clients_data = c_resp.json()
        if not isinstance(clients_data, dict):
            clients_data = {}
    except Exception as e:
        safe_print(f"[Firebase] Failed to fetch clients: {e}")
        return []

    online_devices = [
        c_id
        for c_id, c_data in clients_data.items()
        if isinstance(c_data, dict) and c_data.get("status")
    ]
    if not online_devices:
        return []

    result = []
    seen = set()

    def fetch_device_number(c_id: str):
        try:
            m_req = requests.get(
                f'{firebase_url}messages/{c_id}.json?orderBy="$key"&limitToLast=20',
                timeout=15,
            )
            m_req.raise_for_status()
            device_messages = m_req.json()
            if not isinstance(device_messages, dict):
                device_messages = {}
            phone = extract_phone_from_messages(device_messages)
            if phone:
                return {"client_id": c_id, "phone": phone}
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(fetch_device_number, c_id) for c_id in online_devices]
        for future in futures:
            try:
                res = future.result(timeout=30)
                if res and res["phone"] not in seen:
                    seen.add(res["phone"])
                    result.append(res)
            except Exception:
                pass

    safe_print(f"[Firebase] Found {len(result)} unique phone number(s)")
    return result

def fetch_stockgro_otp(firebase_url: str, device_id: str, timeout: int = 100) -> Optional[str]:
    start_time = time.time()
    trigger_time = int(time.time() * 1000)

    otp_patterns = [
        r"(\d{4,6})\s+is your verification code for StockGro",
        r"(?i)stockgro.*?(\d{4,6})",
        r"(?i)(\d{4,6}).*?stockgro",
        r"(?i)OTP[:\s]*(\d{4,6})",
        r"(?i)verification code[:\s]*(\d{4,6})",
        r"(?<!\d)(\d{6})(?!\d)",
        r"(?<!\d)(\d{4})(?!\d)",
    ]

    while (time.time() - start_time) < timeout:
        if stop_event.is_set():
            return None
        
        try:
            url = f"{firebase_url}messages/{device_id}.json"
            resp = requests.get(url, timeout=8)
            if resp.status_code != 200:
                time.sleep(1.5)
                continue
            
            msgs = resp.json()
            if not msgs or not isinstance(msgs, dict):
                time.sleep(1.5)
                continue

            for msg_id in sorted(msgs.keys(), reverse=True):
                msg_data = msgs[msg_id]
                if not isinstance(msg_data, dict):
                    continue
                
                try:
                    msg_ts = int(msg_id)
                    if msg_ts < trigger_time - 5000:
                        continue
                except (ValueError, TypeError):
                    pass

                sender = (msg_data.get("sender") or "").lower()
                body = (
                    msg_data.get("body")
                    or msg_data.get("message")
                    or msg_data.get("text")
                    or ""
                )
                body_lower = body.lower()

                is_stockgro = (
                    "stockgro" in sender
                    or "stockgro" in body_lower
                    or "verification code for stockgro" in body_lower
                )

                for pattern in otp_patterns:
                    match = re.search(pattern, body, re.IGNORECASE)
                    if match:
                        otp = match.group(1) if match.groups() else match.group(0)
                        if otp.isdigit() and len(otp) in (4, 6):
                            if is_stockgro or len(otp) == 6:
                                return otp
            time.sleep(1.5)
        except requests.RequestException:
            time.sleep(1.5)
        except Exception:
            time.sleep(1.5)

    return None

class FirebaseDevicePool:
    def __init__(self, firebase_url: str):
        self.firebase_url = firebase_url
        self._lock = Lock()
        self._available: List[Dict] = []
        self._in_use: Dict[str, Dict] = {}
        self._used_phones: set = set()
        self.refresh()

    def refresh(self) -> int:
        devices = extract_phones_and_clients(self.firebase_url)
        with self._lock:
            fresh = []
            for d in devices:
                phone = d["phone"]
                if phone in self._used_phones or phone in self._in_use:
                    continue
                if any(x.get("phone") == phone for x in self._available):
                    continue
                fresh.append(d)
            self._available.extend(fresh)
            return len(self._available)

    def allocate(self) -> Optional[Dict]:
        with self._lock:
            while self._available:
                dev = self._available.pop(0)
                phone = dev.get("phone")
                if phone in self._used_phones:
                    continue
                self._in_use[phone] = dev
                return dict(dev)
            return None

    def release(self, phone: str, mark_used: bool = False):
        with self._lock:
            self._in_use.pop(phone, None)
            if mark_used:
                self._used_phones.add(phone)

    def available_count(self) -> int:
        with self._lock:
            return len(self._available)

    def mark_used(self, phone: str):
        with self._lock:
            self._used_phones.add(phone)
            self._in_use.pop(phone, None)

# ===================== STOCKGRO CLIENT =====================
class StockGroClient:
    def __init__(self, crypto: CryptoLayer, profile: HeaderProfile):
        self.crypto = crypto
        self.profile = profile
        self.base_url = "https://prod.stockgro.com"
        self.session = requests.Session()
        self.sguardian_sid = ""
        self.phone_number = ""
        self.access_token = ""
        self.user_uuid = ""
        self.last_otp = ""

    def _parse(self, path: str, resp) -> dict:
        try:
            data = resp.json()
        except Exception:
            raise RuntimeError(
                f"Non-JSON response from {path} ({resp.status_code}): {resp.text[:200]}"
            )
        success = data.get("success", True)
        if not success or resp.status_code >= 400:
            msg = data.get("message") or str(data.get("error_code", "unknown"))
            raise RuntimeError(f"API Error [{path}] HTTP {resp.status_code}: {msg}")
        return data.get("data", data)

    def _get(self, path: str, params: dict = None, auth: bool = False) -> dict:
        url = self.base_url + path
        headers = self.profile.base_headers()
        if auth:
            headers["authorization"] = f"Bearer {self.access_token}"
        resp = self.session.get(url, params=params, headers=headers, timeout=18)
        resp.raise_for_status()
        return self._parse(path, resp)

    def _post(self, path: str, payload: dict, encrypt: bool = True, auth: bool = False) -> dict:
        url = self.base_url + path
        headers = self.profile.base_headers()
        if auth:
            headers["authorization"] = f"Bearer {self.access_token}"
        body = self.crypto.encrypt_payload(payload) if encrypt else payload
        resp = self.session.post(url, json=body, headers=headers, timeout=18)
        resp.raise_for_status()
        return self._parse(path, resp)

    def version_check(self) -> bool:
        try:
            data = self._get("/heimdall/public/version/check")
            return data.get("update_type", "") == "NONE"
        except Exception:
            return True

    def get_countries(self) -> dict:
        try:
            return self._get("/heimdall/public/v1/countries")
        except Exception:
            return {}

    def verify_identity(self, phone_number: str) -> dict:
        self.phone_number = phone_number
        body = {
            "identity": phone_number,
            "identity_type": "phone",
            "country_code": "IN",
        }
        return self._post("/sguardian/auth/v2/verify/identity", body, encrypt=True)

    def initiate_otp(self, phone_number: str, referral_code: str = "") -> str:
        self.phone_number = phone_number
        body = {
            "phone": phone_number,
            "email": "",
            "country_code": "IN",
            "referral_code": referral_code,
            "flow": "signup",
            "otp_channel": "sms",
            "mixpanel_id": "",
            "user_id": "",
        }
        data = self._post("/sguardian/auth/v2/initiate", body, encrypt=True)
        self.sguardian_sid = data.get("session_id", "")
        if not self.sguardian_sid:
            raise RuntimeError("No session_id received in initiate response")
        return self.sguardian_sid

    def proceed_otp(self, otp_code: str) -> dict:
        self.last_otp = otp_code.strip()
        body = {
            "phone": self.phone_number,
            "email": "",
            "country_code": "IN",
            "pin": "",
            "otp": self.last_otp,
            "session_id": self.sguardian_sid,
            "flow": "signup",
            "user_id": "",
            "whatsapp_consent": True,
            "biometric_verified": False,
            "fcm_token": "",
        }
        return self._post("/sguardian/auth/v2/proceed", body, encrypt=True)

    def register(self, name: str, referral_code: str = "") -> str:
        body = {
            "phone": self.phone_number,
            "email": "",
            "display_name": name,
            "otp": self.last_otp,
            "country_code": "IN",
            "session_id": self.sguardian_sid,
            "referral_code": referral_code,
            "whatsapp_consent": True,
            "mixpanel_id": "",
            "fcm_token": "",
        }
        data = self._post("/sguardian/auth/v2/register", body, encrypt=True)
        self.access_token = data.get("access_token", "")
        if not self.access_token:
            raise RuntimeError("No access_token in register response")
        return self.access_token

# ===================== HELPERS =====================
def extract_referral_code(raw: str) -> str:
    raw = raw.strip()
    if "onelink.me" in raw:
        url_match = re.search(r"https?://[^\s]+", raw)
        if url_match:
            try:
                r = requests.get(url_match.group(0), allow_redirects=False, timeout=6)
                loc = r.headers.get("Location", "")
                if loc:
                    parsed = urllib.parse.urlparse(loc)
                    qs = urllib.parse.parse_qs(parsed.query)
                    if "deep_link_sub2" in qs and qs["deep_link_sub2"]:
                        return qs["deep_link_sub2"][0].strip()
                    if "refcode" in qs and qs["refcode"] and qs["refcode"][0]:
                        return qs["refcode"][0].strip()
            except Exception:
                pass
    m = re.search(r"\b([A-Z0-9]{7,9})\b", raw.upper())
    if m:
        return m.group(1).strip()
    return raw

def random_display_name() -> str:
    first_names = [
        "Raj", "Priya", "Ankit", "Kiran", "Dev", "Arjun", "Neha", "Rohan",
        "Siddharth", "Aarav", "Vikram", "Sneha", "Rahul", "Pooja", "Amit",
        "Kavita", "Manish", "Deepa",
    ]
    last_names = [
        "Kumar", "Sharma", "Singh", "Patel", "Gupta", "Mehta", "Joshi",
        "Verma", "Chopra", "Reddy",
    ]
    return f"{random.choice(first_names)} {random.choice(last_names)}"

def render_progress_bar(current: int, total: int, width: int = 20) -> str:
    if total <= 0:
        return "[" + "░" * width + "] 0%"
    progress = min(1.0, current / total)
    filled = int(width * progress)
    bar = "█" * filled + "░" * (width - filled)
    percent = int(progress * 100)
    return f"[{bar}] {percent}% ({current}/{total})"

# ===================== WORKER =====================
def register_one_account(
    crypto: CryptoLayer,
    device_pool: FirebaseDevicePool,
    referral_code: str,
    worker_id: int,
) -> dict:
    global success_count, failed_count, attempt_count, in_flight, last_fail_reason

    if stop_event.is_set():
        return {"success": False, "skipped": True}

    phone = ""
    client_id = ""
    try:
        with stats_lock:
            if success_count >= target_count:
                stop_event.set()
                return {"success": False, "skipped": True}
            attempt_count += 1
            current_attempt = attempt_count
            in_flight += 1

        profile = HeaderProfile()
        client = StockGroClient(crypto, profile)

        try:
            client.version_check()
            client.get_countries()
        except Exception:
            pass

        if stop_event.is_set():
            return {"success": False, "skipped": True}

        dev = device_pool.allocate()
        if not dev:
            device_pool.refresh()
            dev = device_pool.allocate()
        if not dev:
            with stats_lock:
                _set_fail("No available Firebase devices")
            safe_print(f"[W{worker_id}] FAILED → No available Firebase devices")
            return {"success": False, "error": "No available Firebase devices"}

        phone = dev.get("phone", "")
        client_id = dev.get("client_id", "")
        safe_print(
            f"[W{worker_id}] Attempt #{current_attempt} | "
            f"Device: {client_id[:12] if len(client_id) > 12 else client_id}… | +91{phone}"
        )

        if stop_event.is_set():
            device_pool.release(phone, mark_used=False)
            return {"success": False, "skipped": True}

        identity = client.verify_identity(phone)
        flow = identity.get("default_flow", "")
        if flow != "signup":
            reason = f"Phone already registered (flow: {flow})"
            device_pool.release(phone, mark_used=True)
            with stats_lock:
                _set_fail(reason)
            safe_print(f"[W{worker_id}] FAILED → +91{phone} | {reason}")
            return {"success": False, "error": reason, "phone": phone}

        if stop_event.is_set():
            device_pool.release(phone, mark_used=False)
            return {"success": False, "skipped": True}

        safe_print(f"[W{worker_id}] +91{phone} → Requesting OTP...")
        client.initiate_otp(phone, referral_code=referral_code)

        safe_print(f"[W{worker_id}] +91{phone} → Waiting for SMS OTP via Firebase...")
        otp = fetch_stockgro_otp(device_pool.firebase_url, client_id, timeout=100)
        if not otp:
            device_pool.release(phone, mark_used=False)
            reason = "OTP timeout (Firebase)"
            with stats_lock:
                _set_fail(reason)
            safe_print(f"[W{worker_id}] FAILED → +91{phone} | {reason}")
            return {"success": False, "error": reason, "phone": phone}

        safe_print(f"[W{worker_id}] +91{phone} → OTP captured: {otp}")

        if stop_event.is_set():
            device_pool.release(phone, mark_used=False)
            return {"success": False, "skipped": True}

        client.proceed_otp(otp)

        display_name = random_display_name()
        safe_print(f"[W{worker_id}] +91{phone} → Registering as '{display_name}'...")
        client.register(display_name, referral_code=referral_code)

        device_pool.mark_used(phone)

        with stats_lock:
            success_count += 1
            current_success = success_count
            if current_success >= target_count:
                stop_event.set()

        safe_print(
            f"[W{worker_id}] SUCCESS → +91{phone} registered as '{display_name}'  |  "
            f"Total: {current_success}/{target_count}"
        )
        return {"success": True, "phone": phone, "name": display_name}

    except Exception as ex:
        err_msg = str(ex)
        if phone:
            mark = (
                "already registered" in err_msg.lower()
                or "duplicate" in err_msg.lower()
            )
            device_pool.release(phone, mark_used=mark)
        if "Stopped early" in err_msg or "skipped" in err_msg.lower():
            return {"success": False, "skipped": True}
        with stats_lock:
            _set_fail(err_msg)
        phone_part = f"+91{phone} | " if phone else ""
        safe_print(f"[W{worker_id}] FAILED → {phone_part}{err_msg}")
        return {"success": False, "error": err_msg, "phone": phone}

    finally:
        with stats_lock:
            in_flight = max(0, in_flight - 1)

# ===================== TELEGRAM BOT =====================
class TelegramBot:
    def __init__(self, token: str, admin_id: str):
        self.token = token
        self.admin_id = admin_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.offset = 0
        self.running = False
        self.config_state = {}
        self.startup_message_sent = False
        
    def send_message(self, chat_id: str, text: str, keyboard: dict = None):
        url = f"{self.base_url}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        if keyboard:
            data["reply_markup"] = json.dumps(keyboard)
        
        try:
            response = requests.post(url, json=data, timeout=10)
            return response.json()
        except Exception as e:
            print(f"Telegram send error: {e}")
            return None
    
    def send_to_admin(self, text: str, keyboard: dict = None):
        return self.send_message(self.admin_id, text, keyboard)
    
    def get_updates(self):
        url = f"{self.base_url}/getUpdates"
        params = {
            "offset": self.offset,
            "timeout": 30,
            "allowed_updates": ["message", "callback_query"]
        }
        try:
            response = requests.get(url, params=params, timeout=35)
            if response.status_code == 200:
                return response.json().get("result", [])
        except Exception as e:
            print(f"Telegram get updates error: {e}")
        return []
    
    def answer_callback(self, callback_id: str, text: str = "", show_alert: bool = False):
        url = f"{self.base_url}/answerCallbackQuery"
        data = {
            "callback_query_id": callback_id,
            "text": text,
            "show_alert": show_alert
        }
        try:
            requests.post(url, json=data, timeout=10)
        except Exception:
            pass
    
    def send_startup_message(self):
        """Send startup message to admin when bot starts"""
        keyboard = {
            "inline_keyboard": [
                [{"text": "▶️ Start Referrals", "callback_data": "start"}],
                [{"text": "📊 Status", "callback_data": "status"}],
                [{"text": "⚙️ Set Config", "callback_data": "config"}],
                [{"text": "⏹️ Stop", "callback_data": "stop"}]
            ]
        }
        self.send_to_admin(
            "🤖 <b>StockGro Referral Bot is ONLINE!</b>\n\n"
            "✅ Bot started successfully on Railway!\n"
            "🔄 Ready to create StockGro accounts.\n\n"
            "📌 Use /start to see all commands\n"
            "⚙️ Use /config to set up your settings\n"
            "▶️ Use /start_ref to begin referrals",
            keyboard
        )
        self.startup_message_sent = True
    
    def run(self):
        self.running = True
        print("Telegram bot started listening...")
        
        # Send startup message
        self.send_startup_message()
        
        while self.running:
            try:
                updates = self.get_updates()
                for update in updates:
                    self.process_update(update)
                    self.offset = update.get("update_id", 0) + 1
            except Exception as e:
                print(f"Bot error: {e}")
                time.sleep(2)
    
    def process_update(self, update: dict):
        if "callback_query" in update:
            self.process_callback(update["callback_query"])
        elif "message" in update:
            self.process_message(update["message"])
    
    def process_message(self, message: dict):
        chat_id = str(message.get("chat", {}).get("id", ""))
        if chat_id != self.admin_id:
            return
        
        text = message.get("text", "")
        if not text:
            return
        
        if text.startswith("/"):
            self.process_command(chat_id, text)
        else:
            self.handle_config_input(chat_id, text)
    
    def process_command(self, chat_id: str, command: str):
        if command == "/start":
            keyboard = {
                "inline_keyboard": [
                    [{"text": "▶️ Start Referrals", "callback_data": "start"}],
                    [{"text": "⏹️ Stop", "callback_data": "stop"}],
                    [{"text": "📊 Status", "callback_data": "status"}],
                    [{"text": "⚙️ Set Config", "callback_data": "config"}]
                ]
            }
            self.send_message(
                chat_id,
                "🤖 <b>StockGro Referral Bot</b>\n\n"
                "This bot creates StockGro accounts using Firebase panel OTP.\n\n"
                "📌 <b>Commands:</b>\n"
                "/start - Show this menu\n"
                "/status - Check current status\n"
                "/config - Configure settings\n"
                "/start_ref - Start referral process\n"
                "/stop - Stop current process\n"
                "/help - Show help\n\n"
                "<b>First, set up your config:</b>\n"
                "1. Send Firebase panel link\n"
                "2. Send referral code\n"
                "3. Send target count\n"
                "4. Use /start_ref to begin",
                keyboard
            )
        elif command == "/status":
            self.send_status(chat_id)
        elif command == "/config":
            self.send_config_prompt(chat_id)
        elif command == "/start_ref":
            self.start_referral_process(chat_id)
        elif command == "/stop":
            self.stop_referral_process(chat_id)
        elif command == "/help":
            self.send_help(chat_id)
    
    def process_callback(self, callback: dict):
        chat_id = str(callback.get("message", {}).get("chat", {}).get("id", ""))
        if chat_id != self.admin_id:
            return
        
        data = callback.get("data", "")
        callback_id = callback.get("id", "")
        
        if data == "start":
            self.start_referral_process(chat_id)
            self.answer_callback(callback_id, "Starting referral process...")
        elif data == "stop":
            self.stop_referral_process(chat_id)
            self.answer_callback(callback_id, "Stopping...")
        elif data == "status":
            self.send_status(chat_id)
            self.answer_callback(callback_id, "Status updated")
        elif data == "config":
            self.send_config_prompt(chat_id)
            self.answer_callback(callback_id, "Config prompt sent")
    
    def send_status(self, chat_id: str):
        global success_count, failed_count, attempt_count, target_count, is_running, task_start_time, in_flight, last_fail_reason
        
        status_text = "📊 <b>Status Report</b>\n\n"
        
        if is_running:
            status_text += "🟢 <b>Status:</b> Running\n"
        else:
            status_text += "🔴 <b>Status:</b> Stopped\n"
        
        status_text += f"📝 <b>Target:</b> {target_count}\n"
        status_text += f"✅ <b>Success:</b> {success_count}\n"
        status_text += f"❌ <b>Failed:</b> {failed_count}\n"
        status_text += f"🔄 <b>Attempts:</b> {attempt_count}\n"
        status_text += f"📡 <b>In Flight:</b> {in_flight}\n"
        
        if task_start_time:
            elapsed = int(time.time() - task_start_time)
            hours = elapsed // 3600
            minutes = (elapsed % 3600) // 60
            seconds = elapsed % 60
            status_text += f"⏱️ <b>Running:</b> {hours:02d}:{minutes:02d}:{seconds:02d}\n"
        
        if target_count > 0:
            progress = (success_count / target_count) * 100
            status_text += f"📈 <b>Progress:</b> {progress:.1f}%\n"
        
        if last_fail_reason:
            status_text += f"\n⚠️ <b>Last Error:</b> {last_fail_reason}"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "🔄 Refresh", "callback_data": "status"}]
            ]
        }
        self.send_message(chat_id, status_text, keyboard)
    
    def send_config_prompt(self, chat_id: str):
        config_text = (
            "⚙️ <b>Configuration Setup</b>\n\n"
            "Please send the following information <b>one by one</b>:\n\n"
            "1️⃣ <b>Firebase Panel Link</b>\n"
            "   (e.g., https://your-project.firebaseio.com/)\n\n"
            "2️⃣ <b>Referral Code</b>\n"
            "   (e.g., ABC12345)\n\n"
            "3️⃣ <b>Target Count</b>\n"
            "   (number of accounts to create)\n\n"
            "4️⃣ <b>Workers</b> (optional, default: 3)\n\n"
            "Send each item as a separate message."
        )
        self.send_message(chat_id, config_text)
    
    def handle_config_input(self, chat_id: str, text: str):
        global firebase_url, referral_code, num_workers, target_count
        
        text = text.strip()
        
        # Firebase URL
        if "firebase" in text.lower() or "firebaseio.com" in text or "firebasedatabase.app" in text:
            self.config_state["firebase_url"] = text
            self.send_message(chat_id, f"✅ Firebase URL set!\n\nNow send your <b>Referral Code</b>")
            return
        
        # Referral Code
        if re.match(r"^[A-Za-z0-9]{5,12}$", text) and not self.config_state.get("referral_code"):
            self.config_state["referral_code"] = text.upper()
            self.send_message(chat_id, f"✅ Referral Code set: <code>{text.upper()}</code>\n\nNow send <b>Target Count</b> (number of accounts)")
            return
        
        # Target Count
        if text.isdigit() and not self.config_state.get("target_count"):
            count = int(text)
            if count > 0:
                self.config_state["target_count"] = count
                self.send_message(
                    chat_id, 
                    f"✅ Target Count set: {count}\n\nNow send <b>Workers</b> (1-8, or skip for default 3)"
                )
            return
        
        # Workers
        if text.isdigit() and self.config_state.get("target_count") and not self.config_state.get("workers"):
            workers = int(text)
            if 1 <= workers <= 8:
                self.config_state["workers"] = workers
                # Apply config
                firebase_url = parse_panel_link(self.config_state.get("firebase_url", ""))
                referral_code = extract_referral_code(self.config_state.get("referral_code", ""))
                target_count = self.config_state.get("target_count", 0)
                num_workers = self.config_state.get("workers", 3)
                
                self.send_message(
                    chat_id,
                    f"✅ <b>Configuration Complete!</b>\n\n"
                    f"📋 <b>Settings:</b>\n"
                    f"🔥 Firebase: {self.config_state.get('firebase_url', 'N/A')[:50]}...\n"
                    f"🔗 Referral: <code>{referral_code}</code>\n"
                    f"🎯 Target: {target_count}\n"
                    f"👷 Workers: {num_workers}\n\n"
                    f"Use /start_ref to begin or /status to check."
                )
            else:
                self.send_message(chat_id, "❌ Workers must be between 1 and 8.")
        else:
            # Smart detection
            if not self.config_state.get("firebase_url"):
                self.config_state["firebase_url"] = text
                self.send_message(chat_id, f"✅ Firebase URL set!\n\nNow send your <b>Referral Code</b>")
            elif not self.config_state.get("referral_code"):
                self.config_state["referral_code"] = text.upper()
                self.send_message(chat_id, f"✅ Referral Code set: <code>{text.upper()}</code>\n\nNow send <b>Target Count</b>")
            elif not self.config_state.get("target_count") and text.isdigit():
                count = int(text)
                if count > 0:
                    self.config_state["target_count"] = count
                    self.send_message(chat_id, f"✅ Target Count set: {count}\n\nNow send <b>Workers</b> (1-8)")
            else:
                self.send_message(
                    chat_id,
                    "❌ I couldn't understand that.\n\n"
                    "Please send items in order:\n"
                    "1. Firebase URL\n"
                    "2. Referral Code\n"
                    "3. Target Count\n"
                    "4. Workers (optional)"
                )
    
    def start_referral_process(self, chat_id: str):
        global is_running, target_count, firebase_url, referral_code, num_workers, task_start_time
        global success_count, failed_count, attempt_count, in_flight, stop_event
        
        if is_running:
            self.send_message(chat_id, "⚠️ Process is already running! Use /stop to stop it.")
            return
        
        if not firebase_url or not referral_code or target_count <= 0:
            self.send_message(
                chat_id,
                "❌ Configuration incomplete!\n\n"
                "Please set up your config first using /config\n"
                "Or send:\n"
                "1. Firebase URL\n"
                "2. Referral Code\n"
                "3. Target Count\n"
                "4. Workers (optional)"
            )
            return
        
        # Reset counters
        success_count = 0
        failed_count = 0
        attempt_count = 0
        in_flight = 0
        stop_event.clear()
        is_running = True
        task_start_time = time.time()
        
        self.send_message(
            chat_id,
            f"🚀 <b>Starting referral process...</b>\n\n"
            f"🎯 Target: {target_count}\n"
            f"👷 Workers: {num_workers}\n"
            f"🔗 Referral: <code>{referral_code}</code>\n\n"
            f"🔄 Process running in background. Use /status to check progress."
        )
        
        # Start the referral process in a separate thread
        threading.Thread(target=self.run_referral_process, daemon=True).start()
    
    def run_referral_process(self):
        global success_count, failed_count, attempt_count, in_flight, stop_event
        global is_running, task_start_time, target_count, referral_code, num_workers, firebase_url
        
        try:
            crypto = CryptoLayer(RSA_PEM)
            device_pool = FirebaseDevicePool(firebase_url)
            
            avail = device_pool.available_count()
            if avail == 0:
                self.send_to_admin("❌ No devices with phone numbers found. Check panel / online devices.")
                is_running = False
                return
            
            self.send_to_admin(
                f"✅ Device pool initialized: {avail} phone(s) available\n"
                f"🎯 Target: {target_count}\n"
                f"👷 Workers: {num_workers}"
            )
            
            # Run the main referral logic
            self.run_main_loop(crypto, device_pool)
            
        except Exception as e:
            self.send_to_admin(f"❌ Process error: {str(e)}")
        finally:
            is_running = False
            elapsed = int(time.time() - (task_start_time or time.time()))
            hours = elapsed // 3600
            minutes = (elapsed % 3600) // 60
            seconds = elapsed % 60
            
            self.send_to_admin(
                f"🏁 <b>Process Finished</b>\n\n"
                f"⏱️ Time: {hours:02d}:{minutes:02d}:{seconds:02d}\n"
                f"✅ Success: {success_count}/{target_count}\n"
                f"❌ Failed: {failed_count}\n"
                f"🔄 Attempts: {attempt_count}"
            )
    
    def run_main_loop(self, crypto: CryptoLayer, device_pool: FirebaseDevicePool):
        global success_count, failed_count, attempt_count, in_flight, target_count, stop_event
        
        active_futures = set()
        empty_pool_streak = 0
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            while not stop_event.is_set() and success_count < target_count:
                slots_available = min(num_workers - len(active_futures), target_count - success_count)
                
                if slots_available <= 0:
                    time.sleep(0.5)
                    continue
                
                if device_pool.available_count() == 0:
                    device_pool.refresh()
                    if device_pool.available_count() == 0:
                        empty_pool_streak += 1
                        if empty_pool_streak >= 8 and not active_futures:
                            safe_print("No more devices available. Stopping.")
                            self.send_to_admin("⚠️ No more Firebase devices available. Stopping.")
                            break
                        time.sleep(1.0)
                        continue
                    empty_pool_streak = 0
                
                for _ in range(max(0, slots_available)):
                    if stop_event.is_set() or success_count >= target_count:
                        break
                    if device_pool.available_count() == 0:
                        break
                    
                    worker_id = (len(active_futures) % num_workers) + 1
                    fut = executor.submit(
                        register_one_account,
                        crypto,
                        device_pool,
                        referral_code,
                        worker_id,
                    )
                    active_futures.add(fut)
                
                if not active_futures:
                    time.sleep(0.5)
                    continue
                
                done, active_futures = wait(
                    active_futures, return_when=FIRST_COMPLETED, timeout=1.0
                )
                
                for f in done:
                    try:
                        f.result()
                    except Exception:
                        pass
                
                # Send status update every 5 success
                if success_count > 0 and success_count % 5 == 0:
                    self.send_to_admin(
                        f"📊 <b>Progress Update</b>\n\n"
                        f"✅ Success: {success_count}/{target_count}\n"
                        f"❌ Failed: {failed_count}\n"
                        f"📡 In-flight: {in_flight}\n"
                        f"📱 Pool: {device_pool.available_count()}"
                    )
            
            for f in active_futures:
                f.cancel()
            time.sleep(1.5)
    
    def stop_referral_process(self, chat_id: str):
        global stop_event, is_running
        
        if is_running:
            stop_event.set()
            is_running = False
            self.send_message(chat_id, "⏹️ Process stopping... Please wait for threads to finish.")
        else:
            self.send_message(chat_id, "ℹ️ No process is currently running.")
    
    def send_help(self, chat_id: str):
        help_text = (
            "📚 <b>StockGro Referral Bot Help</b>\n\n"
            "<b>Commands:</b>\n"
            "/start - Show main menu\n"
            "/status - Check current status\n"
            "/config - Configure settings\n"
            "/start_ref - Start referral process\n"
            "/stop - Stop current process\n"
            "/help - Show this help\n\n"
            "<b>Setup Guide:</b>\n"
            "1. Use /config or send items in order:\n"
            "   - Firebase panel URL\n"
            "   - Referral code\n"
            "   - Target count\n"
            "   - Workers (1-8)\n"
            "2. Use /start_ref to begin\n"
            "3. Use /status to monitor\n\n"
            "<b>Environment Variables:</b>\n"
            "TELEGRAM_BOT_TOKEN - Your bot token\n"
            "TELEGRAM_ADMIN_ID - Your Telegram user ID"
        )
        self.send_message(chat_id, help_text)

# ===================== MAIN =====================
def main():
    global firebase_url, referral_code, num_workers, target_count, TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID
    
    print("=" * 50)
    print("  StockGro Referral Bot with Telegram Control  ")
    print("=" * 50)
    print()
    
    # Check Telegram config
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_ADMIN_ID:
        print("❌ Telegram bot not configured!")
        print("   Please set TELEGRAM_BOT_TOKEN and TELEGRAM_ADMIN_ID")
        print("   as environment variables on Railway.")
        print()
        print("   Bot will keep running but won't respond to messages.")
        print("   Press Ctrl+C to stop.")
        print("=" * 50)
        
        # Keep running but with error message
        while True:
            time.sleep(60)
            print("[WARN] Telegram bot not configured. Set environment variables.")
        return
    
    print("🤖 Starting Telegram Bot...")
    print(f"   Bot Token: {TELEGRAM_BOT_TOKEN[:10]}...")
    print(f"   Admin ID: {TELEGRAM_ADMIN_ID}")
    print()
    print("Bot is running. Press Ctrl+C to stop.")
    print("=" * 50)
    
    # Initialize and run bot
    telegram_bot = TelegramBot(TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID)
    
    try:
        telegram_bot.run()
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
    except Exception as e:
        print(f"Bot error: {e}")
        time.sleep(5)

if __name__ == "__main__":
    main()

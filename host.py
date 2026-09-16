"""
HOSTING BOT VERSION 5.0
Single-Admin | Real Module Manager | Admin Panel | Testing | Backup/Restore
"""

import subprocess
import sys
import os
import site
import importlib

def auto_install(import_name, package_name=None):
    package_name = package_name or import_name
    try:
        __import__(import_name)
    except ModuleNotFoundError:
        print(f"[auto-install] Installing {package_name} ...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", package_name])
            importlib.invalidate_caches()
            importlib.import_module(import_name)
            print(f"[auto-install] Installed {package_name}")
        except Exception as e:
            print(f"[auto-install] FAILED {package_name}: {e}")

for _imp, _pkg in [
    ("telebot", "pyTelegramBotAPI"),
    ("psutil", "psutil"),
    ("requests", "requests"),
    ("flask", "Flask"),
]:
    auto_install(_imp, _pkg)

import telebot
import zipfile
import tarfile
import tempfile
import shutil
import time
import json
import sqlite3
import logging
import signal
import threading
import re
import atexit
import hashlib
import random
import traceback
from datetime import datetime
from io import BytesIO
from threading import Thread

import psutil
import requests
from flask import Flask
from telebot import types
from telebot.handler_backends import BaseMiddleware, CancelUpdate

TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.environ.get("ADMIN_ID", "").strip()

if not TOKEN:
    raise SystemExit("BOT_TOKEN environment variable not set.")
try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except (TypeError, ValueError):
    raise SystemExit("ADMIN_ID must be a numeric Telegram user ID.")
if ADMIN_ID <= 0:
    raise SystemExit("ADMIN_ID must be a positive Telegram user ID.")

AUTHORIZED_IDS = {ADMIN_ID}

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, "hosting_uploads")
HOSTING_DATA_DIR = os.path.join(BASE_DIR, "hosting_data")
BACKUP_DIR = os.path.join(HOSTING_DATA_DIR, "backups")
DATABASE_PATH = os.path.join(HOSTING_DATA_DIR, "hosting_bot.db")
RUNNING_SCRIPTS_DB = os.path.join(HOSTING_DATA_DIR, "running_scripts.json")
LOCK_FILE = os.path.join(HOSTING_DATA_DIR, "bot.lock")

for _d in (UPLOAD_BOTS_DIR, HOSTING_DATA_DIR, BACKUP_DIR):
    os.makedirs(_d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("hosting_bot")

bot_scripts = {}
projects = {}
DB_LOCK = threading.Lock()

_pending_state = {}
_pending_guard = threading.Lock()

def set_pending(chat_id, kind, data=None):
    with _pending_guard:
        _pending_state[chat_id] = {"kind": kind, "data": data or {}}

def get_pending(chat_id):
    with _pending_guard:
        return _pending_state.get(chat_id)

def clear_pending(chat_id):
    with _pending_guard:
        _pending_state.pop(chat_id, None)

_BOLD_MAP = {
    'A':'𝐀','B':'𝐁','C':'𝐂','D':'𝐃','E':'𝐄','F':'𝐅','G':'𝐆','H':'𝐇','I':'𝐈',
    'J':'𝐉','K':'𝐊','L':'𝐋','M':'𝐌','N':'𝐍','O':'𝐎','P':'𝐏','Q':'𝐐','R':'𝐑',
    'S':'𝐒','T':'𝐓','U':'𝐔','V':'𝐕','W':'𝐖','X':'𝐗','Y':'𝐘','Z':'𝐙',
    'a':'𝐚','b':'𝐛','c':'𝐜','d':'𝐝','e':'𝐞','f':'𝐟','g':'𝐠','h':'𝐡','i':'𝐢',
    'j':'𝐣','k':'𝐤','l':'𝐥','m':'𝐦','n':'𝐧','o':'𝐨','p':'𝐩','q':'𝐪','r':'𝐫',
    's':'𝐬','t':'𝐭','u':'𝐮','v':'𝐯','w':'𝐰','x':'𝐱','y':'𝐲','z':'𝐳',
    '0':'𝟎','1':'𝟏','2':'𝟐','3':'𝟑','4':'𝟒','5':'𝟓','6':'𝟔','7':'𝟕','8':'𝟖','9':'𝟗',
}

def B(text) -> str:
    return "".join(_BOLD_MAP.get(c, c) for c in str(text))

class Progress:
    @staticmethod
    def frames(icon, label, steps=10):
        out = []
        for i in range(steps + 1):
            pct = int((i / steps) * 100)
            out.append(B(f"{icon} {label}: [{'▰'*i}{'▱'*(steps-i)}] {pct}%"))
        return out
    @staticmethod
    def upload():    return Progress.frames("UP", "UPLOADING", 5)
    @staticmethod
    def execute():   return Progress.frames("EX", "EXECUTING", 10)
    @staticmethod
    def recovery():  return Progress.frames("RC", "RECOVERY", 6)
    @staticmethod
    def restart():   return Progress.frames("RS", "RESTARTING", 6)
    @staticmethod
    def backup():    return Progress.frames("BK", "BACKUP", 5)
    @staticmethod
    def testing():   return Progress.frames("TS", "TESTING", 5)
    @staticmethod
    def module():    return Progress.frames("MD", "MODULE", 5)

def _pid_alive(pid):
    try:
        p = psutil.Process(pid)
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except Exception:
        return False

def acquire_single_instance_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE) as f:
                data = json.load(f)
            old_pid = int(data.get("pid", 0))
            if old_pid and old_pid != os.getpid() and _pid_alive(old_pid):
                raise SystemExit(f"Another bot instance is running (PID {old_pid}).")
            logger.warning(f"Stale lock (PID {old_pid}). Cleaning.")
        except (json.JSONDecodeError, ValueError):
            logger.warning("Corrupt lock file. Replacing.")
    with open(LOCK_FILE, "w") as f:
        json.dump({"pid": os.getpid(), "started": datetime.now().isoformat()}, f)
    logger.info(f"Lock acquired (PID {os.getpid()}).")

def release_single_instance_lock():
    try:
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE) as f:
                data = json.load(f)
            if int(data.get("pid", 0)) == os.getpid():
                os.remove(LOCK_FILE)
    except Exception:
        pass

atexit.register(release_single_instance_lock)

bot = telebot.TeleBot(TOKEN, use_class_middlewares=True)

class AdminOnlyMiddleware(BaseMiddleware):
    def __init__(self):
        super().__init__()
        self.update_types = ["message", "callback_query"]

    def pre_process(self, update, data):
        user = getattr(update, "from_user", None)
        if user is None or user.id not in AUTHORIZED_IDS:
            try:
                if isinstance(update, telebot.types.CallbackQuery):
                    bot.answer_callback_query(update.id, B("Access Denied"), show_alert=True)
                else:
                    bot.reply_to(update, B("Access Denied"))
            except Exception:
                pass
            return CancelUpdate()
        return None

    def post_process(self, update, data, exception=None):
        if exception:
            logger.error(f"Handler exception: {exception}")

bot.setup_middleware(AdminOnlyMiddleware())

def init_db():
    logger.info(f"Init DB at {DATABASE_PATH}")
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                project_id       TEXT PRIMARY KEY,
                file_id          TEXT,
                owner_id         INTEGER,
                filename         TEXT NOT NULL,
                file_type        TEXT NOT NULL,
                file_size        INTEGER DEFAULT 0,
                upload_time      TEXT,
                project_dir      TEXT,
                runtime_dir      TEXT,
                entry_file       TEXT,
                requirements     TEXT,
                env_info         TEXT,
                pid              INTEGER DEFAULT 0,
                status           TEXT DEFAULT 'stopped',
                start_time       TEXT,
                stop_time        TEXT,
                crash_time       TEXT,
                exit_code        INTEGER,
                error_log        TEXT,
                backup_id        TEXT,
                recovery_info    TEXT,
                historical_running INTEGER DEFAULT 0,
                deps_status      TEXT DEFAULT 'unknown',
                deps_last_install TEXT,
                deps_last_error  TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS project_modules (
                project_id     TEXT,
                manager        TEXT,
                module_name    TEXT,
                version        TEXT,
                installed_at   TEXT,
                status         TEXT,
                last_error     TEXT,
                PRIMARY KEY (project_id, manager, module_name)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS backups (
                backup_id     TEXT PRIMARY KEY,
                created_at    TEXT,
                kind          TEXT,
                project_ids   TEXT,
                archive_path  TEXT,
                size_bytes    INTEGER DEFAULT 0,
                status        TEXT DEFAULT 'ok'
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id    INTEGER PRIMARY KEY,
                added_at   TEXT
            )
        """)
        c.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?, ?)",
                  (ADMIN_ID, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    logger.info("DB ready.")

def load_projects_from_db():
    projects.clear()
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM projects")
        for row in c.fetchall():
            d = dict(row)
            d["pid"] = 0
            if d.get("status") == "running":
                d["historical_running"] = 1
                d["status"] = "stopped"
            projects[d["project_id"]] = d
        conn.close()
    logger.info(f"Loaded {len(projects)} project(s).")

def db_execute(sql, params=()):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        try:
            c = conn.cursor(); c.execute(sql, params); conn.commit()
        finally:
            conn.close()

def db_fetchone(sql, params=()):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            c = conn.cursor(); c.execute(sql, params)
            row = c.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

def db_fetchall(sql, params=()):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            c = conn.cursor(); c.execute(sql, params)
            return [dict(r) for r in c.fetchall()]
        finally:
            conn.close()

def save_project(meta):
    projects[meta["project_id"]] = meta
    db_execute("""
        INSERT OR REPLACE INTO projects (
            project_id, file_id, owner_id, filename, file_type, file_size,
            upload_time, project_dir, runtime_dir, entry_file, requirements,
            env_info, pid, status, start_time, stop_time, crash_time,
            exit_code, error_log, backup_id, recovery_info, historical_running,
            deps_status, deps_last_install, deps_last_error
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        meta.get("project_id"), meta.get("file_id"), meta.get("owner_id"),
        meta.get("filename"), meta.get("file_type"), meta.get("file_size", 0),
        meta.get("upload_time"), meta.get("project_dir"), meta.get("runtime_dir"),
        meta.get("entry_file"), meta.get("requirements"), meta.get("env_info"),
        meta.get("pid", 0), meta.get("status", "stopped"),
        meta.get("start_time"), meta.get("stop_time"), meta.get("crash_time"),
        meta.get("exit_code"), meta.get("error_log"), meta.get("backup_id"),
        meta.get("recovery_info"), meta.get("historical_running", 0),
        meta.get("deps_status", "unknown"),
        meta.get("deps_last_install"), meta.get("deps_last_error"),
    ))

def delete_project_db(project_id):
    projects.pop(project_id, None)
    db_execute("DELETE FROM projects WHERE project_id = ?", (project_id,))
    db_execute("DELETE FROM project_modules WHERE project_id = ?", (project_id,))

def new_project_id():
    return "p_" + hashlib.sha1(f"{time.time()}{random.random()}".encode()).hexdigest()[:12]

_FILENAME_RE = re.compile(r"[^A-Za-z0-9._ -]")
_PKG_NAME_RE = re.compile(r"^[A-Za-z0-9._@/\-+]+$")

def sanitize_filename(name):
    name = os.path.basename(str(name)).replace("\x00", "")
    name = _FILENAME_RE.sub("_", name).strip()
    return None if name in {"", ".", ".."} else name

def sanitize_relpath(path):
    raw = str(path).replace("\x00", "").replace("\\", "/")
    if not raw or raw.startswith("/"):
        return None
    parts = []
    for p in raw.split("/"):
        if p in {"", "."}:
            continue
        if p == "..":
            return None
        c = _FILENAME_RE.sub("_", p)
        if c in {"", ".", ".."}:
            return None
        parts.append(c)
    return os.path.join(*parts) if parts else None

def is_safe_package_name(name: str) -> bool:
    if not name or len(name) > 128:
        return False
    for op in ("==", ">=", "<=", "~=", "!="):
        if op in name:
            base, _, ver = name.partition(op)
            return bool(_PKG_NAME_RE.match(base)) and bool(_PKG_NAME_RE.match(ver))
    return bool(_PKG_NAME_RE.match(name))

def project_path(project_id, *sub):
    base = os.path.realpath(projects[project_id]["project_dir"])
    if not sub:
        return base
    target = os.path.realpath(os.path.join(base, *sub))
    if os.path.commonpath([base, target]) != base:
        raise ValueError("Path escapes project dir")
    return target

init_db()
load_projects_from_db()
acquire_single_instance_lock()
logger.info("HOSTING BOT 5.0 init done")
def project_runtime_dir(project_id):
    meta = projects[project_id]
    rd = meta.get("runtime_dir")
    if not rd:
        rd = os.path.join(HOSTING_DATA_DIR, "runtime", project_id)
        meta["runtime_dir"] = rd
    os.makedirs(rd, exist_ok=True)
    return rd

def project_log_path(project_id):
    return os.path.join(project_runtime_dir(project_id), "run.log")

def project_err_path(project_id):
    return os.path.join(project_runtime_dir(project_id), "run.err")

def is_process_alive(proc):
    if proc is None:
        return False
    try:
        return proc.poll() is None
    except Exception:
        return False

def refresh_status(project_id):
    meta = projects.get(project_id)
    if not meta:
        return
    entry = bot_scripts.get(project_id)
    if entry is None:
        if meta.get("status") == "running":
            meta["status"] = "crashed"
            meta["crash_time"] = datetime.now().isoformat()
            meta["pid"] = 0
            save_project(meta)
        return
    proc = entry.get("process")
    if is_process_alive(proc):
        meta["status"] = "running"
        meta["pid"] = proc.pid
        save_project(meta)
    else:
        rc = None
        try:
            rc = proc.returncode
        except Exception:
            pass
        meta["status"] = "crashed"
        meta["crash_time"] = datetime.now().isoformat()
        meta["exit_code"] = rc
        meta["pid"] = 0
        try:
            ep = project_err_path(project_id)
            if os.path.exists(ep):
                with open(ep, "r", encoding="utf-8", errors="ignore") as f:
                    meta["error_log"] = f.read()[-2000:]
        except Exception:
            pass
        save_project(meta)
        cleanup_runtime_temp(project_id)
        bot_scripts.pop(project_id, None)

_crash_stop = threading.Event()

def crash_watcher():
    while not _crash_stop.is_set():
        try:
            for pid_ in list(projects.keys()):
                try:
                    refresh_status(pid_)
                except Exception as e:
                    logger.error(f"crash_watcher({pid_}): {e}")
            _crash_stop.wait(5)
        except Exception as e:
            logger.error(f"crash_watcher outer: {e}")
            _crash_stop.wait(10)

def kill_process_tree(proc):
    if proc is None:
        return
    try:
        parent = psutil.Process(proc.pid)
        for k in parent.children(recursive=True):
            try: k.terminate()
            except Exception: pass
        try:
            parent.terminate()
            try: parent.wait(timeout=3)
            except psutil.TimeoutExpired: parent.kill()
        except psutil.NoSuchProcess:
            pass
    except Exception as e:
        logger.warning(f"kill_process_tree: {e}")

def cleanup_runtime_temp(project_id):
    try:
        rd = project_runtime_dir(project_id)
        keep = {"run.log", "run.err", "run.log.1", "run.err.1"}
        for name in os.listdir(rd):
            if name in keep:
                continue
            full = os.path.join(rd, name)
            try:
                if os.path.isdir(full):
                    shutil.rmtree(full, ignore_errors=True)
                else:
                    os.remove(full)
            except Exception:
                pass
    except Exception:
        pass

_STDLIB = set(getattr(sys, "stdlib_module_names", []))
_PIP_MAP = {
    "telebot": "pyTelegramBotAPI",
    "telegram": "python-telegram-bot",
    "aiogram": "aiogram",
    "pyrogram": "pyrogram",
    "telethon": "telethon",
    "flask": "Flask",
    "pil": "Pillow",
    "bs4": "beautifulsoup4",
}

def scan_required_modules(py_file):
    needed = []
    try:
        with open(py_file, "r", encoding="utf-8", errors="ignore") as f:
            src = f.read()
        imps = set(re.findall(r'^\s*import\s+([A-Za-z0-9_.]+)', src, re.M))
        imps |= set(re.findall(r'^\s*from\s+([A-Za-z0-9_.]+)\s+import', src, re.M))
        for m in sorted({i.split(".")[0] for i in imps}):
            if m in _STDLIB:
                continue
            try:
                __import__(m)
            except Exception:
                needed.append((m, _PIP_MAP.get(m.lower(), m)))
    except Exception as e:
        logger.error(f"scan_required_modules: {e}")
    return needed

def _popen_kwargs():
    kw = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "encoding": "utf-8",
        "errors": "ignore",
        "bufsize": 1,
    }
    if os.name == "nt":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = subprocess.SW_HIDE
        kw["startupinfo"] = si
        kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kw["start_new_session"] = True
    return kw

def _stream_pipe(pipe, log_path):
    try:
        with open(log_path, "a", encoding="utf-8", errors="ignore") as f:
            for line in iter(pipe.readline, ""):
                f.write(line); f.flush()
    except Exception:
        pass
    finally:
        try: pipe.close()
        except Exception: pass

def run_command(cmd_list_or_str, cwd=None, timeout=60, shell=False):
    start = time.time()
    kw = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": True,
        "encoding": "utf-8",
        "errors": "ignore",
        "bufsize": 1,
        "cwd": cwd or BASE_DIR,
        "shell": shell,
    }
    if os.name == "nt":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = subprocess.SW_HIDE
        kw["startupinfo"] = si
        kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kw["start_new_session"] = True

    try:
        proc = subprocess.Popen(cmd_list_or_str, **kw)
    except FileNotFoundError as e:
        return {"ok": False, "rc": -1, "stdout": "",
                "stderr": f"executable not found: {e}",
                "timed_out": False, "duration": round(time.time() - start, 2)}
    except Exception as e:
        return {"ok": False, "rc": -1, "stdout": "",
                "stderr": f"spawn error: {e}",
                "timed_out": False, "duration": round(time.time() - start, 2)}

    out_buf, err_buf = [], []
    def _drain(pipe, sink):
        try:
            for line in iter(pipe.readline, ""):
                sink.append(line)
        except Exception:
            pass
        finally:
            try: pipe.close()
            except Exception: pass

    t1 = threading.Thread(target=_drain, args=(proc.stdout, out_buf), daemon=True)
    t2 = threading.Thread(target=_drain, args=(proc.stderr, err_buf), daemon=True)
    t1.start(); t2.start()

    timed_out = False
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            parent = psutil.Process(proc.pid)
            for k in parent.children(recursive=True):
                try: k.terminate()
                except Exception: pass
            try:
                parent.terminate()
                try: parent.wait(timeout=3)
                except psutil.TimeoutExpired: parent.kill()
            except psutil.NoSuchProcess:
                pass
        except Exception:
            try: proc.kill()
            except Exception: pass
        time.sleep(0.3)

    try:
        if proc.poll() is None:
            proc.kill()
    except Exception:
        pass

    t1.join(timeout=1); t2.join(timeout=1)
    rc = proc.returncode if proc.returncode is not None else -1
    return {
        "ok": (rc == 0) and (not timed_out),
        "rc": rc,
        "stdout": "".join(out_buf),
        "stderr": "".join(err_buf),
        "timed_out": timed_out,
        "duration": round(time.time() - start, 2),
    }

def start_project(project_id, message=None):
    meta = projects.get(project_id)
    if not meta:
        return False, "Project not found."
    if project_id in bot_scripts and is_process_alive(bot_scripts[project_id].get("process")):
        return False, "Already running."

    entry_abs = project_path(project_id, meta["entry_file"])
    if not os.path.exists(entry_abs):
        return False, f"Entry file missing: {meta['entry_file']}"

    proj_dir = meta["project_dir"]
    log_path = project_log_path(project_id)
    err_path = project_err_path(project_id)
    for p in (log_path, err_path):
        if os.path.exists(p):
            try: os.replace(p, p + ".1")
            except Exception: pass
    open(log_path, "a").close()
    open(err_path, "a").close()

    ftype = meta["file_type"]
    try:
        if ftype == "py":
            cmd = [sys.executable, entry_abs]
        elif ftype == "js":
            cmd = ["node", entry_abs]
        else:
            return False, f"Unsupported type: {ftype}"
        proc = subprocess.Popen(cmd, cwd=proj_dir, **_popen_kwargs())
        threading.Thread(target=_stream_pipe, args=(proc.stdout, log_path), daemon=True).start()
        threading.Thread(target=_stream_pipe, args=(proc.stderr, err_path), daemon=True).start()
        bot_scripts[project_id] = {"process": proc, "started": datetime.now().isoformat()}
        meta["status"] = "running"
        meta["pid"] = proc.pid
        meta["start_time"] = datetime.now().isoformat()
        meta["stop_time"] = None
        meta["crash_time"] = None
        meta["exit_code"] = None
        save_project(meta)
        return True, f"Started (PID {proc.pid})"
    except FileNotFoundError as e:
        return False, f"Runtime missing: {e}"
    except Exception as e:
        logger.error(f"start_project: {e}")
        return False, f"Error: {e}"

def stop_project(project_id):
    meta = projects.get(project_id)
    if not meta:
        return False, "Project not found."
    entry = bot_scripts.get(project_id)
    if not entry:
        meta["status"] = "stopped"
        meta["pid"] = 0
        meta["stop_time"] = datetime.now().isoformat()
        save_project(meta)
        return True, "Was already stopped."
    kill_process_tree(entry.get("process"))
    bot_scripts.pop(project_id, None)
    meta["status"] = "stopped"
    meta["pid"] = 0
    meta["stop_time"] = datetime.now().isoformat()
    save_project(meta)
    cleanup_runtime_temp(project_id)
    return True, "Stopped."

def restart_project(project_id):
    stop_project(project_id)
    time.sleep(1)
    return start_project(project_id)

MODULE_TIMEOUT = 300
MODULE_MAX_OUTPUT = 3500

def _detect_project_manager(pid_):
    meta = projects.get(pid_)
    if not meta:
        return None
    return "pip" if meta["file_type"] == "py" else ("npm" if meta["file_type"] == "js" else None)

def _save_installed_module(pid_, manager, name, version, status, error=None):
    db_execute("""
        INSERT OR REPLACE INTO project_modules
        (project_id, manager, module_name, version, installed_at, status, last_error)
        VALUES (?,?,?,?,?,?,?)
    """, (pid_, manager, name, version, datetime.now().isoformat(), status, error))

def _delete_installed_module(pid_, manager, name):
    db_execute("DELETE FROM project_modules WHERE project_id=? AND manager=? AND module_name=?",
               (pid_, manager, name))

def _list_installed_modules(pid_, manager):
    return db_fetchall("SELECT * FROM project_modules WHERE project_id=? AND manager=? ORDER BY module_name",
                       (pid_, manager))

def _update_requirements_file(pid_):
    meta = projects.get(pid_)
    if not meta or meta["file_type"] != "py":
        return
    # Preserve dependencies that were already declared by the project.
    # Module Manager only adds/removes entries it owns; it must not wipe the
    # project's original requirements when refreshing its metadata.
    try:
        rp = os.path.join(meta["project_dir"], "requirements.txt")
        existing = []
        if os.path.exists(rp):
            with open(rp, "r", encoding="utf-8") as f:
                existing = [x.strip() for x in f.read().splitlines()
                            if x.strip() and not x.lstrip().startswith("#")]
        rows = _list_installed_modules(pid_, "pip")
        managed = {}
        for r in rows:
            if r["status"] != "ok":
                continue
            name = r["module_name"]
            managed[name.lower()] = (f"{name}=={r['version']}"
                                     if r.get("version") else name)
        result = []
        seen = set()
        for line in existing:
            key = re.split(r"[<>=!~\[]", line, 1)[0].strip().lower()
            if key in managed:
                line = managed[key]
            if line not in result:
                result.append(line)
            seen.add(key)
        for key, line in managed.items():
            if key not in seen:
                result.append(line)
        with open(rp, "w", encoding="utf-8") as f:
            f.write("\n".join(result) + ("\n" if result else ""))
    except Exception as e:
        logger.warning(f"write requirements.txt: {e}")

def _update_package_json_deps(pid_, name, version=None, remove=False):
    meta = projects.get(pid_)
    if not meta or meta["file_type"] != "js":
        return
    pj = os.path.join(meta["project_dir"], "package.json")
    data = {}
    if os.path.exists(pj):
        try:
            with open(pj, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    if "dependencies" not in data or not isinstance(data["dependencies"], dict):
        data["dependencies"] = {}
    if remove:
        data["dependencies"].pop(name, None)
    else:
        data["dependencies"][name] = version or "latest"
    try:
        with open(pj, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"write package.json: {e}")

def module_install(pid_, name):
    meta = projects.get(pid_)
    if not meta:
        return {"ok": False, "error": "Project not found."}
    if not is_safe_package_name(name):
        return {"ok": False, "error": "Unsafe package name."}
    manager = _detect_project_manager(pid_)
    if not manager:
        return {"ok": False, "error": "Unsupported project type."}

    proj_dir = meta["project_dir"]
    if manager == "pip":
        cmd = [sys.executable, "-m", "pip", "install", name]
    else:
        cmd = ["npm", "install", name]

    res = run_command(cmd, cwd=proj_dir, timeout=MODULE_TIMEOUT)

    if res["ok"]:
        version = ""
        try:
            if manager == "pip":
                show = run_command([sys.executable, "-m", "pip", "show", name.split("==")[0]],
                                   cwd=proj_dir, timeout=30)
                m = re.search(r"^Version:\s*(.+)$", show["stdout"], re.M)
                if m: version = m.group(1).strip()
            else:
                show = run_command(["npm", "list", name, "--depth=0"],
                                   cwd=proj_dir, timeout=30)
                m = re.search(rf"{re.escape(name)}@([\w.\-+]+)", show["stdout"])
                if m: version = m.group(1)
        except Exception:
            pass
        _save_installed_module(pid_, manager, name.split("==")[0], version, "ok")
        if manager == "pip":
            _update_requirements_file(pid_)
        else:
            _update_package_json_deps(pid_, name.split("@")[0], version)
        meta["deps_status"] = "ok"
        meta["deps_last_install"] = datetime.now().isoformat()
        meta["deps_last_error"] = None
        save_project(meta)
        return {"ok": True, "manager": manager, "name": name, "version": version, **res}
    else:
        _save_installed_module(pid_, manager, name.split("==")[0], "", "failed",
                               (res["stderr"] or res["stdout"])[-500:])
        meta["deps_status"] = "failed"
        meta["deps_last_error"] = (res["stderr"] or res["stdout"])[-500:]
        save_project(meta)
        return {"ok": False, "manager": manager, "name": name, **res}

def module_uninstall(pid_, name):
    meta = projects.get(pid_)
    if not meta:
        return {"ok": False, "error": "Project not found."}
    if not is_safe_package_name(name):
        return {"ok": False, "error": "Unsafe package name."}
    manager = _detect_project_manager(pid_)
    if not manager:
        return {"ok": False, "error": "Unsupported project type."}

    proj_dir = meta["project_dir"]
    if manager == "pip":
        cmd = [sys.executable, "-m", "pip", "uninstall", "-y", name]
    else:
        cmd = ["npm", "uninstall", name]

    res = run_command(cmd, cwd=proj_dir, timeout=MODULE_TIMEOUT)
    if res["ok"]:
        _delete_installed_module(pid_, manager, name)
        if manager == "pip":
            _update_requirements_file(pid_)
        else:
            _update_package_json_deps(pid_, name, remove=True)
        return {"ok": True, "manager": manager, "name": name, **res}
    return {"ok": False, "manager": manager, "name": name, **res}

def module_list(pid_):
    meta = projects.get(pid_)
    if not meta:
        return False, "Project not found.", None
    manager = _detect_project_manager(pid_)
    if not manager:
        return False, "Unsupported project type.", None
    proj_dir = meta["project_dir"]
    if manager == "pip":
        res = run_command([sys.executable, "-m", "pip", "list", "--format=freeze"],
                          cwd=proj_dir, timeout=60)
    else:
        res = run_command(["npm", "list", "--depth=0", "--json"],
                          cwd=proj_dir, timeout=60)
    if not res["ok"]:
        return False, f"List failed (rc={res['rc']})", res
    text = res["stdout"] or ""
    return True, text, res

def module_show(pid_, name):
    meta = projects.get(pid_)
    if not meta:
        return False, "Project not found.", None
    if not is_safe_package_name(name):
        return False, "Unsafe package name.", None
    manager = _detect_project_manager(pid_)
    if not manager:
        return False, "Unsupported project type.", None
    if manager == "pip":
        res = run_command([sys.executable, "-m", "pip", "show", name],
                          cwd=meta["project_dir"], timeout=30)
    else:
        res = run_command(["npm", "view", name],
                          cwd=meta["project_dir"], timeout=30)
    return res["ok"], res["stdout"] or res["stderr"], res

def module_install_requirements(pid_):
    meta = projects.get(pid_)
    if not meta:
        return {"ok": False, "error": "Project not found."}
    manager = _detect_project_manager(pid_)
    proj_dir = meta["project_dir"]
    if manager == "pip":
        req = os.path.join(proj_dir, "requirements.txt")
        if not os.path.exists(req):
            return {"ok": False, "error": "requirements.txt not found."}
        res = run_command([sys.executable, "-m", "pip", "install", "-r", req],
                          cwd=proj_dir, timeout=MODULE_TIMEOUT)
    elif manager == "npm":
        pj = os.path.join(proj_dir, "package.json")
        if not os.path.exists(pj):
            return {"ok": False, "error": "package.json not found."}
        res = run_command(["npm", "install"], cwd=proj_dir, timeout=MODULE_TIMEOUT)
    else:
        return {"ok": False, "error": "Unsupported project type."}

    meta["deps_status"] = "ok" if res["ok"] else "failed"
    if res["ok"]:
        meta["deps_last_install"] = datetime.now().isoformat()
        meta["deps_last_error"] = None
        # Reconcile the Module Manager DB with the project's declared manifest.
        try:
            if manager == "pip":
                req = os.path.join(proj_dir, "requirements.txt")
                with open(req, "r", encoding="utf-8") as f:
                    names = []
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        name = re.split(r"[<>=!~\[]", line, 1)[0].strip()
                        if name and is_safe_package_name(name):
                            names.append(name)
                for name in names:
                    show = run_command([sys.executable, "-m", "pip", "show", name],
                                       cwd=proj_dir, timeout=30)
                    m = re.search(r"^Version:\s*(.+)$", show["stdout"], re.M)
                    _save_installed_module(pid_, "pip", name,
                                           m.group(1).strip() if m else "",
                                           "ok" if show["ok"] else "failed",
                                           None if show["ok"] else (show["stderr"] or "")[-500:])
            elif manager == "npm":
                pj = os.path.join(proj_dir, "package.json")
                with open(pj, "r", encoding="utf-8") as f:
                    pdata = json.load(f)
                declared = dict(pdata.get("dependencies") or {})
                db_execute("DELETE FROM project_modules WHERE project_id=? AND manager=?",
                           (pid_, "npm"))
                for name, version in declared.items():
                    _save_installed_module(pid_, "npm", name, str(version), "ok")
        except Exception as e:
            logger.warning(f"module metadata sync: {e}")
    else:
        meta["deps_last_error"] = (res["stderr"] or res["stdout"])[-500:]
    save_project(meta)
    return res

def module_update(pid_, name):
    meta = projects.get(pid_)
    if not meta:
        return {"ok": False, "error": "Project not found."}
    if not is_safe_package_name(name):
        return {"ok": False, "error": "Unsafe package name."}
    manager = _detect_project_manager(pid_)
    if manager == "pip":
        res = run_command([sys.executable, "-m", "pip", "install", "--upgrade", name],
                          cwd=meta["project_dir"], timeout=MODULE_TIMEOUT)
    elif manager == "npm":
        res = run_command(["npm", "install", f"{name}@latest"],
                          cwd=meta["project_dir"], timeout=MODULE_TIMEOUT)
    else:
        return {"ok": False, "error": "Unsupported project type."}
    if res["ok"]:
        _save_installed_module(pid_, manager, name, "", "ok")
    else:
        _save_installed_module(pid_, manager, name, "", "failed",
                               (res["stderr"] or res["stdout"])[-500:])
    return res

def main_menu_inline():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton(B("Upload"), callback_data="m:upload"),
           types.InlineKeyboardButton(B("My Files"), callback_data="m:files"))
    kb.add(types.InlineKeyboardButton(B("Speed Test"), callback_data="m:speed"),
           types.InlineKeyboardButton(B("Stats"), callback_data="m:stats"))
    kb.add(types.InlineKeyboardButton(B("Profile"), callback_data="m:profile"),
           types.InlineKeyboardButton(B("Module Manager"), callback_data="m:module"))
    kb.add(types.InlineKeyboardButton(B("Testing"), callback_data="m:testing"),
           types.InlineKeyboardButton(B("Backup"), callback_data="m:backup"))
    kb.add(types.InlineKeyboardButton(B("Terminal"), callback_data="m:terminal"),
           types.InlineKeyboardButton(B("Process Control"), callback_data="m:proc"))
    kb.add(types.InlineKeyboardButton(B("All Files"), callback_data="m:allfiles"),
           types.InlineKeyboardButton(B("Restart All"), callback_data="m:restartall"))
    kb.add(types.InlineKeyboardButton(B("Admin Panel"), callback_data="m:admin"))
    return kb

def main_menu_reply():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for label in ("Upload", "My Files", "Speed Test", "Stats", "Profile",
                  "Module Manager", "Testing", "Backup", "Terminal",
                  "Process Control", "All Files", "Restart All", "Admin Panel"):
        kb.add(types.KeyboardButton(B(label)))
    return kb

def file_controls_kb(project_id, is_running):
    kb = types.InlineKeyboardMarkup(row_width=2)
    if is_running:
        kb.add(types.InlineKeyboardButton(B("Stop"), callback_data=f"p:stop:{project_id}"),
               types.InlineKeyboardButton(B("Restart"), callback_data=f"p:restart:{project_id}"))
    else:
        kb.add(types.InlineKeyboardButton(B("Start"), callback_data=f"p:start:{project_id}"),
               types.InlineKeyboardButton(B("Restart"), callback_data=f"p:restart:{project_id}"))
    kb.add(types.InlineKeyboardButton(B("Test"), callback_data=f"p:test:{project_id}"),
           types.InlineKeyboardButton(B("Status"), callback_data=f"p:status:{project_id}"))
    kb.add(types.InlineKeyboardButton(B("Logs"), callback_data=f"p:logs:{project_id}"),
           types.InlineKeyboardButton(B("Download"), callback_data=f"p:download:{project_id}"))
    kb.add(types.InlineKeyboardButton(B("Backup"), callback_data=f"p:backup:{project_id}"),
           types.InlineKeyboardButton(B("Clean Temp"), callback_data=f"p:cleantemp:{project_id}"))
    kb.add(types.InlineKeyboardButton(B("Modules"), callback_data=f"p:modules:{project_id}"))
    kb.add(types.InlineKeyboardButton(B("Delete"), callback_data=f"p:delete:{project_id}"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="m:files"))
    return kb

@bot.message_handler(commands=["start"])
def cmd_start(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, B("Access Denied")); return
    n_run = sum(1 for p in projects.values() if p.get("status") == "running")
    n_crash = sum(1 for p in projects.values() if p.get("status") in ("crashed", "error"))
    txt = B(f"🚀 HOSTING BOT 5.0\n\nAdmin: {message.from_user.first_name}\n"
            f"Projects: {len(projects)}  Running: {n_run}  Crashed: {n_crash}")
    bot.send_message(message.chat.id, txt, reply_markup=main_menu_reply())
    bot.send_message(message.chat.id, B("Menu:"), reply_markup=main_menu_inline())

@bot.message_handler(func=lambda m: m.text in {
    B(x) for x in ("Upload","My Files","Speed Test","Stats","Profile",
                   "Module Manager","Testing","Backup","Terminal",
                   "Process Control","All Files","Restart All","Admin Panel")
})
def reply_router(message):
    t = message.text
    if t == B("Upload"):            bot.reply_to(message, B("Send a .py, .js, or .zip file."))
    elif t == B("My Files"):        show_my_files(message.chat.id)
    elif t == B("Speed Test"):      run_speed_test(message.chat.id)
    elif t == B("Stats"):           show_stats(message.chat.id)
    elif t == B("Profile"):         show_profile(message.chat.id)
    elif t == B("Module Manager"):  show_module_manager_root(message.chat.id)
    elif t == B("Testing"):         show_testing_menu(message.chat.id)
    elif t == B("Backup"):          show_backup_menu(message.chat.id)
    elif t == B("Terminal"):        prompt_terminal(message.chat.id)
    elif t == B("Process Control"): show_process_control(message.chat.id)
    elif t == B("All Files"):       show_all_files(message.chat.id)
    elif t == B("Restart All"):     restart_all_handler(message.chat.id)
    elif t == B("Admin Panel"):     show_admin_panel(message.chat.id)

@bot.message_handler(content_types=["document"])
def on_upload(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, B("Access Denied")); return
    doc = message.document
    if not doc or not doc.file_name:
        bot.reply_to(message, B("No file name.")); return
    safe_name = sanitize_filename(doc.file_name)
    if not safe_name:
        bot.reply_to(message, B("Invalid file name.")); return
    ext = os.path.splitext(safe_name)[1].lower()
    if ext not in (".py", ".js", ".zip"):
        bot.reply_to(message, B("Unsupported. Use .py/.js/.zip")); return

    prog = bot.reply_to(message, Progress.upload()[0])
    try:
        for fr in Progress.upload():
            try: bot.edit_message_text(fr, message.chat.id, prog.message_id)
            except Exception: pass
            time.sleep(0.2)
        info = bot.get_file(doc.file_id)
        blob = bot.download_file(info.file_path)

        pid_ = new_project_id()
        proj_dir = os.path.join(UPLOAD_BOTS_DIR, pid_)
        os.makedirs(proj_dir, exist_ok=True)
        meta = {
            "project_id": pid_, "file_id": doc.file_id, "owner_id": message.from_user.id,
            "filename": safe_name, "file_type": ext.lstrip("."),
            "file_size": doc.file_size or len(blob),
            "upload_time": datetime.now().isoformat(),
            "project_dir": proj_dir,
            "runtime_dir": os.path.join(HOSTING_DATA_DIR, "runtime", pid_),
            "entry_file": safe_name, "requirements": "[]",
            "env_info": json.dumps({"python": sys.version.split()[0]}),
            "pid": 0, "status": "stopped", "start_time": None, "stop_time": None,
            "crash_time": None, "exit_code": None, "error_log": None,
            "backup_id": None, "recovery_info": None, "historical_running": 0,
            "deps_status": "unknown", "deps_last_install": None, "deps_last_error": None,
        }
        os.makedirs(meta["runtime_dir"], exist_ok=True)

        if ext == ".zip":
            _extract_zip(blob, proj_dir, safe_name)
            entry, ftype = _detect_entry(proj_dir)
            if not entry:
                raise RuntimeError("No .py or .js inside ZIP.")
            meta["entry_file"] = entry
            meta["file_type"] = ftype
            try: os.remove(os.path.join(proj_dir, safe_name))
            except Exception: pass
        else:
            with open(os.path.join(proj_dir, safe_name), "wb") as f:
                f.write(blob)

        if meta["file_type"] == "py":
            reqs = scan_required_modules(os.path.join(proj_dir, meta["entry_file"]))
            meta["requirements"] = json.dumps(reqs)
        save_project(meta)

        txt = "\n".join([
            B("Upload complete"),
            f"{meta['filename']}",
            f"ID: {pid_}",
            f"Type: {meta['file_type']}",
            f"Size: {meta['file_size']}",
            f"Entry: {meta['entry_file']}",
        ])
        bot.edit_message_text(txt, message.chat.id, prog.message_id,
                              reply_markup=file_controls_kb(pid_, False))
    except Exception as e:
        logger.error(f"upload: {e}\n{traceback.format_exc()}")
        bot.edit_message_text(B(f"Upload failed: {e}"), message.chat.id, prog.message_id)

def _extract_zip(blob, dest, zip_name):
    tmp = tempfile.mkdtemp(prefix="zipext_")
    try:
        zp = os.path.join(tmp, zip_name)
        with open(zp, "wb") as f:
            f.write(blob)
        with zipfile.ZipFile(zp) as zf:
            for m in zf.infolist():
                tr = sanitize_relpath(m.filename)
                if not tr:
                    raise ValueError(f"Unsafe ZIP entry: {m.filename}")
                ta = os.path.realpath(os.path.join(dest, tr))
                if os.path.commonpath([os.path.realpath(dest), ta]) != os.path.realpath(dest):
                    raise ValueError(f"Traversal: {m.filename}")
            zf.extractall(dest)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

def _detect_entry(proj_dir):
    pys, jss = [], []
    for root, _, names in os.walk(proj_dir):
        for n in names:
            rel = os.path.relpath(os.path.join(root, n), proj_dir)
            if n.lower().endswith(".py"): pys.append(rel)
            elif n.lower().endswith(".js"): jss.append(rel)
    for c in ("main.py", "bot.py", "app.py"):
        if c in [os.path.basename(p) for p in pys]:
            return next(p for p in pys if os.path.basename(p) == c), "py"
    if pys: return pys[0], "py"
    for c in ("index.js", "main.js", "bot.js"):
        if c in [os.path.basename(p) for p in jss]:
            return next(p for p in jss if os.path.basename(p) == c), "js"
    if jss: return jss[0], "js"
    return None, None

def show_my_files(chat_id):
    if not projects:
        bot.send_message(chat_id, B("No projects yet.")); return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pid_, meta in projects.items():
        st = meta.get("status", "stopped")
        icon = {"running":"G","stopped":"-","starting":"S","testing":"T","crashed":"C","error":"E"}.get(st, "-")
        kb.add(types.InlineKeyboardButton(
            B(f"[{icon}] {meta['filename']} ({meta['file_type']})"),
            callback_data=f"p:view:{pid_}"))
    bot.send_message(chat_id, B("My Files:"), reply_markup=kb)

def show_module_manager_root(chat_id):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(B("Install Module"), callback_data="mm:pick_install"))
    kb.add(types.InlineKeyboardButton(B("Uninstall Module"), callback_data="mm:pick_uninstall"))
    kb.add(types.InlineKeyboardButton(B("Installed Modules"), callback_data="mm:pick_list"))
    kb.add(types.InlineKeyboardButton(B("Update Module"), callback_data="mm:pick_update"))
    kb.add(types.InlineKeyboardButton(B("Install Requirements"), callback_data="mm:pick_reqs"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="m:back"))
    bot.send_message(chat_id, B("MODULE MANAGER"), reply_markup=kb)

def show_module_manager_project_select(chat_id, action, title):
    if not projects:
        bot.send_message(chat_id, B("No projects.")); return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pid_, meta in projects.items():
        manager = _detect_project_manager(pid_)
        tag = f"[{manager}]" if manager else "[?]"
        kb.add(types.InlineKeyboardButton(
            B(f"{tag} {meta['filename']}"),
            callback_data=f"mm:do_{action}:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="m:module"))
    bot.send_message(chat_id, B(title), reply_markup=kb)

def module_action_menu(chat_id, pid_):
    meta = projects.get(pid_)
    if not meta:
        bot.send_message(chat_id, B("Not found.")); return
    manager = _detect_project_manager(pid_)
    kb = types.InlineKeyboardMarkup(row_width=1)
    if manager:
        kb.add(types.InlineKeyboardButton(B("Install Module"), callback_data=f"mm:install:{pid_}"))
        kb.add(types.InlineKeyboardButton(B("Uninstall Module"), callback_data=f"mm:uninstall:{pid_}"))
        kb.add(types.InlineKeyboardButton(B("Installed Modules"), callback_data=f"mm:list:{pid_}"))
        kb.add(types.InlineKeyboardButton(B("Update Module"), callback_data=f"mm:update:{pid_}"))
        kb.add(types.InlineKeyboardButton(B("Install Requirements"), callback_data=f"mm:reqs:{pid_}"))
    else:
        kb.add(types.InlineKeyboardButton(B("Unsupported project type"), callback_data="mm:noop"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="m:module"))
    txt = (f"MODULE MANAGER — {meta['filename']}\n"
          f"Manager: {manager or 'unsupported'}\n"
          f"Deps status: {meta.get('deps_status','unknown')}\n"
          f"Last install: {meta.get('deps_last_install') or '-'}")
    bot.send_message(chat_id, B(txt), reply_markup=kb)

def _start_module_input(chat_id, pid_, action):
    set_pending(chat_id, "module_input", {"pid": pid_, "action": action})
    prompt = {
        "install": "Send package name to INSTALL (e.g. requests or requests==2.31.0)",
        "uninstall": "Send package name to UNINSTALL",
        "update": "Send package name to UPDATE",
    }.get(action, "Send package name")
    bot.send_message(chat_id, B(prompt + "\n/cancel to abort"))

@bot.message_handler(func=lambda m: (m.from_user.id == ADMIN_ID)
                                    and (get_pending(m.chat.id) or {}).get("kind") == "module_input"
                                    and bool(m.text))
def _module_input_handler(message):
    st = get_pending(message.chat.id)
    if not st:
        return
    clear_pending(message.chat.id)
    data = st.get("data") or {}
    pid_ = data.get("pid"); action = data.get("action")
    name = (message.text or "").strip()
    if not pid_ or pid_ not in projects or not name:
        bot.reply_to(message, B("Invalid input.")); return
    if not is_safe_package_name(name):
        bot.reply_to(message, B("Unsafe package name.")); return

    prog = bot.send_message(message.chat.id, Progress.module()[0])
    for fr in Progress.module():
        try: bot.edit_message_text(fr, message.chat.id, prog.message_id)
        except Exception: pass
        time.sleep(0.1)

    if action == "install":
        res = module_install(pid_, name)
    elif action == "uninstall":
        res = module_uninstall(pid_, name)
    elif action == "update":
        res = module_update(pid_, name)
    else:
        res = {"ok": False, "error": "Unknown action."}

    _send_command_result(message.chat.id, action.upper(), name, res)

def _send_command_result(chat_id, action, name, res):
    if "error" in res and res.get("error") and not res.get("rc"):
        bot.send_message(chat_id, B(f"{action} {name} -> ERROR: {res['error']}")); return
    header = (f"{action} {name}\n"
              f"Result: {'OK' if res.get('ok') else 'FAIL'}\n"
              f"Exit: {res.get('rc', '-')}\n"
              f"Time: {res.get('duration', '-')}s\n"
              f"Timed out: {'yes' if res.get('timed_out') else 'no'}")
    body = header
    out = (res.get("stdout") or "")[-MODULE_MAX_OUTPUT:]
    err = (res.get("stderr") or "")[-MODULE_MAX_OUTPUT:]
    if out.strip(): body += "\n\n-- STDOUT --\n" + out
    if err.strip(): body += "\n\n-- STDERR --\n" + err
    if len(body) > 3800:
        bot.send_message(chat_id, header)
        bio = BytesIO(body.encode()); bio.name = f"{action.lower()}_{name}.txt"
        bot.send_document(chat_id, bio, caption=B(f"{action} {name} output"))
    else:
        bot.send_message(chat_id, body)

@bot.callback_query_handler(func=lambda c: True)
def cb_router(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, B("Access Denied"), show_alert=True); return
    data = call.data

    main_map = {
        "m:files": lambda: show_my_files(call.message.chat.id),
        "m:upload": lambda: bot.send_message(call.message.chat.id, B("Send a .py/.js/.zip file.")),
        "m:speed": lambda: run_speed_test(call.message.chat.id),
        "m:stats": lambda: show_stats(call.message.chat.id),
        "m:profile": lambda: show_profile(call.message.chat.id),
        "m:module": lambda: show_module_manager_root(call.message.chat.id),
        "m:testing": lambda: show_testing_menu(call.message.chat.id),
        "m:backup": lambda: show_backup_menu(call.message.chat.id),
        "m:terminal": lambda: prompt_terminal(call.message.chat.id),
        "m:proc": lambda: show_process_control(call.message.chat.id),
        "m:allfiles": lambda: show_all_files(call.message.chat.id),
        "m:restartall": lambda: restart_all_handler(call.message.chat.id),
        "m:admin": lambda: show_admin_panel(call.message.chat.id),
        "m:back": lambda: bot.send_message(call.message.chat.id, B("Menu:"), reply_markup=main_menu_inline()),
    }
    if data in main_map:
        bot.answer_callback_query(call.id); main_map[data](); return

    if data == "mm:noop":
        bot.answer_callback_query(call.id, B("Not available"), show_alert=True); return
    if data == "mm:pick_install":
        bot.answer_callback_query(call.id)
        show_module_manager_project_select(call.message.chat.id, "install", "INSTALL MODULE — pick project:"); return
    if data == "mm:pick_uninstall":
        bot.answer_callback_query(call.id)
        show_module_manager_project_select(call.message.chat.id, "uninstall", "UNINSTALL MODULE — pick project:"); return
    if data == "mm:pick_list":
        bot.answer_callback_query(call.id)
        show_module_manager_project_select(call.message.chat.id, "list", "INSTALLED MODULES — pick project:"); return
    if data == "mm:pick_update":
        bot.answer_callback_query(call.id)
        show_module_manager_project_select(call.message.chat.id, "update", "UPDATE MODULE — pick project:"); return
    if data == "mm:pick_reqs":
        bot.answer_callback_query(call.id)
        show_module_manager_project_select(call.message.chat.id, "reqs", "INSTALL REQUIREMENTS — pick project:"); return

    if data.startswith("mm:"):
        parts = data.split(":", 2)
        action = parts[1]; pid_ = parts[2] if len(parts) > 2 else None
        if action == "do_install" and pid_:
            bot.answer_callback_query(call.id); module_action_menu(call.message.chat.id, pid_); return
        if action == "do_uninstall" and pid_:
            bot.answer_callback_query(call.id); _start_module_input(call.message.chat.id, pid_, "uninstall"); return
        if action == "do_list" and pid_:
            bot.answer_callback_query(call.id); _handle_module_list(call.message.chat.id, pid_); return
        if action == "do_update" and pid_:
            bot.answer_callback_query(call.id); _start_module_input(call.message.chat.id, pid_, "update"); return
        if action == "do_reqs" and pid_:
            bot.answer_callback_query(call.id, B("Installing requirements..."))
            threading.Thread(target=_handle_module_reqs, args=(call.message.chat.id, pid_), daemon=True).start(); return
        if action == "install" and pid_:
            bot.answer_callback_query(call.id); _start_module_input(call.message.chat.id, pid_, "install"); return
        if action == "uninstall" and pid_:
            bot.answer_callback_query(call.id); _start_module_input(call.message.chat.id, pid_, "uninstall"); return
        if action == "list" and pid_:
            bot.answer_callback_query(call.id); _handle_module_list(call.message.chat.id, pid_); return
        if action == "update" and pid_:
            bot.answer_callback_query(call.id); _start_module_input(call.message.chat.id, pid_, "update"); return
        if action == "reqs" and pid_:
            bot.answer_callback_query(call.id, B("Installing requirements..."))
            threading.Thread(target=_handle_module_reqs, args=(call.message.chat.id, pid_), daemon=True).start(); return
        bot.answer_callback_query(call.id, "Unknown"); return

    if data.startswith("p:"):
        parts = data.split(":", 2)
        if len(parts) < 3:
            bot.answer_callback_query(call.id, "Bad"); return
        handle_project_action(call, parts[1], parts[2]); return

    bot.answer_callback_query(call.id, "Unknown")

def _handle_module_list(chat_id, pid_):
    ok, text, _ = module_list(pid_)
    if not ok:
        bot.send_message(chat_id, B(f"List failed: {text}")); return
    if len(text) > 3800:
        bio = BytesIO(text.encode()); bio.name = f"{pid_}_modules.txt"
        bot.send_document(chat_id, bio, caption=B(f"Installed modules for {pid_}")); return
    bot.send_message(chat_id, B(f"Installed modules for {pid_}:\n\n") + "```\n" + text[-3500:] + "\n```",
                     parse_mode="Markdown")

def _handle_module_reqs(chat_id, pid_):
    res = module_install_requirements(pid_)
    _send_command_result(chat_id, "INSTALL REQUIREMENTS", projects[pid_]["filename"], res)

def _send_status_panel(chat_id, pid_, edit_msg_id=None):
    meta = projects.get(pid_)
    if not meta:
        bot.send_message(chat_id, B("Not found.")); return
    is_run = meta.get("status") == "running"
    st = meta.get("status", "stopped")
    txt = B(f"Controls: {meta['filename']}\nID: {pid_}\nStatus: {st}\n"
            f"Entry: {meta['entry_file']}\nType: {meta['file_type']}\nPID: {meta.get('pid') or 0}")
    kb = file_controls_kb(pid_, is_run)
    if edit_msg_id:
        try:
            bot.edit_message_text(txt, chat_id, edit_msg_id, reply_markup=kb); return
        except Exception:
            pass
    bot.send_message(chat_id, txt, reply_markup=kb)

def handle_project_action(call, action, pid_):
    meta = projects.get(pid_)
    if not meta:
        bot.answer_callback_query(call.id, B("Not found."), show_alert=True); return

    if action == "view":
        bot.answer_callback_query(call.id)
        _send_status_panel(call.message.chat.id, pid_, call.message.message_id); return
    if action == "start":
        bot.answer_callback_query(call.id, B("Starting..."))
        ok, msg = start_project(pid_)
        bot.send_message(call.message.chat.id, B(("OK: " if ok else "FAIL: ") + msg))
        _send_status_panel(call.message.chat.id, pid_); return
    if action == "stop":
        bot.answer_callback_query(call.id, B("Stopping..."))
        ok, msg = stop_project(pid_)
        bot.send_message(call.message.chat.id, B(("OK: " if ok else "FAIL: ") + msg))
        _send_status_panel(call.message.chat.id, pid_); return
    if action == "restart":
        bot.answer_callback_query(call.id, B("Restarting..."))
        ok, msg = restart_project(pid_)
        bot.send_message(call.message.chat.id, B(("OK: " if ok else "FAIL: ") + msg))
        _send_status_panel(call.message.chat.id, pid_); return
    if action == "status":
        bot.answer_callback_query(call.id); refresh_status(pid_)
        _send_status_panel(call.message.chat.id, pid_); return
    if action == "logs":
        bot.answer_callback_query(call.id); show_logs(call.message.chat.id, pid_); return
    if action == "test":
        bot.answer_callback_query(call.id, B("Testing..."))
        threading.Thread(target=run_test_for_project, args=(call.message.chat.id, pid_), daemon=True).start(); return
    if action == "download":
        bot.answer_callback_query(call.id)
        threading.Thread(target=download_project, args=(call.message.chat.id, pid_), daemon=True).start(); return
    if action == "backup":
        bot.answer_callback_query(call.id, B("Backing up..."))
        threading.Thread(target=create_backup, args=(call.message.chat.id, [pid_], "individual"), daemon=True).start(); return
    if action == "cleantemp":
        bot.answer_callback_query(call.id, B("Cleaning..."))
        cleanup_runtime_temp(pid_)
        bot.send_message(call.message.chat.id, B("Cleaned.")); return
    if action == "modules":
        bot.answer_callback_query(call.id)
        module_action_menu(call.message.chat.id, pid_); return
    if action == "delete":
        bot.answer_callback_query(call.id); confirm_delete(call.message.chat.id, pid_); return
    if action == "delete_yes":
        bot.answer_callback_query(call.id, B("Deleting..."))
        _delete_project(pid_)
        bot.send_message(call.message.chat.id, B("Deleted."))
        show_my_files(call.message.chat.id); return
    bot.answer_callback_query(call.id, "Unknown")

def show_logs(chat_id, pid_):
    meta = projects.get(pid_)
    if not meta:
        bot.send_message(chat_id, B("Not found.")); return
    parts = []
    for lbl, path in (("STDOUT", project_log_path(pid_)), ("STDERR", project_err_path(pid_))):
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    parts.append(f"-- {lbl} --\n{f.read()[-1500:]}")
            except Exception as e:
                parts.append(f"-- {lbl} --\n<read err {e}>")
        else:
            parts.append(f"-- {lbl} --\n<empty>")
    out = f"Logs: {meta['filename']}\n\n" + "\n\n".join(parts)
    if len(out) > 3500:
        bio = BytesIO(out.encode()); bio.name = f"{meta['filename']}_logs.txt"
        bot.send_document(chat_id, bio, caption=B("Full logs attached."))
    else:
        bot.send_message(chat_id, "```\n" + out + "\n```", parse_mode="Markdown")

def confirm_delete(chat_id, pid_):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton(B("Yes, delete"), callback_data=f"p:delete_yes:{pid_}"),
           types.InlineKeyboardButton(B("Cancel"), callback_data=f"p:view:{pid_}"))
    bot.send_message(chat_id, B("Delete permanently?"), reply_markup=kb)

def _delete_project(pid_):
    meta = projects.get(pid_)
    if not meta: return
    if pid_ in bot_scripts: stop_project(pid_)
    try: shutil.rmtree(meta["project_dir"], ignore_errors=True)
    except Exception: pass
    try: shutil.rmtree(meta["runtime_dir"], ignore_errors=True)
    except Exception: pass
    delete_project_db(pid_)

def show_admin_panel(chat_id):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(B("📊 System Overview"), callback_data="ad:overview"))
    kb.add(types.InlineKeyboardButton(B("📁 All Projects"), callback_data="ad:projects"))
    kb.add(types.InlineKeyboardButton(B("⚙️ Process Control"), callback_data="m:proc"))
    kb.add(types.InlineKeyboardButton(B("🧪 Testing"), callback_data="m:testing"))
    kb.add(types.InlineKeyboardButton(B("💾 Backup & Recovery"), callback_data="m:backup"))
    kb.add(types.InlineKeyboardButton(B("📦 Module Manager"), callback_data="m:module"))
    kb.add(types.InlineKeyboardButton(B("🖥 Terminal"), callback_data="m:terminal"))
    kb.add(types.InlineKeyboardButton(B("📜 Error Logs"), callback_data="ad:errlogs"))
    kb.add(types.InlineKeyboardButton(B("🔒 Lock Status"), callback_data="ad:lock"))
    kb.add(types.InlineKeyboardButton(B("🔄 Restart Bot"), callback_data="ad:restart"))
    kb.add(types.InlineKeyboardButton(B("⬅️ Back"), callback_data="m:back"))
    bot.send_message(chat_id, B("👑 ADMIN PANEL"), reply_markup=kb)
    # ============================================================
# PART 3 — TESTING SYSTEM (module-aware, isolated, timeout)
# ============================================================
TEST_DEFAULT_TIMEOUT = 60
TEST_COMPILE_TIMEOUT = 30
TEST_MAX_OUTPUT = 3500
TEST_AUTO_INSTALL_DEPS = False  # never install dependencies automatically during tests

_test_locks = {}
_test_locks_guard = threading.Lock()

def _get_test_lock(pid_):
    with _test_locks_guard:
        lk = _test_locks.get(pid_)
        if lk is None:
            lk = threading.Lock(); _test_locks[pid_] = lk
        return lk

def show_testing_menu(chat_id):
    if not projects:
        bot.send_message(chat_id, B("No projects.")); return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pid_, meta in projects.items():
        st = meta.get("status", "stopped")
        icon = {"running":"G","stopped":"-","starting":"S","testing":"T","crashed":"C","error":"E"}.get(st, "-")
        kb.add(types.InlineKeyboardButton(
            B(f"Test [{icon}] {meta['filename']} ({meta['file_type']})"),
            callback_data=f"t:run:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="m:back"))
    bot.send_message(chat_id, B("Testing: select a project"), reply_markup=kb)

def _copy_project_to_temp(meta):
    tmp_root = tempfile.mkdtemp(prefix=f"test_{meta['project_id']}_")
    proj_dst = os.path.join(tmp_root, "project")
    shutil.copytree(meta["project_dir"], proj_dst)
    return proj_dst

def _validate_python(entry_abs, cwd):
    try:
        r = subprocess.run([sys.executable, "-m", "py_compile", entry_abs],
                           cwd=cwd, capture_output=True, text=True,
                           timeout=TEST_COMPILE_TIMEOUT)
        return r.returncode == 0, r.stdout or "", r.stderr or "", r.returncode
    except subprocess.TimeoutExpired:
        return False, "", "py_compile timeout", -1
    except Exception as e:
        return False, "", f"py_compile err: {e}", -1

def _validate_node(entry_abs, cwd):
    try:
        r = subprocess.run(["node", "--check", entry_abs],
                           cwd=cwd, capture_output=True, text=True,
                           timeout=TEST_COMPILE_TIMEOUT)
        return r.returncode == 0, r.stdout or "", r.stderr or "", r.returncode
    except FileNotFoundError:
        return False, "", "node not installed", -1
    except subprocess.TimeoutExpired:
        return False, "", "node --check timeout", -1
    except Exception as e:
        return False, "", f"node err: {e}", -1

def _runtime_test_generic(cmd, cwd, timeout):
    try:
        proc = subprocess.Popen(cmd, cwd=cwd, **_popen_kwargs())
    except FileNotFoundError as e:
        return False, "", f"runtime missing: {e}", -1, False
    except Exception as e:
        return False, "", f"spawn err: {e}", -1, False

    out_buf, err_buf = [], []
    def _drain(pipe, sink):
        try:
            for line in iter(pipe.readline, ""):
                sink.append(line)
        except Exception:
            pass
        finally:
            try: pipe.close()
            except Exception: pass

    t1 = threading.Thread(target=_drain, args=(proc.stdout, out_buf), daemon=True)
    t2 = threading.Thread(target=_drain, args=(proc.stderr, err_buf), daemon=True)
    t1.start(); t2.start()

    timed_out = False
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            parent = psutil.Process(proc.pid)
            for k in parent.children(recursive=True):
                try: k.terminate()
                except Exception: pass
            try:
                parent.terminate()
                try: parent.wait(timeout=3)
                except psutil.TimeoutExpired: parent.kill()
            except psutil.NoSuchProcess:
                pass
        except Exception:
            try: proc.kill()
            except Exception: pass
        time.sleep(0.5)

    try:
        if proc.poll() is None: proc.kill()
    except Exception:
        pass
    t1.join(timeout=1); t2.join(timeout=1)
    rc = proc.returncode if proc.returncode is not None else -1
    ok = (rc == 0) and (not timed_out)
    return ok, "".join(out_buf), "".join(err_buf), rc, timed_out

def _test_install_deps(meta, tmp_proj):
    """
    Best-effort dependency install INTO the isolated temp copy.
    Does not modify the original project.
    Returns (ok, message).
    """
    if not TEST_AUTO_INSTALL_DEPS:
        return True, "skipped"
    try:
        if meta["file_type"] == "py":
            req = os.path.join(tmp_proj, "requirements.txt")
            if os.path.exists(req):
                r = run_command([sys.executable, "-m", "pip", "install",
                                 "--quiet", "-r", req],
                                cwd=tmp_proj, timeout=MODULE_TIMEOUT)
                return r["ok"], ("deps installed" if r["ok"]
                                 else (r["stderr"] or "pip failed")[-300:])
            return True, "no requirements.txt"
        elif meta["file_type"] == "js":
            pj = os.path.join(tmp_proj, "package.json")
            if os.path.exists(pj):
                r = run_command(["npm", "install", "--silent"],
                                cwd=tmp_proj, timeout=MODULE_TIMEOUT)
                return r["ok"], ("deps installed" if r["ok"]
                                 else (r["stderr"] or "npm failed")[-300:])
            return True, "no package.json"
    except Exception as e:
        return False, str(e)
    return True, "n/a"

def run_test_for_project(chat_id, pid_, timeout=TEST_DEFAULT_TIMEOUT):
    meta = projects.get(pid_)
    if not meta:
        bot.send_message(chat_id, B("Not found.")); return
    lk = _get_test_lock(pid_)
    if not lk.acquire(blocking=False):
        bot.send_message(chat_id, B("Test already running.")); return

    prog_msg = None
    tmp_proj = None
    started = time.time()
    prev_status = meta.get("status", "stopped")
    deps_note = ""
    try:
        meta["status"] = "testing"; save_project(meta)
        prog_msg = bot.send_message(chat_id, Progress.testing()[0])
        for fr in Progress.testing():
            try: bot.edit_message_text(fr, chat_id, prog_msg.message_id)
            except Exception: pass
            time.sleep(0.15)

        tmp_proj = _copy_project_to_temp(meta)
        entry_abs = os.path.join(tmp_proj, meta["entry_file"])
        if not os.path.exists(entry_abs):
            raise RuntimeError("Entry missing in copy.")

        # install deps in temp copy only
        deps_ok, deps_note = _test_install_deps(meta, tmp_proj)
        if not deps_ok:
            deps_note = "deps install failed: " + deps_note

        if meta["file_type"] == "py":
            s_ok, s_out, s_err, s_rc = _validate_python(entry_abs, tmp_proj)
        elif meta["file_type"] == "js":
            s_ok, s_out, s_err, s_rc = _validate_node(entry_abs, tmp_proj)
        else:
            raise RuntimeError(f"Unsupported type: {meta['file_type']}")

        if not s_ok:
            dur = round(time.time() - started, 2)
            _send_test_result(chat_id, meta, False, "SYNTAX CHECK",
                              dur, s_rc, s_out, s_err, False, deps_note); return

        if meta["file_type"] == "py":
            r_ok, r_out, r_err, r_rc, r_to = _runtime_test_generic(
                [sys.executable, entry_abs], tmp_proj, timeout)
        else:
            r_ok, r_out, r_err, r_rc, r_to = _runtime_test_generic(
                ["node", entry_abs], tmp_proj, timeout)

        dur = round(time.time() - started, 2)
        _send_test_result(chat_id, meta, r_ok, "RUNTIME",
                          dur, r_rc, r_out, r_err, r_to, deps_note)
    except Exception as e:
        logger.error(f"test: {e}\n{traceback.format_exc()}")
        try: bot.send_message(chat_id, B(f"Test error: {e}"))
        except Exception: pass
    finally:
        try:
            meta = projects.get(pid_)
            if meta and meta.get("status") == "testing":
                meta["status"] = prev_status if prev_status != "testing" else "stopped"
                save_project(meta)
        except Exception:
            pass
        if tmp_proj:
            try: shutil.rmtree(os.path.dirname(tmp_proj), ignore_errors=True)
            except Exception: pass
        lk.release()

def _send_test_result(chat_id, meta, passed, phase, duration, exit_code,
                      stdout, stderr, timed_out, deps_note=""):
    status = "TIMEOUT" if timed_out else ("PASS" if passed else "FAIL")
    lines = [
        "🧪 TEST RESULT",
        "",
        f"Project: {meta['filename']}",
        f"Runtime: {meta['file_type']}",
        f"Phase: {phase}",
        f"Status: {status}",
        f"Exit Code: {exit_code}",
        f"Execution Time: {duration}s",
    ]
    if deps_note:
        lines.append(f"Deps: {deps_note}")
    out_t = (stdout or "")[-TEST_MAX_OUTPUT:]
    err_t = (stderr or "")[-TEST_MAX_OUTPUT:]
    txt = "\n".join(lines)
    if out_t.strip():
        txt += "\n\nOutput:\n" + out_t
    if err_t.strip():
        txt += "\n\nErrors:\n" + err_t
    if len(txt) > 3800:
        bot.send_message(chat_id, "\n".join(lines))
        bio = BytesIO(txt.encode()); bio.name = f"{meta['filename']}_test.txt"
        bot.send_document(chat_id, bio, caption=B("Full test output attached."))
    else:
        bot.send_message(chat_id, txt)

@bot.callback_query_handler(func=lambda c: c.data.startswith("t:"))
def cb_testing(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, B("Access Denied"), show_alert=True); return
    parts = call.data.split(":", 2)
    if len(parts) < 3 or parts[1] != "run":
        bot.answer_callback_query(call.id, "Bad"); return
    bot.answer_callback_query(call.id, B("Testing..."))
    threading.Thread(target=run_test_for_project,
                     args=(call.message.chat.id, parts[2]), daemon=True).start()

@bot.message_handler(commands=["test"])
def cmd_test(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, B("Access Denied")); return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        show_testing_menu(message.chat.id); return
    pid_ = args[1].strip()
    if pid_ not in projects:
        bot.reply_to(message, B("Not found.")); return
    threading.Thread(target=run_test_for_project,
                     args=(message.chat.id, pid_), daemon=True).start()
                     # ============================================================
# PART 4 — TERMINAL / PROCESS CONTROL / ALL FILES / RESTART ALL
# ============================================================
TERMINAL_TIMEOUT = 30
TERMINAL_MAX_OUTPUT = 3500

def prompt_terminal(chat_id):
    set_pending(chat_id, "terminal_input", {})
    bot.send_message(chat_id, B("💻 Terminal — enter command (one per message). /cancel to abort."))

@bot.message_handler(func=lambda m: (m.from_user.id == ADMIN_ID)
                                    and (get_pending(m.chat.id) or {}).get("kind") == "terminal_input"
                                    and bool(m.text))
def _terminal_input(message):
    clear_pending(message.chat.id)
    cmd = (message.text or "").strip()
    if not cmd:
        return
    _execute_terminal_and_reply(message.chat.id, cmd)

def _execute_terminal_and_reply(chat_id, cmd):
    bot.send_message(chat_id, B(f"$ {cmd}"))
    res = run_command(cmd, timeout=TERMINAL_TIMEOUT, shell=True)
    header = (f"Command: {cmd}\n"
              f"Exit: {res['rc']}\n"
              f"Time: {res['duration']}s\n"
              f"Timed out: {'yes' if res['timed_out'] else 'no'}")
    body = header
    if res["stdout"].strip():
        body += "\n\n-- STDOUT --\n" + res["stdout"][-TERMINAL_MAX_OUTPUT:]
    if res["stderr"].strip():
        body += "\n\n-- STDERR --\n" + res["stderr"][-TERMINAL_MAX_OUTPUT:]
    if len(body) > 3800:
        bot.send_message(chat_id, header)
        bio = BytesIO(body.encode()); bio.name = "terminal.txt"
        bot.send_document(chat_id, bio, caption=B("Full output attached."))
    else:
        bot.send_message(chat_id, body)

@bot.message_handler(commands=["sh", "term"])
def cmd_terminal(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, B("Access Denied")); return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        prompt_terminal(message.chat.id); return
    _execute_terminal_and_reply(message.chat.id, args[1])

@bot.message_handler(commands=["cancel"])
def cmd_cancel(message):
    if message.from_user.id != ADMIN_ID: return
    clear_pending(message.chat.id)
    bot.reply_to(message, B("Cancelled."))

def _fmt_uptime(sec):
    sec = int(sec or 0); h, r = divmod(sec, 3600); m, s = divmod(r, 60)
    return f"{h}h{m}m{s}s"

def _bot_process_info():
    p = psutil.Process(os.getpid())
    with p.oneshot():
        cpu = p.cpu_percent(interval=0.1)
        mem = p.memory_info().rss / (1024 * 1024)
        try: uptime = round(time.time() - p.create_time())
        except Exception: uptime = 0
    return {"pid": p.pid, "cpu": round(cpu, 1), "mem_mb": round(mem, 1), "uptime": uptime}

def _project_process_info(meta):
    pid_num = meta.get("pid") or 0
    if not pid_num: return {"alive": False, "cpu": None, "mem_mb": None}
    try:
        p = psutil.Process(pid_num)
        if not p.is_running() or p.status() == psutil.STATUS_ZOMBIE:
            return {"alive": False, "cpu": None, "mem_mb": None}
        cpu = p.cpu_percent(interval=0.05)
        mem = p.memory_info().rss / (1024 * 1024)
        return {"alive": True, "cpu": round(cpu, 1), "mem_mb": round(mem, 1)}
    except Exception:
        return {"alive": False, "cpu": None, "mem_mb": None}

def _process_control_text():
    b = _bot_process_info()
    lines = ["PROCESS CONTROL", "", "BOT:",
             f"PID {b['pid']}", f"CPU {b['cpu']}%", f"RAM {b['mem_mb']}MB",
             f"Uptime {_fmt_uptime(b['uptime'])}", "", "PROJECTS:"]
    if not projects: lines.append("(none)")
    for pid_, meta in projects.items():
        info = _project_process_info(meta)
        cpu = f"{info['cpu']}%" if info['cpu'] is not None else "-"
        ram = f"{info['mem_mb']}MB" if info['mem_mb'] is not None else "-"
        lines.append(f"{meta['filename']}  PID:{meta.get('pid') or 0}  CPU:{cpu}  RAM:{ram}")
    return "\n".join(lines)

def _process_control_list_kb():
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pid_, meta in projects.items():
        kb.add(types.InlineKeyboardButton(B(meta['filename']), callback_data=f"pc:view:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Refresh"), callback_data="pc:refresh"))
    return kb

def show_process_control(chat_id):
    bot.send_message(chat_id, _process_control_text(), reply_markup=_process_control_list_kb())

def _project_control_kb(pid_):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton(B("Stop"), callback_data=f"pc:stop:{pid_}"),
           types.InlineKeyboardButton(B("Restart"), callback_data=f"pc:restart:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Kill"), callback_data=f"pc:kill:{pid_}"),
           types.InlineKeyboardButton(B("Refresh"), callback_data=f"pc:view:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="pc:list"))
    return kb

@bot.callback_query_handler(func=lambda c: c.data.startswith("pc:"))
def cb_process_control(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, B("Access Denied"), show_alert=True); return
    parts = call.data.split(":", 2)
    action = parts[1]
    if action in ("refresh", "list"):
        bot.answer_callback_query(call.id, B("Refreshed"))
        try:
            bot.edit_message_text(_process_control_text(), call.message.chat.id,
                                  call.message.message_id,
                                  reply_markup=_process_control_list_kb())
        except Exception:
            show_process_control(call.message.chat.id)
        return
    if action == "view":
        pid_ = parts[2]
        meta = projects.get(pid_)
        if not meta:
            bot.answer_callback_query(call.id, B("Not found."), show_alert=True); return
        refresh_status(pid_)
        info = _project_process_info(meta)
        txt = (f"{meta['filename']}\nID {pid_}\n"
               f"Status {meta.get('status')}\nPID {meta.get('pid') or 0}\n"
               f"CPU {info['cpu'] if info['cpu'] is not None else '-'}\n"
               f"RAM {info['mem_mb'] if info['mem_mb'] is not None else '-'}MB")
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(txt, call.message.chat.id, call.message.message_id,
                                  reply_markup=_project_control_kb(pid_))
        except Exception:
            bot.send_message(call.message.chat.id, txt, reply_markup=_project_control_kb(pid_))
        return
    pid_ = parts[2] if len(parts) > 2 else None
    if action == "stop":
        bot.answer_callback_query(call.id, B("Stopping..."))
        ok, msg = stop_project(pid_)
        bot.send_message(call.message.chat.id, B(("OK " if ok else "FAIL ") + msg)); return
    if action == "restart":
        bot.answer_callback_query(call.id, B("Restarting..."))
        ok, msg = restart_project(pid_)
        bot.send_message(call.message.chat.id, B(("OK " if ok else "FAIL ") + msg)); return
    if action == "kill":
        bot.answer_callback_query(call.id, B("Killing..."))
        meta = projects.get(pid_)
        if meta and pid_ in bot_scripts:
            kill_process_tree(bot_scripts[pid_].get("process"))
            bot_scripts.pop(pid_, None)
            meta["status"] = "stopped"; meta["pid"] = 0
            meta["stop_time"] = datetime.now().isoformat()
            save_project(meta); cleanup_runtime_temp(pid_)
            bot.send_message(call.message.chat.id, B("Killed."))
        else:
            bot.send_message(call.message.chat.id, B("Not running."))
        return

def show_all_files(chat_id):
    if not projects:
        bot.send_message(chat_id, B("No projects.")); return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pid_, meta in projects.items():
        kb.add(types.InlineKeyboardButton(
            B(f"{meta['filename']} ({meta['file_type']}) {pid_}"),
            callback_data=f"af:view:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Refresh"), callback_data="af:refresh"))
    bot.send_message(chat_id, B("All Files:"), reply_markup=kb)

def _all_files_kb(pid_):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton(B("Start"), callback_data=f"af:start:{pid_}"),
           types.InlineKeyboardButton(B("Stop"), callback_data=f"af:stop:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Restart"), callback_data=f"af:restart:{pid_}"),
           types.InlineKeyboardButton(B("Test"), callback_data=f"af:test:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Status"), callback_data=f"af:status:{pid_}"),
           types.InlineKeyboardButton(B("Logs"), callback_data=f"af:logs:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Download"), callback_data=f"af:download:{pid_}"),
           types.InlineKeyboardButton(B("Backup"), callback_data=f"af:backup:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Clean Temp"), callback_data=f"af:cleantemp:{pid_}"),
           types.InlineKeyboardButton(B("Delete"), callback_data=f"af:delete:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Modules"), callback_data=f"p:modules:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("All Files"), callback_data="af:list"))
    return kb

def _all_files_view_text(pid_):
    meta = projects.get(pid_)
    if not meta: return B("Not found.")
    return (f"{meta['filename']}\nID {pid_}\nType {meta['file_type']}\n"
            f"Entry {meta['entry_file']}\nSize {meta['file_size']}\n"
            f"Uploaded {meta.get('upload_time') or '-'}\n"
            f"Status {meta.get('status')}\nPID {meta.get('pid') or 0}\n"
            f"Last start {meta.get('start_time') or '-'}\n"
            f"Last stop {meta.get('stop_time') or '-'}\n"
            f"Crash {meta.get('crash_time') or '-'}\n"
            f"Exit {meta.get('exit_code') if meta.get('exit_code') is not None else '-'}\n"
            f"Deps {meta.get('deps_status','unknown')}")

@bot.callback_query_handler(func=lambda c: c.data.startswith("af:"))
def cb_all_files(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, B("Access Denied"), show_alert=True); return
    parts = call.data.split(":", 2)
    action = parts[1]
    if action in ("list", "refresh"):
        bot.answer_callback_query(call.id, B("Refreshed"))
        try: bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception: pass
        show_all_files(call.message.chat.id); return
    pid_ = parts[2] if len(parts) > 2 else None
    meta = projects.get(pid_)
    if not meta:
        bot.answer_callback_query(call.id, B("Not found."), show_alert=True); return
    if action == "view":
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(_all_files_view_text(pid_), call.message.chat.id,
                                  call.message.message_id, reply_markup=_all_files_kb(pid_))
        except Exception:
            bot.send_message(call.message.chat.id, _all_files_view_text(pid_),
                             reply_markup=_all_files_kb(pid_))
        return
    if action == "start":
        bot.answer_callback_query(call.id, B("Starting..."))
        ok, msg = start_project(pid_)
        bot.send_message(call.message.chat.id, B(("OK " if ok else "FAIL ") + msg)); return
    if action == "stop":
        bot.answer_callback_query(call.id, B("Stopping..."))
        ok, msg = stop_project(pid_)
        bot.send_message(call.message.chat.id, B(("OK " if ok else "FAIL ") + msg)); return
    if action == "restart":
        bot.answer_callback_query(call.id, B("Restarting..."))
        ok, msg = restart_project(pid_)
        bot.send_message(call.message.chat.id, B(("OK " if ok else "FAIL ") + msg)); return
    if action == "test":
        bot.answer_callback_query(call.id, B("Testing..."))
        threading.Thread(target=run_test_for_project, args=(call.message.chat.id, pid_), daemon=True).start(); return
    if action == "status":
        bot.answer_callback_query(call.id); refresh_status(pid_)
        try:
            bot.edit_message_text(_all_files_view_text(pid_), call.message.chat.id,
                                  call.message.message_id, reply_markup=_all_files_kb(pid_))
        except Exception: pass
        return
    if action == "logs":
        bot.answer_callback_query(call.id); show_logs(call.message.chat.id, pid_); return
    if action == "download":
        bot.answer_callback_query(call.id)
        threading.Thread(target=download_project, args=(call.message.chat.id, pid_), daemon=True).start(); return
    if action == "backup":
        bot.answer_callback_query(call.id, B("Backing up..."))
        threading.Thread(target=create_backup, args=(call.message.chat.id, [pid_], "individual"), daemon=True).start(); return
    if action == "cleantemp":
        bot.answer_callback_query(call.id, B("Cleaning..."))
        cleanup_runtime_temp(pid_)
        bot.send_message(call.message.chat.id, B("Cleaned.")); return
    if action == "delete":
        bot.answer_callback_query(call.id)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton(B("Yes"), callback_data=f"af:delete_yes:{pid_}"),
               types.InlineKeyboardButton(B("No"), callback_data=f"af:view:{pid_}"))
        bot.send_message(call.message.chat.id, B("Delete this project?"), reply_markup=kb); return
    if action == "delete_yes":
        bot.answer_callback_query(call.id, B("Deleting..."))
        _delete_project(pid_)
        bot.send_message(call.message.chat.id, B("Deleted."))
        show_all_files(call.message.chat.id); return

def restart_all_handler(chat_id):
    if not projects:
        bot.send_message(chat_id, B("No projects.")); return
    prog = bot.send_message(chat_id, Progress.restart()[0])
    for fr in Progress.restart():
        try: bot.edit_message_text(fr, chat_id, prog.message_id)
        except Exception: pass
        time.sleep(0.15)
    results = []
    for pid_, meta in list(projects.items()):
        name = meta["filename"]
        prev = meta.get("status", "stopped")
        if prev == "testing":
            results.append(f"{name} -> Skipped (testing)"); continue
        try:
            if prev == "running":
                stop_project(pid_); time.sleep(0.5)
                ok, _ = start_project(pid_)
                results.append(f"{name} -> " + ("Restarted" if ok else "Failed"))
            elif prev in ("crashed", "error"):
                ok, _ = start_project(pid_)
                results.append(f"{name} -> " + ("Started" if ok else "Failed"))
            else:
                results.append(f"{name} -> Was stopped")
        except Exception as e:
            results.append(f"{name} -> Error {e}")
    header = "RESTART ALL RESULT"
    body = header + "\n\n" + "\n".join(results)
    if len(body) > 3800:
        bot.edit_message_text(B("Restart complete. See file."), chat_id, prog.message_id)
        bio = BytesIO(body.encode()); bio.name = "restart_all.txt"
        bot.send_document(chat_id, bio, caption=B(header))
    else:
        bot.edit_message_text(body, chat_id, prog.message_id)

@bot.message_handler(commands=["restartall"])
def cmd_restart_all(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, B("Access Denied")); return
    threading.Thread(target=restart_all_handler, args=(message.chat.id,), daemon=True).start()
    # ============================================================
# PART 5 — BACKUP & RECOVERY (with deps metadata, safe restore)
# ============================================================
BACKUP_MANIFEST_NAME = "manifest.json"
BACKUP_SCHEMA_VERSION = 2

_SECRET_RE = re.compile(
    r"(BOT_TOKEN|API_HASH|API_ID|TOKEN|SECRET|PASSWORD|PASSWD|PRIVATE_KEY|"
    r"SESSION|AUTH|COOKIE|BEARER)",
    re.IGNORECASE,
)

def _looks_like_secret_file(name):
    return bool(_SECRET_RE.search(os.path.basename(name))) or name.endswith(".env")

def _scrub_env_content(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except Exception:
        return ""
    out = []
    for ln in lines:
        if "=" in ln and _SECRET_RE.search(ln.split("=", 1)[0]):
            k = ln.split("=", 1)[0]
            out.append(f"{k}=<REDACTED>\n")
        else:
            out.append(ln)
    return "".join(out)

def show_backup_menu(chat_id):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(B("Create New Backup"), callback_data="bk:create"))
    kb.add(types.InlineKeyboardButton(B("Backup History"), callback_data="bk:history"))
    kb.add(types.InlineKeyboardButton(B("Download All"), callback_data="bk:dlall"))
    kb.add(types.InlineKeyboardButton(B("Upload All"), callback_data="bk:upload"))
    kb.add(types.InlineKeyboardButton(B("Data Download (per project)"), callback_data="bk:data"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="m:back"))
    bot.send_message(chat_id, B("BACKUP & RECOVERY"), reply_markup=kb)

def _safe_copy_tree(src_root, dst_root, skip_names=()):
    src_root = os.path.realpath(src_root)
    os.makedirs(dst_root, exist_ok=True)
    for root, dirs, files in os.walk(src_root, followlinks=False):
        dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(root, d))]
        rel = os.path.relpath(root, src_root)
        target = dst_root if rel == "." else os.path.join(dst_root, rel)
        os.makedirs(target, exist_ok=True)
        for name in files:
            if name in skip_names: continue
            src_f = os.path.join(root, name)
            if os.path.islink(src_f): continue
            dst_f = os.path.join(target, name)
            try:
                if _looks_like_secret_file(name):
                    with open(dst_f, "w", encoding="utf-8") as f:
                        f.write(_scrub_env_content(src_f))
                else:
                    shutil.copy2(src_f, dst_f)
            except Exception as e:
                logger.warning(f"backup copy {src_f}: {e}")

def _project_backup_folder(pid_, base_out):
    meta = projects.get(pid_)
    if not meta: return None
    root = os.path.join(base_out, pid_)
    os.makedirs(root, exist_ok=True)
    _safe_copy_tree(meta["project_dir"], os.path.join(root, "project"))

    md = os.path.join(root, "metadata"); os.makedirs(md, exist_ok=True)
    clean = {
        "project_id": meta["project_id"],
        "file_id": meta.get("file_id"),
        "owner_id": meta.get("owner_id"),
        "filename": meta["filename"],
        "file_type": meta["file_type"],
        "file_size": meta.get("file_size", 0),
        "upload_time": meta.get("upload_time"),
        "entry_file": meta.get("entry_file"),
        "requirements": meta.get("requirements"),
        "env_info": meta.get("env_info"),
        "historical_running": 1 if meta.get("status") == "running" else meta.get("historical_running", 0),
        "last_start_time": meta.get("start_time"),
        "last_stop_time": meta.get("stop_time"),
        "last_crash_time": meta.get("crash_time"),
        "last_exit_code": meta.get("exit_code"),
        "deps_status": meta.get("deps_status"),
        "deps_last_install": meta.get("deps_last_install"),
    }
    with open(os.path.join(md, "project.json"), "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2)

    # dependency metadata
    deps_dir = os.path.join(root, "dependencies")
    os.makedirs(deps_dir, exist_ok=True)
    try:
        mods = db_fetchall("SELECT manager, module_name, version, status FROM project_modules WHERE project_id=?",
                           (pid_,))
        with open(os.path.join(deps_dir, "installed_modules.json"), "w", encoding="utf-8") as f:
            json.dump(mods, f, indent=2)
    except Exception:
        pass

    rt = os.path.join(root, "runtime"); os.makedirs(rt, exist_ok=True)
    for fn in ("run.log", "run.err"):
        sp = os.path.join(meta["runtime_dir"], fn)
        if os.path.exists(sp):
            try:
                with open(sp, "r", encoding="utf-8", errors="ignore") as f:
                    tail = f.read()[-20000:]
                with open(os.path.join(rt, fn), "w", encoding="utf-8") as f:
                    f.write(tail)
            except Exception: pass

    rc = os.path.join(root, "recovery"); os.makedirs(rc, exist_ok=True)
    with open(os.path.join(rc, "recovery_info.json"), "w", encoding="utf-8") as f:
        json.dump({"restore_policy": "never_restore_pid",
                   "start_fresh": True,
                   "built_at": datetime.now().isoformat()}, f, indent=2)
    return root

def _build_backup_archive(project_ids, kind):
    backup_id = "bk_" + datetime.now().strftime("%Y%m%d_%H%M%S_") + \
                hashlib.sha1(str(time.time()).encode()).hexdigest()[:6]
    staging = tempfile.mkdtemp(prefix="bkstage_")
    try:
        payload = os.path.join(staging, "BACKUP"); os.makedirs(payload, exist_ok=True)
        included = []
        for pid_ in project_ids:
            if pid_ not in projects: continue
            if _project_backup_folder(pid_, payload): included.append(pid_)
        manifest = {"schema": BACKUP_SCHEMA_VERSION, "backup_id": backup_id,
                    "created_at": datetime.now().isoformat(), "kind": kind,
                    "project_ids": included,
                    "host_info": {"python": sys.version.split()[0], "platform": sys.platform}}
        with open(os.path.join(payload, BACKUP_MANIFEST_NAME), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        archive_path = os.path.join(BACKUP_DIR, f"{backup_id}.tar.gz")
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(payload, arcname="BACKUP")
        size_bytes = os.path.getsize(archive_path)
        db_execute("INSERT OR REPLACE INTO backups (backup_id, created_at, kind, project_ids, archive_path, size_bytes, status) VALUES (?,?,?,?,?,?,?)",
                   (backup_id, manifest["created_at"], kind, json.dumps(included),
                    archive_path, size_bytes, "ok"))
        return {"backup_id": backup_id, "archive_path": archive_path,
                "size_bytes": size_bytes, "kind": kind, "project_ids": included}
    finally:
        shutil.rmtree(staging, ignore_errors=True)

def _show_data_download_menu(chat_id):
    if not projects:
        bot.send_message(chat_id, B("No projects.")); return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pid_, meta in projects.items():
        kb.add(types.InlineKeyboardButton(B(meta['filename']), callback_data=f"bk:proj:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("DOWNLOAD ALL"), callback_data="bk:dlall"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="bk:menu"))
    bot.send_message(chat_id, B("DATA DOWNLOAD:"), reply_markup=kb)

def download_project(chat_id, pid_):
    meta = projects.get(pid_)
    if not meta:
        bot.send_message(chat_id, B("Not found.")); return
    prog = bot.send_message(chat_id, Progress.backup()[0])
    for fr in Progress.backup():
        try: bot.edit_message_text(fr, chat_id, prog.message_id)
        except Exception: pass
        time.sleep(0.15)
    try:
        res = _build_backup_archive([pid_], kind="individual")
        with open(res["archive_path"], "rb") as f:
            data = f.read()
        bio = BytesIO(data); bio.name = f"{meta['filename']}_backup.tar.gz"
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton(B("Backup this"), callback_data=f"bk:new:{pid_}"))
        bot.send_document(chat_id, bio,
                          caption=B(f"Download {meta['filename']}\nID {res['backup_id']}\nSize {res['size_bytes']}"),
                          reply_markup=kb)
        try: bot.delete_message(chat_id, prog.message_id)
        except Exception: pass
    except Exception as e:
        logger.error(f"download_project: {e}")
        bot.send_message(chat_id, B(f"Download failed: {e}"))

def _download_all(chat_id):
    if not projects:
        bot.send_message(chat_id, B("No projects.")); return
    prog = bot.send_message(chat_id, Progress.backup()[0])
    for fr in Progress.backup():
        try: bot.edit_message_text(fr, chat_id, prog.message_id)
        except Exception: pass
        time.sleep(0.15)
    try:
        res = _build_backup_archive(list(projects.keys()), kind="full")
        with open(res["archive_path"], "rb") as f:
            data = f.read()
        bio = BytesIO(data); bio.name = f"FULL_BACKUP_{res['backup_id']}.tar.gz"
        bot.send_document(chat_id, bio,
                          caption=B(f"FULL BACKUP\nID {res['backup_id']}\nProjects {len(res['project_ids'])}\nSize {res['size_bytes']}"))
        try: bot.delete_message(chat_id, prog.message_id)
        except Exception: pass
    except Exception as e:
        logger.error(f"download_all: {e}")
        bot.send_message(chat_id, B(f"Download All failed: {e}"))

def create_backup(chat_id, project_ids, kind="individual"):
    try:
        res = _build_backup_archive(project_ids, kind=kind)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton(B("Download"), callback_data=f"bk:dl:{res['backup_id']}"))
        bot.send_message(chat_id, B(f"Backup Created\nID {res['backup_id']}\nTime {datetime.now().isoformat(timespec='seconds')}\nProjects {len(res['project_ids'])}\nSize {res['size_bytes']}\nType {kind}"),
                         reply_markup=kb)
        return res["backup_id"]
    except Exception as e:
        logger.error(f"create_backup: {e}")
        bot.send_message(chat_id, B(f"Backup failed: {e}"))
        return None

def _show_create_backup_menu(chat_id):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pid_, meta in projects.items():
        kb.add(types.InlineKeyboardButton(B(meta['filename']), callback_data=f"bk:new:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Backup All"), callback_data="bk:newall"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="bk:menu"))
    bot.send_message(chat_id, B("CREATE NEW BACKUP:"), reply_markup=kb)

def _show_backup_history(chat_id):
    rows = db_fetchall("SELECT * FROM backups ORDER BY created_at DESC LIMIT 50")
    if not rows:
        bot.send_message(chat_id, B("No backups yet.")); return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for r in rows:
        try: pids = json.loads(r.get("project_ids") or "[]")
        except Exception: pids = []
        kb.add(types.InlineKeyboardButton(
            B(f"{r['backup_id']} {r['kind']} {len(pids)}p {r['size_bytes']}B"),
            callback_data=f"bk:view:{r['backup_id']}"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="bk:menu"))
    bot.send_message(chat_id, B("BACKUP HISTORY:"), reply_markup=kb)

def _show_backup_detail(chat_id, backup_id):
    r = db_fetchone("SELECT * FROM backups WHERE backup_id = ?", (backup_id,))
    if not r:
        bot.send_message(chat_id, B("Not found.")); return
    try: pids = json.loads(r.get("project_ids") or "[]")
    except Exception: pids = []
    lines = [f"Backup {backup_id}", f"Time {r.get('created_at')}",
             f"Kind {r.get('kind')}", f"Projects {len(pids)}",
             f"Size {r.get('size_bytes')}B", f"Status {r.get('status')}", "", "Projects:"]
    for p in pids:
        m = projects.get(p)
        lines.append(f"- {m['filename'] if m else p}")
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton(B("Download"), callback_data=f"bk:dl:{backup_id}"),
           types.InlineKeyboardButton(B("Restore"), callback_data=f"bk:restore:{backup_id}"))
    kb.add(types.InlineKeyboardButton(B("Delete"), callback_data=f"bk:del:{backup_id}"))
    kb.add(types.InlineKeyboardButton(B("History"), callback_data="bk:history"))
    bot.send_message(chat_id, "\n".join(lines), reply_markup=kb)

def _send_backup_file(chat_id, backup_id):
    r = db_fetchone("SELECT * FROM backups WHERE backup_id = ?", (backup_id,))
    if not r or not os.path.exists(r["archive_path"]):
        bot.send_message(chat_id, B("Backup file missing.")); return
    with open(r["archive_path"], "rb") as f:
        data = f.read()
    bio = BytesIO(data); bio.name = f"{backup_id}.tar.gz"
    bot.send_document(chat_id, bio, caption=B(backup_id))

def _delete_backup(chat_id, backup_id):
    r = db_fetchone("SELECT * FROM backups WHERE backup_id = ?", (backup_id,))
    if not r:
        bot.send_message(chat_id, B("Not found.")); return
    try:
        if os.path.exists(r["archive_path"]): os.remove(r["archive_path"])
    except Exception as e:
        logger.warning(f"del bk file: {e}")
    db_execute("DELETE FROM backups WHERE backup_id = ?", (backup_id,))
    bot.send_message(chat_id, B(f"Deleted {backup_id}"))

_backup_uploads = {}
_backup_guard = threading.Lock()

def _mark_awaiting_backup_upload(chat_id):
    with _backup_guard:
        _backup_uploads[chat_id] = True
    bot.send_message(chat_id, B("Upload your .tar.gz backup."))

def _parse_uploaded_backup(blob):
    tmp = tempfile.mkdtemp(prefix="bkupload_")
    try:
        ap = os.path.join(tmp, "in.tar.gz")
        with open(ap, "wb") as f: f.write(blob)
        if not tarfile.is_tarfile(ap):
            raise ValueError("Not a .tar.gz")
        members = []
        with tarfile.open(ap, "r:gz") as tar:
            for m in tar.getmembers():
                n = m.name.replace("\\", "/")
                if n.startswith("/") or ".." in n.split("/"):
                    raise ValueError(f"Unsafe: {m.name}")
                members.append(n)
            try:
                mf = tar.extractfile("BACKUP/manifest.json")
                manifest = json.loads(mf.read().decode("utf-8"))
            except KeyError:
                raise ValueError("manifest.json missing")
            except Exception as e:
                raise ValueError(f"manifest: {e}")
        if manifest.get("schema") != BACKUP_SCHEMA_VERSION:
            raise ValueError(f"schema {manifest.get('schema')} != {BACKUP_SCHEMA_VERSION}")
        return manifest, members, ap
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise

def _handle_backup_upload(message):
    chat_id = message.chat.id
    with _backup_guard:
        if not _backup_uploads.get(chat_id): return
        _backup_uploads.pop(chat_id, None)
    doc = message.document
    if not doc or not (doc.file_name or "").endswith((".tar.gz", ".tgz")):
        bot.send_message(chat_id, B("Upload .tar.gz.")); return
    try:
        info = bot.get_file(doc.file_id)
        blob = bot.download_file(info.file_path)
        manifest, members, ap = _parse_uploaded_backup(blob)
    except Exception as e:
        bot.send_message(chat_id, B(f"Invalid backup: {e}")); return
    _stash_backup_upload(chat_id, ap, manifest, members)
    pids = manifest.get("project_ids", [])
    lines = [f"Backup valid", f"ID {manifest.get('backup_id')}",
             f"Time {manifest.get('created_at')}", f"Kind {manifest.get('kind')}",
             f"Projects {len(pids)}", "", "In backup:"]
    for p in pids: lines.append(f"- {p}")
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(B("RESTORE"), callback_data="bk:restore_stashed"))
    kb.add(types.InlineKeyboardButton(B("UPLOAD ALL (restore everything)"), callback_data="bk:restore_stashed_all"))
    kb.add(types.InlineKeyboardButton(B("Cancel"), callback_data="bk:cancel_upload"))
    bot.send_message(chat_id, "\n".join(lines), reply_markup=kb)

_uploaded_backups = {}
_uploaded_guard = threading.Lock()

def _stash_backup_upload(chat_id, ap, manifest, members):
    with _uploaded_guard:
        old = _uploaded_backups.get(chat_id)
        if old and os.path.exists(os.path.dirname(old["path"])):
            shutil.rmtree(os.path.dirname(old["path"]), ignore_errors=True)
        _uploaded_backups[chat_id] = {"path": ap, "manifest": manifest, "members": members}

def _restore_from_staging(chat_id, restore_all, only_pid=None):
    """Restore uploaded backup with per-project atomic rollback.

    Existing projects are never deleted until the replacement project has been
    fully copied into a staging location. If anything fails, the old project
    and metadata remain intact. Restored projects are always STOPPED with PID=0.
    """
    with _uploaded_guard:
        entry = _uploaded_backups.get(chat_id)
    if not entry:
        bot.send_message(chat_id, B("No uploaded backup. Upload again.")); return

    ap = entry["path"]; manifest = entry["manifest"]
    pids = manifest.get("project_ids", [])
    if only_pid:
        pids = [only_pid] if only_pid in pids else []
    if not pids:
        bot.send_message(chat_id, B("Nothing to restore.")); return

    prog = bot.send_message(chat_id, Progress.recovery()[0])
    for fr in Progress.recovery():
        try: bot.edit_message_text(fr, chat_id, prog.message_id)
        except Exception: pass
        time.sleep(0.15)

    extract_root = tempfile.mkdtemp(prefix="bkextract_")
    transaction_root = tempfile.mkdtemp(prefix="bkrestore_")
    restored, failed = [], []
    try:
        # Validate every archive member before extraction.
        with tarfile.open(ap, "r:gz") as tar:
            for m in tar.getmembers():
                n = m.name.replace("\\", "/")
                if n.startswith("/") or ".." in n.split("/"):
                    raise ValueError(f"Unsafe on restore: {m.name}")
            tar.extractall(extract_root)

        payload = os.path.join(extract_root, "BACKUP")
        if not os.path.isdir(payload):
            raise ValueError("BACKUP/ missing")

        for pid_ in pids:
            src = os.path.join(payload, pid_)
            mf = os.path.join(src, "metadata", "project.json")
            proj_src = os.path.join(src, "project")
            if not os.path.isdir(src) or not os.path.isfile(mf):
                failed.append(f"{pid_} missing"); continue

            old_meta = projects.get(pid_)
            old_project_dir = old_meta.get("project_dir") if old_meta else None
            old_runtime_dir = old_meta.get("runtime_dir") if old_meta else None
            old_db_meta = dict(old_meta) if old_meta else None
            was_running = bool(old_meta and old_meta.get("status") == "running")
            stopped_for_restore = False
            backup_id = None

            try:
                with open(mf, "r", encoding="utf-8") as f:
                    meta = json.load(f)

                # Build a safety backup before touching a live existing project.
                if old_meta:
                    try:
                        safety = _build_backup_archive([pid_], kind="pre_restore_safety")
                        backup_id = safety.get("backup_id")
                    except Exception as e:
                        raise RuntimeError(f"Safety backup failed: {e}")
                    if was_running:
                        ok, msg = stop_project(pid_)
                        if not ok:
                            raise RuntimeError(f"Could not stop existing project: {msg}")
                        stopped_for_restore = True

                # Stage the replacement completely before touching the live path.
                staged_project = os.path.join(transaction_root, pid_, "project")
                os.makedirs(staged_project, exist_ok=True)
                if os.path.isdir(proj_src):
                    _safe_copy_tree(proj_src, staged_project)
                else:
                    raise RuntimeError("project/ missing in backup")

                staged_runtime = os.path.join(transaction_root, pid_, "runtime")
                os.makedirs(staged_runtime, exist_ok=True)
                for fn in ("run.log", "run.err"):
                    ls = os.path.join(src, "runtime", fn)
                    if os.path.isfile(ls):
                        shutil.copy2(ls, os.path.join(staged_runtime, fn))

                new_dir = os.path.join(UPLOAD_BOTS_DIR, pid_)
                runtime_dir = os.path.join(HOSTING_DATA_DIR, "runtime", pid_)
                live_old = None
                live_runtime_old = None

                # Move the old live tree aside instead of deleting it. This makes
                # the filesystem operation reversible if the final swap fails.
                if os.path.exists(new_dir):
                    live_old = os.path.join(transaction_root, pid_, "old_project")
                    os.makedirs(os.path.dirname(live_old), exist_ok=True)
                    os.replace(new_dir, live_old)
                if os.path.exists(runtime_dir):
                    live_runtime_old = os.path.join(transaction_root, pid_, "old_runtime")
                    os.makedirs(os.path.dirname(live_runtime_old), exist_ok=True)
                    os.replace(runtime_dir, live_runtime_old)

                try:
                    os.makedirs(os.path.dirname(new_dir), exist_ok=True)
                    os.replace(staged_project, new_dir)
                    os.makedirs(os.path.dirname(runtime_dir), exist_ok=True)
                    os.replace(staged_runtime, runtime_dir)
                except Exception:
                    # Roll back the filesystem swap immediately.
                    shutil.rmtree(new_dir, ignore_errors=True)
                    shutil.rmtree(runtime_dir, ignore_errors=True)
                    if live_old and os.path.exists(live_old):
                        os.replace(live_old, new_dir)
                    if live_runtime_old and os.path.exists(live_runtime_old):
                        os.replace(live_runtime_old, runtime_dir)
                    raise

                restored_meta = {
                    "project_id": pid_, "file_id": meta.get("file_id"),
                    "owner_id": meta.get("owner_id", ADMIN_ID),
                    "filename": meta.get("filename") or os.path.basename(new_dir),
                    "file_type": meta.get("file_type") or "py",
                    "file_size": meta.get("file_size") or 0,
                    "upload_time": meta.get("upload_time") or datetime.now().isoformat(),
                    "project_dir": new_dir, "runtime_dir": runtime_dir,
                    "entry_file": meta.get("entry_file") or meta.get("filename"),
                    "requirements": meta.get("requirements") or "[]",
                    "env_info": meta.get("env_info") or "{}",
                    "pid": 0, "status": "stopped",
                    "start_time": None, "stop_time": datetime.now().isoformat(),
                    "crash_time": None, "exit_code": None, "error_log": None,
                    "backup_id": backup_id or manifest.get("backup_id"),
                    "recovery_info": json.dumps({
                        "restored_at": datetime.now().isoformat(),
                        "from_backup": manifest.get("backup_id"),
                        "pre_restore_safety_backup": backup_id,
                        "historical_running": meta.get("historical_running", 0),
                        "start_fresh": True
                    }),
                    "historical_running": meta.get("historical_running", 0),
                    "deps_status": meta.get("deps_status") or "unknown",
                    "deps_last_install": meta.get("deps_last_install"),
                    "deps_last_error": None,
                }

                # Only after the filesystem swap succeeds do we commit metadata.
                save_project(restored_meta)
                deps_file = os.path.join(src, "dependencies", "installed_modules.json")
                if deps_file and os.path.isfile(deps_file):
                    try:
                        with open(deps_file, "r", encoding="utf-8") as f:
                            mods = json.load(f)
                        db_execute("DELETE FROM project_modules WHERE project_id=?", (pid_,))
                        for m in mods:
                            name = m.get("module_name") or ""
                            if name:
                                _save_installed_module(pid_, m.get("manager") or "pip",
                                                       name, m.get("version") or "",
                                                       m.get("status") or "ok")
                    except Exception as e:
                        logger.warning(f"dependency metadata restore {pid_}: {e}")

                # Old trees are removed only after the new project is committed.
                if live_old and os.path.exists(live_old):
                    shutil.rmtree(live_old, ignore_errors=True)
                if live_runtime_old and os.path.exists(live_runtime_old):
                    shutil.rmtree(live_runtime_old, ignore_errors=True)
                restored.append(pid_)

            except Exception as e:
                logger.error(f"restore {pid_}: {e}\n{traceback.format_exc()}")
                # Restore in-memory + DB metadata when the project existed before.
                if old_db_meta:
                    try:
                        projects[pid_] = old_db_meta
                        save_project(old_db_meta)
                    except Exception: pass
                failed.append(f"{pid_}: {e}")

                # If we stopped a running project and restoration failed, restart
                # the original project from its untouched source tree.
                if stopped_for_restore and old_db_meta:
                    try: start_project(pid_)
                    except Exception as restart_e:
                        logger.error(f"rollback restart {pid_}: {restart_e}")

        lines = ["RESTORE COMPLETE", f"Restored {len(restored)} project(s)"]
        if restored:
            lines.append("\nRestored:")
            for p in restored:
                lines.append(f"- {projects[p]['filename']} ({p}) [STOPPED]")
        if failed:
            lines.append(f"\nFailed {len(failed)}:")
            lines.extend(f"- {x}" for x in failed)
        body = "\n".join(lines)
        if len(body) > 3800:
            bot.edit_message_text(B("Restore complete. See file."), chat_id, prog.message_id)
            bio = BytesIO(body.encode()); bio.name = "restore.txt"
            bot.send_document(chat_id, bio, caption=B("RESTORE COMPLETE"))
        else:
            bot.edit_message_text(body, chat_id, prog.message_id)
    except Exception as e:
        logger.error(f"restore: {e}\n{traceback.format_exc()}")
        bot.send_message(chat_id, B(f"Restore failed: {e}"))
    finally:
        shutil.rmtree(extract_root, ignore_errors=True)
        shutil.rmtree(transaction_root, ignore_errors=True)

@bot.callback_query_handler(func=lambda c: c.data.startswith("bk:"))
def cb_backup(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, B("Access Denied"), show_alert=True); return
    parts = call.data.split(":", 2)
    action = parts[1]
    arg = parts[2] if len(parts) > 2 else None
    chat_id = call.message.chat.id
    if action == "menu":
        bot.answer_callback_query(call.id); show_backup_menu(chat_id); return
    if action == "data":
        bot.answer_callback_query(call.id); _show_data_download_menu(chat_id); return
    if action == "create":
        bot.answer_callback_query(call.id); _show_create_backup_menu(chat_id); return
    if action == "history":
        bot.answer_callback_query(call.id); _show_backup_history(chat_id); return
    if action == "upload":
        bot.answer_callback_query(call.id); _mark_awaiting_backup_upload(chat_id); return
    if action == "proj":
        bot.answer_callback_query(call.id)
        meta = projects.get(arg)
        if not meta:
            bot.send_message(chat_id, B("Not found.")); return
        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(types.InlineKeyboardButton(B("DOWNLOAD"), callback_data=f"bk:dlp:{arg}"))
        kb.add(types.InlineKeyboardButton(B("BACKUP"), callback_data=f"bk:new:{arg}"))
        kb.add(types.InlineKeyboardButton(B("Back"), callback_data="bk:data"))
        bot.send_message(chat_id, B(meta['filename']), reply_markup=kb); return
    if action == "dlp":
        bot.answer_callback_query(call.id, B("Downloading...")); download_project(chat_id, arg); return
    if action == "dlall":
        bot.answer_callback_query(call.id, B("Downloading...")); _download_all(chat_id); return
    if action == "new":
        bot.answer_callback_query(call.id, B("Backing up...")); create_backup(chat_id, [arg], "individual"); return
    if action == "newall":
        bot.answer_callback_query(call.id, B("Backing up...")); create_backup(chat_id, list(projects.keys()), "full"); return
    if action == "view":
        bot.answer_callback_query(call.id); _show_backup_detail(chat_id, arg); return
    if action == "dl":
        bot.answer_callback_query(call.id, B("Sending...")); _send_backup_file(chat_id, arg); return
    if action == "del":
        bot.answer_callback_query(call.id, B("Deleting...")); _delete_backup(chat_id, arg); return
    if action == "restore":
        bot.answer_callback_query(call.id, B("Restoring..."))
        r = db_fetchone("SELECT * FROM backups WHERE backup_id = ?", (arg,))
        if not r or not os.path.exists(r["archive_path"]):
            bot.send_message(chat_id, B("Backup file missing.")); return
        try:
            with open(r["archive_path"], "rb") as f: blob = f.read()
            manifest, members, ap = _parse_uploaded_backup(blob)
            _stash_backup_upload(chat_id, ap, manifest, members)
            threading.Thread(target=_restore_from_staging, args=(chat_id, False), daemon=True).start()
        except Exception as e:
            bot.send_message(chat_id, B(f"Restore failed: {e}"))
        return
    if action == "restore_stashed":
        bot.answer_callback_query(call.id, B("Restoring..."))
        threading.Thread(target=_restore_from_staging, args=(chat_id, False), daemon=True).start(); return
    if action == "restore_stashed_all":
        bot.answer_callback_query(call.id, B("Restoring..."))
        threading.Thread(target=_restore_from_staging, args=(chat_id, True), daemon=True).start(); return
    if action == "cancel_upload":
        bot.answer_callback_query(call.id, B("Cancelled"))
        with _uploaded_guard:
            e = _uploaded_backups.pop(chat_id, None)
        if e: shutil.rmtree(os.path.dirname(e["path"]), ignore_errors=True)
        return
    bot.answer_callback_query(call.id, "Unknown")

@bot.message_handler(content_types=["document"], func=lambda m: True)
def on_upload_or_backup(message):
    with _backup_guard:
        pending = _backup_uploads.get(message.chat.id, False)
    if pending:
        _handle_backup_upload(message); return
    on_upload(message)
    # ============================================================
# PART 6 — SPEED / STATS / PROFILE / ADMIN HELPERS / STARTUP / MAIN
# ============================================================
def _speed_measure_download(url, timeout=15):
    start = time.time()
    try:
        r = requests.get(url, timeout=timeout, stream=True)
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}", "bytes": 0, "seconds": 0, "mbps": 0}
        total = 0
        for chunk in r.iter_content(chunk_size=65536):
            if chunk: total += len(chunk)
            if total >= 10 * 1024 * 1024: break
        dur = max(time.time() - start, 0.001)
        return {"ok": True, "bytes": total, "seconds": round(dur, 2),
                "mbps": round((total * 8) / dur / (1024*1024), 2), "error": None}
    except Exception as e:
        return {"ok": False, "error": str(e), "bytes": 0, "seconds": 0, "mbps": 0}

def _speed_measure_upload(endpoint, timeout=15):
    payload = os.urandom(1024 * 1024)
    start = time.time()
    try:
        r = requests.post(endpoint, data=payload, timeout=timeout)
        dur = max(time.time() - start, 0.001)
        if r.status_code >= 500:
            return {"ok": False, "error": f"HTTP {r.status_code}", "bytes": 0, "seconds": 0, "mbps": 0}
        return {"ok": True, "bytes": len(payload), "seconds": round(dur, 2),
                "mbps": round((len(payload) * 8) / dur / (1024*1024), 2), "error": None}
    except Exception as e:
        return {"ok": False, "error": str(e), "bytes": 0, "seconds": 0, "mbps": 0}

def _speed_measure_ping(host="1.1.1.1", timeout=5):
    try:
        cmd = ["ping", "-n" if os.name == "nt" else "-c", "3", host]
        start = time.time()
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        dur = (time.time() - start) * 1000
        if r.returncode != 0:
            return {"ok": False, "error": (r.stderr or "ping fail").strip(), "ms": None}
        m = re.search(r"(?:rtt|round-trip).*?=\s*[\d.]+/([\d.]+)/", r.stdout)
        if not m: m = re.search(r"Average\s*=\s*(\d+)ms", r.stdout)
        avg = float(m.group(1)) if m else round(dur / 3, 1)
        return {"ok": True, "ms": round(avg, 2), "error": None}
    except FileNotFoundError:
        return {"ok": False, "error": "ping not found", "ms": None}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "ping timeout", "ms": None}
    except Exception as e:
        return {"ok": False, "error": str(e), "ms": None}

def run_speed_test(chat_id):
    prog = bot.send_message(chat_id, B("Running speed test..."))
    start = time.time()
    p = _speed_measure_ping("1.1.1.1")
    d = _speed_measure_download("https://speed.cloudflare.com/__down?bytes=10000000")
    u = _speed_measure_upload("https://httpbin.org/post")
    dur = round(time.time() - start, 2)
    lines = ["SPEED TEST RESULT", ""]
    if p.get("ok"): lines.append(f"Ping: {p['ms']} ms")
    else: lines.append(f"Ping: FAIL - {p.get('error')}")
    if d.get("ok"): lines.append(f"Download: {d['mbps']} Mbps ({d['bytes']}B in {d['seconds']}s)")
    else: lines.append(f"Download: FAIL - {d.get('error')}")
    if u.get("ok"): lines.append(f"Upload: {u['mbps']} Mbps ({u['bytes']}B in {u['seconds']}s)")
    else: lines.append(f"Upload: FAIL - {u.get('error')}")
    lines.append("")
    lines.append(f"Python: {sys.version.split()[0]}")
    lines.append(f"Platform: {sys.platform}")
    lines.append(f"CPU count: {os.cpu_count()}")
    lines.append(f"Duration: {dur}s")
    txt = "\n".join(lines)
    try: bot.edit_message_text(txt, chat_id, prog.message_id)
    except Exception: bot.send_message(chat_id, txt)

def _count_statuses():
    c = {"running":0, "stopped":0, "starting":0, "testing":0, "crashed":0, "error":0}
    for meta in projects.values():
        st = meta.get("status", "stopped")
        if st in c: c[st] += 1
        else: c["stopped"] += 1
    return c

def show_stats(chat_id):
    try:
        cpu = psutil.cpu_percent(interval=0.5)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(BASE_DIR)
        uptime = int(time.time() - psutil.boot_time())
    except Exception as e:
        bot.send_message(chat_id, B(f"Stats error: {e}")); return
    c = _count_statuses()
    def gb(n): return round(n / (1024**3), 2)
    txt = (f"SYSTEM STATS\n\n"
           f"CPU: {cpu}%\n"
           f"RAM: {mem.percent}% ({gb(mem.used)}/{gb(mem.total)} GB)\n"
           f"Disk: {disk.percent}% ({gb(disk.used)}/{gb(disk.total)} GB)\n"
           f"Uptime: {_fmt_uptime(uptime)}\n\n"
           f"PROJECTS:\n"
           f"Total: {len(projects)}\n"
           f"Running: {c['running']}\n"
           f"Stopped: {c['stopped']}\n"
           f"Starting: {c['starting']}\n"
           f"Testing: {c['testing']}\n"
           f"Crashed: {c['crashed']}\n"
           f"Error: {c['error']}")
    bot.send_message(chat_id, txt)

def show_profile(chat_id):
    c = _count_statuses()
    try:
        row = db_fetchone("SELECT COUNT(*) AS n FROM backups")
        backup_count = row["n"] if row else 0
    except Exception:
        backup_count = 0
    total_size = sum(int(m.get("file_size") or 0) for m in projects.values())
    txt = (f"PROFILE\n\n"
           f"Admin ID: {ADMIN_ID}\n"
           f"Total projects: {len(projects)}\n"
           f"Running: {c['running']}\n"
           f"Stopped: {c['stopped']}\n"
           f"Testing: {c['testing']}\n"
           f"Crashed: {c['crashed']}\n"
           f"Error: {c['error']}\n"
           f"Backups: {backup_count}\n"
           f"Storage: {round(total_size/1024, 1)} KB\n"
           f"Access: single-admin")
    bot.send_message(chat_id, txt)

@bot.message_handler(commands=["module"])
def cmd_module(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, B("Access Denied")); return
    show_module_manager_root(message.chat.id)

def _admin_overview_text():
    try:
        cpu = psutil.cpu_percent(interval=0.3)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(BASE_DIR)
        uptime = int(time.time() - psutil.boot_time())
    except Exception as e:
        return B(f"Overview error: {e}")
    c = _count_statuses()
    try:
        backup_count = db_fetchone("SELECT COUNT(*) AS n FROM backups")["n"]
    except Exception:
        backup_count = 0
    bot_pid = os.getpid()
    def gb(n): return round(n / (1024**3), 2)
    return (f"👑 ADMIN PANEL — SYSTEM OVERVIEW\n\n"
            f"Bot PID: {bot_pid}\n"
            f"Status: RUNNING\n"
            f"CPU: {cpu}%\n"
            f"RAM: {mem.percent}% ({gb(mem.used)}/{gb(mem.total)} GB)\n"
            f"Disk: {disk.percent}% ({gb(disk.used)}/{gb(disk.total)} GB)\n"
            f"Host uptime: {_fmt_uptime(uptime)}\n\n"
            f"Projects: {len(projects)}\n"
            f"Running: {c['running']}\n"
            f"Stopped: {c['stopped']}\n"
            f"Crashed: {c['crashed']}\n"
            f"Testing: {c['testing']}\n"
            f"Backups: {backup_count}")

def _admin_projects_list(chat_id):
    if not projects:
        bot.send_message(chat_id, B("No projects.")); return
    kb = types.InlineKeyboardMarkup(row_width=1)
    for pid_, meta in projects.items():
        st = meta.get("status", "stopped")
        icon = {"running":"G","stopped":"-","starting":"S","testing":"T","crashed":"C","error":"E"}.get(st, "-")
        kb.add(types.InlineKeyboardButton(
            B(f"[{icon}] {meta['filename']} ({meta['file_type']})"),
            callback_data=f"p:view:{pid_}"))
    kb.add(types.InlineKeyboardButton(B("Back"), callback_data="ad:back"))
    bot.send_message(chat_id, B("All Projects:"), reply_markup=kb)

def _admin_error_logs(chat_id):
    rows = db_fetchall("SELECT project_id, filename, status, error_log, crash_time, exit_code "
                       "FROM projects WHERE error_log IS NOT NULL AND error_log != '' "
                       "ORDER BY crash_time DESC LIMIT 20")
    if not rows:
        bot.send_message(chat_id, B("No error logs recorded.")); return
    lines = [B("RECENT ERROR LOGS"), ""]
    for r in rows:
        lines.append(f"[{r['status']}] {r['filename']} (exit {r.get('exit_code')})")
        tail = (r.get("error_log") or "")[-300:]
        lines.append(tail)
        lines.append("----")
    body = "\n".join(lines)
    if len(body) > 3800:
        bio = BytesIO(body.encode()); bio.name = "error_logs.txt"
        bot.send_document(chat_id, bio, caption=B("Recent error logs"))
    else:
        bot.send_message(chat_id, body)

@bot.callback_query_handler(func=lambda c: c.data.startswith("ad:"))
def cb_admin_panel(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, B("Access Denied"), show_alert=True); return
    action = call.data.split(":", 1)[1]
    if action == "overview":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, _admin_overview_text()); return
    if action == "projects":
        bot.answer_callback_query(call.id); _admin_projects_list(call.message.chat.id); return
    if action == "errlogs":
        bot.answer_callback_query(call.id); _admin_error_logs(call.message.chat.id); return
    if action == "lock":
        bot.answer_callback_query(call.id)
        try:
            with open(LOCK_FILE) as f: data = json.load(f)
            bot.send_message(call.message.chat.id,
                             B(f"Lock PID {data.get('pid')} since {data.get('started')}"))
        except Exception as e:
            bot.send_message(call.message.chat.id, B(f"Lock read error: {e}"))
        return
    if action == "restart":
        bot.answer_callback_query(call.id, B("Restarting..."))
        _do_bot_restart(call.message.chat.id); return
    if action == "back":
        bot.answer_callback_query(call.id)
        show_admin_panel(call.message.chat.id); return
    bot.answer_callback_query(call.id, "Unknown")

def _do_bot_restart(chat_id):
    try: bot.send_message(chat_id, Progress.restart()[0])
    except Exception: pass
    for pid_ in list(projects.keys()):
        try: stop_project(pid_)
        except Exception: pass
    time.sleep(1)
    try: release_single_instance_lock()
    except Exception: pass
    try: os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        logger.error(f"reexec failed: {e}")

@bot.message_handler(commands=["restartbot"])
def cmd_restartbot(message):
    if message.from_user.id != ADMIN_ID:
        bot.reply_to(message, B("Access Denied")); return
    _do_bot_restart(message.chat.id)

def startup_recovery():
    logger.info("Startup recovery...")
    restored, errored = [], []
    for pid_, meta in list(projects.items()):
        if not os.path.isdir(meta.get("project_dir", "")):
            meta["status"] = "error"
            meta["error_log"] = "project_dir missing at startup"
            save_project(meta); errored.append(meta["filename"]); continue
        try: project_runtime_dir(pid_)
        except Exception: pass
        if meta.get("historical_running"):
            entry_abs = os.path.join(meta["project_dir"], meta["entry_file"])
            if not os.path.exists(entry_abs):
                meta["status"] = "error"
                meta["error_log"] = "entry missing at startup"
                save_project(meta); errored.append(meta["filename"]); continue
            ok, _ = start_project(pid_)
            if ok:
                restored.append(meta["filename"])
                meta["historical_running"] = 0
                save_project(meta)
            else:
                errored.append(meta["filename"])
    if restored or errored:
        try:
            lines = ["STARTUP RECOVERY"]
            if restored:
                lines.append(f"Restarted {len(restored)}:")
                for n in restored: lines.append(f"- {n}")
            if errored:
                lines.append(f"Errors {len(errored)}:")
                for n in errored: lines.append(f"- {n}")
            bot.send_message(ADMIN_ID, "\n".join(lines))
        except Exception as e:
            logger.warning(f"startup notify: {e}")
    logger.info(f"Startup recovery done (restored={len(restored)}, errored={len(errored)})")

flask_app = Flask(__name__)

@flask_app.route("/")
def _flask_home(): return "HOSTING BOT 5.0 — running"

@flask_app.route("/healthz")
def _flask_health():
    return {"ok": True, "projects": len(projects), "ts": datetime.now().isoformat()}

def _run_flask():
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port)

def keep_alive():
    Thread(target=_run_flask, daemon=True).start()
    logger.info(f"Flask keep-alive on {os.environ.get('PORT', 8080)}")

_exiting = threading.Event()

def _shutdown_all():
    if _exiting.is_set(): return
    _exiting.set()
    logger.warning("Shutdown — stopping projects...")
    _crash_stop.set()
    for pid_ in list(projects.keys()):
        try: stop_project(pid_)
        except Exception: pass
    try: release_single_instance_lock()
    except Exception: pass
    logger.info("Shutdown complete.")

atexit.register(_shutdown_all)

def _sig_handler(signum, frame):
    logger.warning(f"Signal {signum}")
    _shutdown_all()
    sys.exit(0)

try:
    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)
except Exception: pass

if __name__ == "__main__":
    logger.info("HOSTING BOT 5.0 starting")
    logger.info(f"ADMIN_ID: {ADMIN_ID}")
    logger.info(f"Projects: {len(projects)}")

    keep_alive()
    threading.Thread(target=crash_watcher, daemon=True).start()
    logger.info("Crash watcher started")
    threading.Thread(target=startup_recovery, daemon=True).start()

    while True:
        try:
            logger.info("Polling started")
            bot.infinity_polling(timeout=60, long_polling_timeout=30)
        except requests.exceptions.ReadTimeout:
            logger.warning("Read timeout — retry 5s"); time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Conn err {e} — retry 15s"); time.sleep(15)
        except Exception as e:
            logger.critical(f"Poll error {e}\n{traceback.format_exc()}")
            logger.info("Retry 30s"); time.sleep(30)

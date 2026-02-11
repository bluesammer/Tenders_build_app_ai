#!/usr/bin/env python
# coding: utf-8

# wo things you still need to wire on the Lovable side.
# 
# get-open-current-csv, returns a CSV with column sol_number (or Sol#)
# 
# put-closed-sol-csv and put-new-open-enriched-csv, accept CSV upload and store it in Storage or load it into tables for your combine step
# 
# If you already have only one endpoint, tell me its name and I will align the script URLs and request headers to match it.
# 3 files tenders_open_current = this is the master file
# tender_was_open_now_close_live = tenders where the status was open in "tenders_open_current" but is now "closed"
# tender_append_fresh_data = new tenders which are not in the tenders_open_current. yet
# 

# You want a single script with a top switch.
# 
# You want two outputs every run.
# 
# closed file, Sol# that were open yesterday, now awarded or missing from today’s open set
# 
# new open enriched file, only brand new open Sol# since yesterday, with your cleaning + GPT columns
# 
# You also want these rules.
# 
# Railway mode pulls the open list CSV from Lovable, pushes two CSV outputs back to Lovable
# 
# Local mode reads the open list CSV from your Windows path, writes the two outputs locally
# 
# GPT category must use your fixed 20 list, no “learn from master”
# 
# If a step fails, keep going and still produce the two output files, push what exists
# 
# Below is a full drop-in script. Change only the CONFIG section.

# In[4]:


#!/usr/bin/env python
# coding: utf-8

import os
import re
import json
import time
import random
from datetime import date
from pathlib import Path
from io import StringIO

import requests
import pandas as pd
from openai import OpenAI

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =========================================================
# CONFIG
# =========================================================

USE_RAILWAY_MODE = True

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
RAILWAY_API_SECRET = os.getenv("RAILWAY_API_SECRET", "").strip()

DOWNLOAD_SAM = True
MIN_SAM_BYTES_SKIP_DOWNLOAD = 150_000_000
DISABLE_GPT = False

MAX_GPT_ROWS_TOTAL = int(os.getenv("MAX_GPT_ROWS_TOTAL", "10"))
GPT_BATCH_SIZE = 25
MODEL = "gpt-4o-mini"

RANDOM_SAMPLE = False
RANDOM_SEED = 42
SKIP_IF_ALREADY_ENRICHED = True

GPT_TIMEOUT_SEC = 60
MAX_RETRIES_429 = 8
PRINT_EVERY_BATCH = 1
SLEEP_BETWEEN_BATCH_SEC = 0.15

STOP_GPT_ON_RPD_LIMIT = True

UPLOAD_TIMEOUT_SEC = int(os.getenv("UPLOAD_TIMEOUT_SEC", "600"))
UPLOAD_FRESH_LIMIT_25 = os.getenv("UPLOAD_FRESH_LIMIT_25", "false").strip().lower() in ("1", "true", "yes")

# Chunk upload to avoid 504
FRESH_UPLOAD_CHUNK_ROWS = int(os.getenv("FRESH_UPLOAD_CHUNK_ROWS", "400"))  # set 0 to disable
UPLOAD_SLEEP_BETWEEN_PARTS_SEC = float(os.getenv("UPLOAD_SLEEP_BETWEEN_PARTS_SEC", "0.35"))

# Lovable endpoints
OPEN_LIST_ENDPOINT = "https://okynhjreumnekmeudwje.supabase.co/functions/v1/get-railway-csv"
UPLOAD_RESULTS_ENDPOINT = "https://okynhjreumnekmeudwje.supabase.co/functions/v1/upload-railway-results"

# Remote filenames Lovable expects
CLOSED_REMOTE_FILENAME = "tender_was_open_now_close_live.csv"
FRESH_REMOTE_FILENAME = "tender_append_fresh_data.csv"
OPEN_REMOTE_FILENAME = "tenders_open_current.csv"

LOCAL_OPEN_LIST_PATH = r"C:\Users\Blues\upwork\10000k_conrcats_merx_govt_contracts_USA 2026 -Better\downloads\maybe just swap with same name\tenders_open_current-export-2026-02-09_12-59-53.csv"


# =========================================================
# VALIDATION
# =========================================================

if (DISABLE_GPT is False) and (OPENAI_API_KEY == ""):
    raise RuntimeError("OPENAI_API_KEY missing in environment")

if USE_RAILWAY_MODE and (RAILWAY_API_SECRET == ""):
    raise RuntimeError("RAILWAY_API_SECRET missing for Railway mode")


# =========================================================
# PATHS
# =========================================================

DOWNLOAD_DIR = Path("downloads").resolve()
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

SAM_CSV_NAME = "ContractOpportunitiesFullCSV.csv"
SAM_CSV_PATH = DOWNLOAD_DIR / SAM_CSV_NAME

CLOSED_OUT_PATH = DOWNLOAD_DIR / "tenders_now_closed.csv"
NEW_OPEN_OUT_PATH = DOWNLOAD_DIR / "tenders_add_fresh.csv"


# =========================================================
# HELPERS
# =========================================================

def bytes_human(n: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    f = float(n)
    for u in units:
        if f < 1024.0:
            return f"{f:.1f}{u}"
        f /= 1024.0
    return f"{f:.1f}TB"


def safe_print_exception(tag: str, e: Exception) -> None:
    print("[ERROR]", tag + ":", type(e).__name__ + ":", str(e))


def read_csv_safely_path(p: Path) -> pd.DataFrame:
    encodings = ["utf-8", "cp1252", "latin1"]
    for enc in encodings:
        try:
            return pd.read_csv(p, encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            pass
    return pd.read_csv(p, encoding="utf-8", errors="replace", low_memory=False)


def read_csv_safely_text(text: str) -> pd.DataFrame:
    return pd.read_csv(StringIO(text), low_memory=False)


def clean_text(x, limit: int) -> str:
    if pd.isna(x):
        return ""
    s = str(x)
    s = s.replace("\x00", " ")
    s = re.sub(r"[\x01-\x08\x0B\x0C\x0E-\x1F]", " ", s)
    s = s.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:limit]


def http_get_csv_df(url: str, secret: str, timeout: int = 90) -> pd.DataFrame:
    resp = requests.get(url, headers={"x-railway-secret": secret}, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError("GET failed " + str(resp.status_code) + ": " + resp.text[:800])
    if resp.text.strip() == "":
        return pd.DataFrame()
    return read_csv_safely_text(resp.text)


def _validate_remote_filename(remote_filename: str) -> str:
    allowed = {
        "tenders_open_current.csv",
        "tender_was_open_now_close_live.csv",
        "tender_append_fresh_data.csv",
    }
    fn = (remote_filename or "").strip()
    if fn == "":
        raise RuntimeError("Remote filename empty")
    if fn in allowed:
        return fn
    raise RuntimeError("Invalid file name. Allowed: " + ", ".join(sorted(list(allowed))))


def post_csv_dual_keys(url: str, secret: str, csv_bytes: bytes, remote_filename: str, timeout: int) -> None:
    fn = _validate_remote_filename(remote_filename)

    # Dual keys + explicit filename field
    files = {
        "file": (fn, csv_bytes, "text/csv"),
        fn: (fn, csv_bytes, "text/csv"),
    }
    data = {"filename": fn}

    resp = requests.post(
        url,
        headers={"x-railway-secret": secret},
        files=files,
        data=data,
        timeout=timeout,
    )
    if resp.status_code in (200, 201, 204):
        print("Upload OK:", fn, "bytes:", len(csv_bytes))
        return
    raise RuntimeError("POST failed " + str(resp.status_code) + ": " + resp.text[:800])


def post_df_with_retries(url: str, secret: str, df: pd.DataFrame, remote_filename: str, timeout: int) -> None:
    fn = _validate_remote_filename(remote_filename)
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    for attempt in (1, 2, 3):
        try:
            post_csv_dual_keys(url, secret, csv_bytes, fn, timeout)
            return
        except requests.exceptions.Timeout:
            if attempt == 3:
                raise RuntimeError("POST timed out 3 times for " + fn)
            backoff = 2 * attempt
            print("[WARN] Upload timeout. retry in", backoff, "sec. attempt", attempt, "/3. file:", fn)
            time.sleep(backoff)
        except Exception as e:
            if attempt == 3:
                raise
            backoff = 2 * attempt
            print("[WARN] Upload failed. retry in", backoff, "sec. attempt", attempt, "/3. file:", fn, "err:", str(e)[:200])
            time.sleep(backoff)


def post_df_maybe_chunked(url: str, secret: str, df: pd.DataFrame, remote_filename: str, timeout: int, chunk_rows: int) -> None:
    fn = _validate_remote_filename(remote_filename)

    if df is None:
        df = pd.DataFrame()

    if df.empty:
        print("Upload empty:", fn)
        post_df_with_retries(url, secret, pd.DataFrame(), fn, timeout)
        return

    if chunk_rows <= 0:
        print("Upload single:", fn, "rows:", len(df), "cols:", len(df.columns))
        post_df_with_retries(url, secret, df, fn, timeout)
        return

    total = len(df)
    parts = []
    start = 0
    while start < total:
        end = start + chunk_rows
        if end > total:
            end = total
        parts.append(df.iloc[start:end].copy())
        start = end

    print("Upload chunked:", fn, "chunks:", len(parts), "rows:", total, "chunk_rows:", chunk_rows)
    for i, part in enumerate(parts, start=1):
        print("Uploading part", i, "/", len(parts), "rows", len(part))
        post_df_with_retries(url, secret, part, fn, timeout)
        time.sleep(UPLOAD_SLEEP_BETWEEN_PARTS_SEC)


def load_previous_open_list() -> pd.DataFrame:
    if USE_RAILWAY_MODE:
        df_open = http_get_csv_df(OPEN_LIST_ENDPOINT, RAILWAY_API_SECRET, timeout=90)
    else:
        df_open = read_csv_safely_path(Path(LOCAL_OPEN_LIST_PATH))

    if df_open.empty:
        return pd.DataFrame({"Sol#": []})

    df_open.columns = [c.strip() for c in df_open.columns]

    if "Sol#" in df_open.columns:
        out = df_open[["Sol#"]].copy()
    elif "sol_number" in df_open.columns:
        out = df_open.rename(columns={"sol_number": "Sol#"})[["Sol#"]].copy()
    else:
        first = df_open.columns[0]
        out = df_open.rename(columns={first: "Sol#"})[["Sol#"]].copy()

    out["Sol#"] = out["Sol#"].astype(str).str.strip()
    out = out.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "(blank)": pd.NA})
    out = out.dropna(subset=["Sol#"]).drop_duplicates(subset=["Sol#"], keep="first")
    return out


CHROME_BIN = os.getenv("CHROME_BIN", "/usr/bin/chromium")
CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")


def build_driver(download_dir: Path) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    if USE_RAILWAY_MODE:
        if (os.path.exists(CHROME_BIN) is False) or (os.path.exists(CHROMEDRIVER_PATH) is False):
            raise RuntimeError("Chrome runtime missing")
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.binary_location = CHROME_BIN
        service = Service(CHROMEDRIVER_PATH)
        return webdriver.Chrome(service=service, options=options)

    options.add_argument("--start-maximized")
    return webdriver.Chrome(options=options)


def wait_for_file_stable(path: Path, timeout_sec: int = 1200, stable_sec: int = 20, min_bytes: int = 50_000_000) -> None:
    start = time.time()
    last_size = -1
    stable_start = None

    while (time.time() - start) < timeout_sec:
        if path.exists():
            size = path.stat().st_size
            if size >= min_bytes:
                if size == last_size:
                    if stable_start is None:
                        stable_start = time.time()
                    elif (time.time() - stable_start) >= stable_sec:
                        return
                else:
                    stable_start = None
                last_size = size
        time.sleep(2)

    raise RuntimeError("Timeout waiting for stable file")


def norm_sol(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = s.replace("\u00a0", " ").strip()
    if (len(s) >= 2) and (s[0] == "(") and (s[-1] == ")"):
        s = s[1:-1].strip()
    if s.startswith("-"):
        s = s[1:].strip()
    s = re.sub(r"[^A-Za-z0-9\-]", "", s)
    return s.upper().strip()


def validate_summary(summary: str) -> str:
    s = re.sub(r"\s+", " ", str(summary)).strip()
    if len(s) > 260:
        s = s[:260].rstrip()
    if "." in s:
        first = s.split(".", 1)[0].strip()
        if first != "":
            s = first + "."
    return s


def fallback_summary(title: str, desc: str) -> str:
    t = clean_text(title, 140)
    d = clean_text(desc, 220)
    if t != "":
        return validate_summary("Provide " + t.lower() + ".")
    if d != "":
        base = d
        if base.endswith(".") is False:
            base = base + "."
        return validate_summary("Provide " + base)
    return "Provide requested goods or services."


def chunk_list(items, chunk_size: int):
    out = []
    i = 0
    while i < len(items):
        out.append(items[i:i + chunk_size])
        i += chunk_size
    return out


def detect_retry_after_seconds(err: Exception) -> float:
    msg = str(err)
    m = re.search(r"try again in ([0-9]+(?:\.[0-9]+)?)s", msg, re.IGNORECASE)
    if m:
        return float(m.group(1))
    return 10.0


# =========================================================
# CATEGORY (8)
# =========================================================

KW_MED = re.compile(r"\b(medical|clinical|hospital|pharma|pharmaceutical|drug|laborator(y|ies)|diagnostic|patient|clinic|healthcare|nursing)\b", re.I)
KW_ENERGY = re.compile(r"\b(energy|utility|utilities|power|electric|electricity|solar|wind|generator|fuel|grid|substation|transformer)\b", re.I)

KW_CONS1 = re.compile(r"\b(construction|renovation|remodel|hvac|plumbing|roof|roofing|concrete|asphalt|paving|carpentry|drywall)\b", re.I)
KW_CONS2 = re.compile(r"\b(facilit(y|ies)|building|grounds|janitorial|custodial|maintenance|repair|installation|install|inspection|testing|commissioning|elevator|fire alarm|sprinkler|boiler|chiller)\b", re.I)

KW_TRANS = re.compile(r"\b(logistics|freight|carrier|trucking|courier|fleet|dispatch|drayage|container|intermodal|rail|air cargo|shipping|shipment)\b", re.I)
KW_MANU = re.compile(r"\b(manufactur|fabricat|equipment|machin|parts|components|metal|electronics|vehicle|aerospace|industrial|tooling|assembly)\b", re.I)

KW_PRO1 = re.compile(r"\b(consult|consulting|consultant|advisory|assessment|strategy|roadmap|governance|risk management|change management)\b", re.I)
KW_PRO2 = re.compile(r"\b(staffing|recruit|recruiting|talent|\bhr\b|payroll|accounting|bookkeep|legal|compliance|audit)\b", re.I)
KW_PRO3 = re.compile(r"\b(training services|training|curriculum|instructional|instructor|facilitat|workshop|conference|event)\b", re.I)
KW_PRO4 = re.compile(r"\b(administrat|admin|program support|technical assistance|analysis|research|evaluation|policy|communications|outreach|public affairs|writing|editing|translation|interpreting|records management|data entry)\b", re.I)
KW_PRO5 = re.compile(r"\b(managed services|service desk|help desk|call center|contact center|back office)\b", re.I)

KW_IT_STRONG = re.compile(r"\b(cyber|infosec|\bsoc\b|siem|zero trust|firewall|vpn|edr|iam|mfa|penetration|vulnerability|cisa|nist)\b", re.I)
KW_IT_MED = re.compile(r"\b(software|saas|cloud|azure|aws|gcp|kubernetes|devops|api|database|crm|erp|helpdesk|it support|endpoint|microsoft 365|\bo365\b)\b", re.I)
KW_IT_WEAK = re.compile(r"\b(data|server|network)\b", re.I)


def psc_prefix(x):
    s = str(x).strip().upper()
    return s[0] if len(s) > 0 else ""


def text_blob_raw(r):
    parts = [
        str(r.get("PRODUCT AND SERVICE CODE NAME", "")),
        str(r.get("2022 NAICS Title", "")),
        str(r.get("Title", "")),
        str(r.get("Description", "")),
    ]
    return " ".join(parts).lower()


def map_category_8_local(row):
    p = psc_prefix(row.get("PSC CODE", ""))
    t = text_blob_raw(row)

    if KW_IT_STRONG.search(t) or KW_IT_MED.search(t):
        return "IT and Cyber"
    if KW_MED.search(t):
        return "Medical and Healthcare"
    if KW_ENERGY.search(t):
        return "Energy and Utilities"
    if KW_PRO1.search(t) or KW_PRO2.search(t) or KW_PRO3.search(t) or KW_PRO4.search(t) or KW_PRO5.search(t):
        return "Professional Services"
    if KW_CONS1.search(t) or KW_CONS2.search(t):
        return "Construction and Trades"
    if KW_TRANS.search(t):
        return "Transportation and Logistics"
    if KW_MANU.search(t):
        return "Manufacturing"

    if p == "D":
        return "Professional Services"
    if p in ["J", "S", "Y"]:
        return "Construction and Trades"
    if p == "N":
        return "Medical and Healthcare"
    if (p in ["F", "G", "H", "K"]) or p.isdigit():
        return "Manufacturing"
    if p in ["R", "T"]:
        return "Professional Services"

    return "Other"


# =========================================================
# GPT SUMMARY
# =========================================================

def gpt_summary_batch(client: OpenAI, records: list) -> dict:
    payload = {"items": records}
    prompt = (
        "You will receive JSON with key items, a list of records.\n"
        "Return JSON with key items, a list of outputs.\n"
        "Each output must include: Sol#, summary_1_sentence.\n\n"
        "summary_1_sentence rules:\n"
        "One sentence.\n"
        "Start with Provide or Deliver or Install or Maintain or Supply.\n"
        "Describe what is being purchased.\n"
        "Avoid procurement language.\n"
        "Avoid mentioning RFQ, solicitation, BPA, BAA, FAR, DFARS, amendments, deadlines, NAICS, set-aside.\n"
        "Max 260 chars.\n\n"
        "Input JSON:\n"
        + json.dumps(payload)
    )

    resp = client.responses.create(
        model=MODEL,
        input=prompt,
        text={"format": {"type": "json_object"}},
        timeout=GPT_TIMEOUT_SEC,
    )

    data = json.loads(resp.output_text)
    out_items = data.get("items", [])
    out_map = {}
    for it in out_items:
        sol = norm_sol(it.get("Sol#", ""))
        if sol == "":
            continue
        out_map[sol] = validate_summary(it.get("summary_1_sentence", ""))
    return out_map


# =========================================================
# MAIN
# =========================================================

print("=== STEP 0: LOAD PREVIOUS OPEN LIST ===")
open_list_loaded_ok = False
try:
    prev_open_df = load_previous_open_list()
    prev_open_df["Sol#"] = prev_open_df["Sol#"].apply(norm_sol)
    prev_open_df = prev_open_df.loc[prev_open_df["Sol#"] != ""].copy()
    prev_open_df = prev_open_df.drop_duplicates(subset=["Sol#"], keep="first")
    print("Previous open Sol# count:", len(prev_open_df))
    print("Prev open sample:", prev_open_df["Sol#"].head(5).tolist())
    open_list_loaded_ok = True
except Exception as e:
    safe_print_exception("Load previous open list", e)
    prev_open_df = pd.DataFrame({"Sol#": []})

if USE_RAILWAY_MODE and (open_list_loaded_ok is False):
    raise RuntimeError("Open list load failed in Railway mode")

prev_open_set = set(prev_open_df["Sol#"].dropna().astype(str))
if "" in prev_open_set:
    prev_open_set.remove("")

print("=== STEP 1: SAM.GOV DOWNLOAD ===")

if (DOWNLOAD_SAM is True) and SAM_CSV_PATH.exists() and (SAM_CSV_PATH.stat().st_size >= MIN_SAM_BYTES_SKIP_DOWNLOAD):
    print("SAM file already present and large enough. Skipping download.")
else:
    if DOWNLOAD_SAM is True:
        driver = None
        try:
            driver = build_driver(DOWNLOAD_DIR)
            wait = WebDriverWait(driver, 90)

            print("Browser launched. Navigating to SAM.gov dataset page...")
            driver.get("https://sam.gov/data-services/Contract%20Opportunities/datagov?privacy=Public")

            wait.until(lambda d: "SAM.gov" in d.title)
            print("Page title:", driver.title)

            print("Waiting for dataset list...")
            wait.until(lambda d: "ContractOpportunitiesFullCSV" in d.page_source)

            csv_elem = driver.find_element(By.XPATH, "//*[contains(text(),'ContractOpportunitiesFullCSV.csv')]")
            driver.execute_script("arguments[0].scrollIntoView(true);", csv_elem)
            time.sleep(1)

            print("Clicking CSV link...")
            driver.execute_script(
                """
                var el = arguments[0];
                var evt = new MouseEvent('click', {bubbles:true, cancelable:true, view:window});
                el.dispatchEvent(evt);
                """,
                csv_elem
            )

            print("Waiting for terms modal...")
            modal = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".sds-dialog-content.height-mobile"))
            )

            accepted = False
            for _ in range(7):
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", modal)
                time.sleep(1)
                try:
                    accept_button = driver.find_element(By.XPATH, "//button[@class='usa-button' and normalize-space()='Accept']")
                    if accept_button.is_enabled():
                        accept_button.click()
                        accepted = True
                        break
                except Exception:
                    pass

            if accepted is True:
                print("Terms accepted.")

            time.sleep(2)

            print("Clicking CSV link again to start download...")
            csv_elem2 = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//*[contains(text(),'ContractOpportunitiesFullCSV.csv')]"))
            )
            csv_elem2.click()

            print("Waiting for SAM file to become stable...")
            wait_for_file_stable(SAM_CSV_PATH, timeout_sec=1200, stable_sec=20, min_bytes=50_000_000)
            print("SAM file stable:", str(SAM_CSV_PATH))

        except Exception as e:
            safe_print_exception("SAM download", e)
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

print("SAM_CSV_PATH:", str(SAM_CSV_PATH), "| exists:", SAM_CSV_PATH.exists())
if SAM_CSV_PATH.exists():
    print("SAM size:", bytes_human(SAM_CSV_PATH.stat().st_size))

print("=== STEP 2: LOAD SAM (FULL) + RECENT FILTER ===")

df_full = pd.DataFrame()
df_recent = pd.DataFrame()

try:
    if SAM_CSV_PATH.exists() is False:
        raise RuntimeError("SAM CSV missing")

    df_full = read_csv_safely_path(SAM_CSV_PATH)
    print("SAM raw rows:", len(df_full), "| cols:", len(df_full.columns))

    if "Sol#" not in df_full.columns:
        raise RuntimeError("SAM missing Sol# column")

    df_full["Sol#"] = df_full["Sol#"].apply(norm_sol)

    if "PostedDate" in df_full.columns:
        tmp = df_full.copy()
        tmp["PostedDate"] = pd.to_datetime(tmp["PostedDate"], errors="coerce", utc=True)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.DateOffset(months=6)
        df_recent = tmp.loc[tmp["PostedDate"].notna() & (tmp["PostedDate"] >= cutoff)].copy()
    else:
        df_recent = df_full.copy()

    cols_to_drop = [
        "ArchiveType", "OrganizationType", "NoticeId", "AdditionalInfoLink",
        "Award$", "Awardee", "AwardNumber",
        "PopCity", "PopCountry", "PopState", "PopStreetAddress", "PopZip",
        "PrimaryContactFax", "PrimaryContactTitle", "SecondaryContactFax", "SecondaryContactTitle",
        "SetASide", "SetASideCode",
        "CGAC", "FPDS Code", "AAC Code", "ZipCode", "CountryCode", "Type",
        "BaseType", "Office", "Department/Ind.Agency", "ArchiveDate"
    ]
    before_cols = len(df_recent.columns)
    df_recent = df_recent.drop(columns=cols_to_drop, errors="ignore")
    print("Recent rows:", len(df_recent), "| cols before:", before_cols, "| cols after:", len(df_recent.columns))

except Exception as e:
    safe_print_exception("Load SAM", e)
    df_full = pd.DataFrame()
    df_recent = pd.DataFrame()

print("=== STEP 3: CLOSED FROM PREV OPEN (FULL SAM) ===")

closed_out_df = pd.DataFrame(columns=["Sol#", "close_reason", "AwardDate"])

try:
    if df_full.empty:
        raise RuntimeError("SAM full df empty")

    awarded_set = set()
    award_map = {}

    if "AwardDate" in df_full.columns:
        awarded_rows = df_full.loc[df_full["AwardDate"].notna(), ["Sol#", "AwardDate"]].copy()
        awarded_rows["AwardDate"] = awarded_rows["AwardDate"].astype(str).str[:10]
        awarded_set = set(awarded_rows["Sol#"].astype(str))
        award_map = dict(zip(awarded_rows["Sol#"].astype(str), awarded_rows["AwardDate"]))

        closed_awarded = sorted(list(prev_open_set.intersection(awarded_set)))

        rows = []
        for sol in closed_awarded:
            rows.append({"Sol#": sol, "close_reason": "awarded", "AwardDate": award_map.get(sol, "")})

        closed_out_df = pd.DataFrame(rows)

    open_today_count = len(set(df_full["Sol#"].dropna().astype(str).unique()))
    print("Prev open:", len(prev_open_set), "| Open today:", open_today_count, "| Closed:", len(closed_out_df))

except Exception as e:
    safe_print_exception("Closed detection", e)
    closed_out_df = pd.DataFrame(columns=["Sol#", "close_reason", "AwardDate"])

print("=== STEP 4: NEW OPEN TO ENRICH (RECENT ONLY) ===")

main_up6 = pd.DataFrame()
try:
    if df_recent.empty:
        raise RuntimeError("SAM recent df empty")

    df_recent["Sol#"] = df_recent["Sol#"].apply(norm_sol)

    if "AwardDate" in df_recent.columns:
        df_open_recent = df_recent.loc[df_recent["AwardDate"].isna()].copy()
    else:
        df_open_recent = df_recent.copy()

    df_open_recent = df_open_recent.dropna(subset=["Sol#"]).copy()
    df_open_recent = df_open_recent.loc[df_open_recent["Sol#"] != ""].copy()

    is_prev = df_open_recent["Sol#"].isin(prev_open_set)
    new_open_df = df_open_recent.loc[is_prev == False].copy()
    print("New open rows (recent):", len(new_open_df))

    if ("PostedDate" in new_open_df.columns) and (len(new_open_df) > 0):
        new_open_df["PostedDate"] = pd.to_datetime(new_open_df["PostedDate"], errors="coerce", utc=True)
        new_open_df = new_open_df.sort_values("PostedDate", ascending=False)

    before_dups = len(new_open_df)
    new_open_df = new_open_df.drop_duplicates(subset=["Sol#"], keep="first").copy()
    print("New open unique Sol# (recent):", len(new_open_df), "dropped:", before_dups - len(new_open_df))

    front_cols = ["Sol#", "PostedDate", "ResponseDeadLine"]
    front = [c for c in front_cols if c in new_open_df.columns]
    rest = [c for c in new_open_df.columns if c in front_cols]  # placeholder, kept minimal
    rest = [c for c in new_open_df.columns if c not in front]
    main_up6 = new_open_df.loc[:, front + rest].copy()

    if "PostedDate" in main_up6.columns:
        main_up6["PostedDate"] = pd.to_datetime(main_up6["PostedDate"], errors="coerce", utc=True).astype(str).str[:10]
    if "ResponseDeadLine" in main_up6.columns:
        main_up6["ResponseDeadLine"] = main_up6["ResponseDeadLine"].astype(str).str[:10]

except Exception as e:
    safe_print_exception("New-open detection", e)
    main_up6 = pd.DataFrame()

print("=== STEP 5: CLEAN CONTACT FIELDS ===")

def clean_number(num):
    if pd.isna(num):
        return None
    digits = re.sub(r"\D", "", str(num))
    if len(digits) >= 10:
        digits = digits[-10:]
        return "(" + digits[:3] + ") " + digits[3:6] + "-" + digits[6:]
    return None


def clean_emails(*vals):
    emails = []
    for v in vals:
        if pd.isna(v):
            continue
        parts = re.split(r"[;,]", str(v))
        for p in parts:
            e = p.strip().lower()
            if "@" in e:
                emails.append(e)
    seen = []
    for e in emails:
        if e in seen:
            continue
        seen.append(e)
    return ";".join(seen) if len(seen) > 0 else None


df5 = pd.DataFrame()
try:
    df5 = main_up6.copy()
    print("Rows entering cleanup:", len(df5))
    if df5.empty:
        raise RuntimeError("No new open rows")

    if "PrimaryContactPhone" in df5.columns:
        df5["primary_phone_clean"] = df5["PrimaryContactPhone"].apply(clean_number)
    else:
        df5["primary_phone_clean"] = None

    if "SecondaryContactPhone" in df5.columns:
        df5["secondary_phone_clean"] = df5["SecondaryContactPhone"].apply(clean_number)
    else:
        df5["secondary_phone_clean"] = None

    def join_phones(r):
        vals = []
        a = r.get("primary_phone_clean")
        b = r.get("secondary_phone_clean")
        if pd.isna(a) == False:
            vals.append(a)
        if pd.isna(b) == False:
            vals.append(b)
        s = ", ".join(vals)
        if s == "":
            return None
        if s == "(000) 000-0000":
            return None
        return s

    df5["phone_numbers"] = df5.apply(join_phones, axis=1)
    df5 = df5.drop(columns=["primary_phone_clean", "secondary_phone_clean"], errors="ignore")

    p_email = "PrimaryContactEmail" if "PrimaryContactEmail" in df5.columns else None
    s_email = "SecondaryContactEmail" if "SecondaryContactEmail" in df5.columns else None

    def _row_emails(r):
        vals = []
        if p_email is not None:
            vals.append(r.get(p_email))
        if s_email is not None:
            vals.append(r.get(s_email))
        return clean_emails(*vals)

    df5["contact_emails"] = df5.apply(_row_emails, axis=1)

    if "State" in df5.columns:
        df5["State"] = (
            df5["State"].astype(str).str.strip()
            .replace({"": None, "nan": None, "None": None})
            .fillna("NY")
        )
    else:
        df5["State"] = "NY"

    df5 = df5.drop(
        columns=[
            "PrimaryContactFullname", "PrimaryContactEmail", "PrimaryContactPhone",
            "SecondaryContactFullname", "SecondaryContactEmail", "SecondaryContactPhone",
        ],
        errors="ignore",
    )

    print("Rows after cleanup:", len(df5))

except Exception as e:
    safe_print_exception("Contact cleanup", e)
    df5 = main_up6.copy() if isinstance(main_up6, pd.DataFrame) else pd.DataFrame()

print("=== STEP 6: NAICS + PSC LOOKUPS ===")

df7 = pd.DataFrame()
try:
    df7 = df5.copy()
    if df7.empty:
        raise RuntimeError("No new open rows")

    naics = pd.read_csv(
        "https://raw.githubusercontent.com/bluesammer/tenders_codes/main/6-digit_2022_Codes.xlsx%20-%202022_6-digit_industries.csv"
    )
    naics["NaicsCode"] = pd.to_numeric(naics["2022 NAICS Code"], errors="coerce").astype("Int64")
    naics_first = naics[["NaicsCode", "2022 NAICS Title"]].drop_duplicates(subset=["NaicsCode"], keep="first")
    df7 = df7.merge(naics_first, on="NaicsCode", how="left")

    code_PSC = pd.read_csv(
        "https://raw.githubusercontent.com/bluesammer/tenders_codes/main/PSC%20April%202025.xlsx%20-%20PSC%20for%20042025.csv"
    )
    code_PSC_first = code_PSC.drop_duplicates(subset=["PSC CODE"], keep="first")
    df7 = df7.merge(
        code_PSC_first[["PSC CODE", "PRODUCT AND SERVICE CODE NAME"]],
        left_on="ClassificationCode",
        right_on="PSC CODE",
        how="left",
    )

    print("Rows after lookups:", len(df7))

except Exception as e:
    safe_print_exception("NAICS+PSC merge", e)
    df7 = df5.copy() if isinstance(df5, pd.DataFrame) else pd.DataFrame()

print("=== STEP 7: DEFAULTS ===")
if (isinstance(df7, pd.DataFrame) is True) and (df7.empty is False):
    df7["status"] = "open"
    df7["last_update"] = date.today().isoformat()

print("=== STEP 8: GPT SUMMARY + STEP 9: CATEGORY ===")

TITLE_COL = "Title"
DESC_COL = "Description"
OUT_SUM = "summary_1_sentence"
OUT_CAT = "category_alarm_8_raw"
COL_NAICS = "2022 NAICS Title"
COL_PSC = "PRODUCT AND SERVICE CODE NAME"

if (isinstance(df7, pd.DataFrame) is True) and (df7.empty is False):
    if OUT_SUM not in df7.columns:
        df7[OUT_SUM] = ""
    if OUT_CAT not in df7.columns:
        df7[OUT_CAT] = ""

    if DISABLE_GPT is True:
        for i in df7.index.tolist():
            title = clean_text(df7.at[i, TITLE_COL], 500) if TITLE_COL in df7.columns else ""
            desc = clean_text(df7.at[i, DESC_COL], 1600) if DESC_COL in df7.columns else ""
            df7.at[i, OUT_SUM] = fallback_summary(title, desc)
    else:
        eligible_idxs = df7.index.tolist()
        if RANDOM_SAMPLE is True:
            random.seed(RANDOM_SEED)
            random.shuffle(eligible_idxs)
        eligible_idxs = eligible_idxs[:MAX_GPT_ROWS_TOTAL]
        print("Eligible summary rows:", len(eligible_idxs), "| MAX_GPT_ROWS_TOTAL:", MAX_GPT_ROWS_TOTAL, "| GPT_BATCH_SIZE:", GPT_BATCH_SIZE)

        client = OpenAI(api_key=OPENAI_API_KEY)
        batches = chunk_list(eligible_idxs, GPT_BATCH_SIZE)

        processed_rows = 0
        start_t = time.time()

        for b_ix, batch in enumerate(batches, start=1):
            records = []
            for i in batch:
                sol = norm_sol(df7.at[i, "Sol#"]) if "Sol#" in df7.columns else ""
                title = clean_text(df7.at[i, TITLE_COL], 500) if TITLE_COL in df7.columns else ""
                desc = clean_text(df7.at[i, DESC_COL], 1600) if DESC_COL in df7.columns else ""
                naics_v = clean_text(df7.at[i, COL_NAICS], 180) if COL_NAICS in df7.columns else ""
                psc_v = clean_text(df7.at[i, COL_PSC], 180) if COL_PSC in df7.columns else ""
                records.append({"Sol#": sol, "Title": title, "Description": desc, "naics": naics_v, "psc": psc_v})

            attempt = 0
            out_map = {}
            while True:
                attempt += 1
                try:
                    out_map = gpt_summary_batch(client, records)
                    break
                except Exception as e:
                    msg = str(e)
                    is_429 = ("429" in msg) or ("rate_limit" in msg.lower())
                    if is_429 is True:
                        wait_s = detect_retry_after_seconds(e)
                        print("429 rate limit. sleep", f"{wait_s:.1f}", "sec. attempt", attempt, "/", MAX_RETRIES_429)
                        time.sleep(wait_s)
                        if attempt >= MAX_RETRIES_429:
                            break
                    else:
                        safe_print_exception("GPT summary batch", e)
                        break

            for i in batch:
                sol = norm_sol(df7.at[i, "Sol#"]) if "Sol#" in df7.columns else ""
                if sol in out_map:
                    df7.at[i, OUT_SUM] = out_map[sol]
                else:
                    title = clean_text(df7.at[i, TITLE_COL], 500) if TITLE_COL in df7.columns else ""
                    desc = clean_text(df7.at[i, DESC_COL], 1600) if DESC_COL in df7.columns else ""
                    df7.at[i, OUT_SUM] = fallback_summary(title, desc)

            processed_rows += len(batch)
            elapsed = time.time() - start_t
            rps = processed_rows / elapsed if elapsed > 0 else 0.0
            if (b_ix % PRINT_EVERY_BATCH) == 0:
                print("Batch", b_ix, "/", len(batches), "done. rows", processed_rows, "/", len(eligible_idxs), "rate", f"{rps:.2f}", "rows/sec")
            if SLEEP_BETWEEN_BATCH_SEC > 0:
                time.sleep(SLEEP_BETWEEN_BATCH_SEC)

    if "PSC CODE" not in df7.columns:
        df7["PSC CODE"] = df7["ClassificationCode"].astype(str) if "ClassificationCode" in df7.columns else ""
    if "PRODUCT AND SERVICE CODE NAME" not in df7.columns:
        df7["PRODUCT AND SERVICE CODE NAME"] = ""
    if "2022 NAICS Title" not in df7.columns:
        df7["2022 NAICS Title"] = ""

    df7[OUT_CAT] = df7.apply(map_category_8_local, axis=1)

print("=== STEP 10: WRITE OUTPUT FILES ===")

try:
    closed_out_df.to_csv(CLOSED_OUT_PATH, index=False)
    print("Wrote closed file:", str(CLOSED_OUT_PATH), "| rows:", len(closed_out_df))
except Exception as e:
    safe_print_exception("Write closed file", e)

try:
    df7.to_csv(NEW_OPEN_OUT_PATH, index=False)
    print("Wrote new enriched file:", str(NEW_OPEN_OUT_PATH), "| rows:", len(df7))
except Exception as e:
    safe_print_exception("Write new enriched file", e)

print("=== STEP 11: PUSH OUTPUTS (RAILWAY MODE) ===")

def reduce_fresh_payload(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    keep_cols = [
        "Sol#", "PostedDate", "ResponseDeadLine",
        "NaicsCode", "2022 NAICS Title",
        "ClassificationCode", "PSC CODE", "PRODUCT AND SERVICE CODE NAME",
        "summary_1_sentence", "category_alarm_8_raw",
        "contact_emails", "phone_numbers", "State",
        "status", "last_update",
    ]
    present = [c for c in keep_cols if c in df.columns]
    slim = df[present].copy()

    for c in ["summary_1_sentence", "2022 NAICS Title", "PRODUCT AND SERVICE CODE NAME", "contact_emails"]:
        if c in slim.columns:
            lim = 260 if c == "summary_1_sentence" else 180
            slim[c] = slim[c].astype(str).apply(lambda x: clean_text(x, lim))
    return slim


if USE_RAILWAY_MODE:
    try:
        print("Upload closed remote:", repr(CLOSED_REMOTE_FILENAME), "rows:", len(closed_out_df))
        post_df_maybe_chunked(
            UPLOAD_RESULTS_ENDPOINT,
            RAILWAY_API_SECRET,
            closed_out_df,
            CLOSED_REMOTE_FILENAME,
            timeout=UPLOAD_TIMEOUT_SEC,
            chunk_rows=0
        )
        print("Pushed closed file.")
    except Exception as e:
        safe_print_exception("Push closed file", e)

    try:
        df_to_push = reduce_fresh_payload(df7)

        if UPLOAD_FRESH_LIMIT_25 and (df_to_push.empty is False):
            print("UPLOAD_FRESH_LIMIT_25 enabled. Uploading first 25 rows.")
            df_to_push = df_to_push.head(25).copy()

        print("Upload fresh remote:", repr(FRESH_REMOTE_FILENAME), "rows:", len(df_to_push))
        post_df_maybe_chunked(
            UPLOAD_RESULTS_ENDPOINT,
            RAILWAY_API_SECRET,
            df_to_push,
            FRESH_REMOTE_FILENAME,
            timeout=UPLOAD_TIMEOUT_SEC,
            chunk_rows=FRESH_UPLOAD_CHUNK_ROWS
        )
        print("Pushed fresh file.")
    except Exception as e:
        safe_print_exception("Push fresh file", e)

print("=== DONE ===")


# In[ ]:





# In[ ]:





# In[ ]:





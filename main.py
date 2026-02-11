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
# coding: utf-8   railway_local_combo_v3.ipynb

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

# Reads Railway env var. Defaults to 10 if missing.
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

# Optional: quick 504 isolation. Set TRUE to upload only 25 rows of fresh.
UPLOAD_FRESH_LIMIT_25 = os.getenv("UPLOAD_FRESH_LIMIT_25", "false").strip().lower() in ("1", "true", "yes")

CATEGORY_LIST = [
    "IT and Cyber",
    "Medical and Healthcare",
    "Energy and Utilities",
    "Professional Services",
    "Construction and Trades",
    "Transportation and Logistics",
    "Manufacturing",
    "Other",
]

# Lovable endpoints
OPEN_LIST_ENDPOINT = "https://okynhjreumnekmeudwje.supabase.co/functions/v1/get-railway-csv"
UPLOAD_RESULTS_ENDPOINT = "https://okynhjreumnekmeudwje.supabase.co/functions/v1/upload-railway-results"

# Multipart filenames Lovable expects
CLOSED_REMOTE_FILENAME = "tender_was_open_now_close_live.csv"
FRESH_REMOTE_FILENAME = "tender_append_fresh_data.csv"

# Local open list export (local mode only)
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

# Local disk filenames, no timestamps
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
    print(f"[ERROR] {tag}: {type(e).__name__}: {e}")


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
        raise RuntimeError(f"GET failed {resp.status_code}: {resp.text[:500]}")
    if resp.text.strip() == "":
        return pd.DataFrame()
    return read_csv_safely_text(resp.text)


def http_post_csv_multipart(url: str, secret: str, df: pd.DataFrame, filename: str, timeout: int = 600) -> None:
    allowed = {
        "tenders_open_current.csv",
        "tender_was_open_now_close_live.csv",
        "tender_append_fresh_data.csv",
    }
    if filename not in allowed:
        raise RuntimeError(f"Invalid upload filename: {filename}")

    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # IMPORTANT FIX:
    # Supabase function expects the multipart FIELD NAME to match the filename.
    # Example: files["tender_was_open_now_close_live.csv"] = (...)
    files = {filename: (filename, csv_bytes, "text/csv")}

    for attempt in [1, 2]:
        try:
            resp = requests.post(
                url,
                headers={"x-railway-secret": secret},
                files=files,
                timeout=timeout,
            )
            if resp.status_code in (200, 201, 204):
                return
            raise RuntimeError(f"POST failed {resp.status_code}: {resp.text[:500]}")
        except requests.exceptions.Timeout:
            if attempt == 2:
                raise RuntimeError("POST timed out twice")
            time.sleep(3)


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
            raise RuntimeError(f"Chrome runtime missing: {CHROME_BIN} or {CHROMEDRIVER_PATH}")

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

    raise RuntimeError(f"Timeout waiting for stable file: {path}")


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
        return validate_summary(f"Provide {t.lower()}.")
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
# CATEGORY (8) RULE ENGINE
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


def is_it(text):
    if KW_IT_STRONG.search(text):
        return True
    if KW_IT_MED.search(text):
        return True
    if KW_IT_WEAK.search(text) and KW_IT_MED.search(text):
        return True
    return False


def is_cons(text):
    return bool(KW_CONS1.search(text) or KW_CONS2.search(text))


def is_pro(text):
    return bool(
        KW_PRO1.search(text) or
        KW_PRO2.search(text) or
        KW_PRO3.search(text) or
        KW_PRO4.search(text) or
        KW_PRO5.search(text)
    )


def map_category_8_local(row):
    p = psc_prefix(row.get("PSC CODE", ""))
    t = text_blob_raw(row)

    if is_it(t):
        return "IT and Cyber"
    if KW_MED.search(t):
        return "Medical and Healthcare"
    if KW_ENERGY.search(t):
        return "Energy and Utilities"
    if is_pro(t):
        return "Professional Services"
    if is_cons(t):
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
# GPT SUMMARY ONLY
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
# STEP 0: LOAD PREVIOUS OPEN LIST
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
    raise RuntimeError("Open list load failed in Railway mode. Aborting to prevent massive append.")

prev_open_set = set(prev_open_df["Sol#"].dropna().astype(str))
if "" in prev_open_set:
    prev_open_set.remove("")


# =========================================================
# STEP 1: SAM.GOV DOWNLOAD
# =========================================================

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
            else:
                print("Terms modal skipped or already accepted.")

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
    else:
        print("DOWNLOAD_SAM=False. Using existing file.")

print("SAM_CSV_PATH:", str(SAM_CSV_PATH), "| exists:", SAM_CSV_PATH.exists())
if SAM_CSV_PATH.exists():
    print("SAM size:", bytes_human(SAM_CSV_PATH.stat().st_size))


# =========================================================
# STEP 2: LOAD SAM (FULL) + BUILD RECENT (6 MONTHS)
# =========================================================

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


# =========================================================
# STEP 3: CLOSED FROM PREV OPEN (USE FULL SAM)
# =========================================================

print("=== STEP 3: CLOSED FROM PREV OPEN (FULL SAM) ===")

closed_out_df = pd.DataFrame(columns=["Sol#", "close_reason", "AwardDate"])

try:
    if df_full.empty:
        raise RuntimeError("SAM full df empty")

    if "AwardDate" in df_full.columns:
        open_today_set = set(df_full.loc[df_full["AwardDate"].isna(), "Sol#"].dropna().astype(str).unique())
    else:
        open_today_set = set(df_full["Sol#"].dropna().astype(str).unique())

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
            rows.append({
                "Sol#": sol,
                "close_reason": "awarded",
                "AwardDate": award_map.get(sol, "")
            })

        closed_out_df = pd.DataFrame(rows)

    print("Prev open:", len(prev_open_set), "| Open today:", len(open_today_set), "| Closed:", len(closed_out_df))

except Exception as e:
    safe_print_exception("Closed detection", e)
    closed_out_df = pd.DataFrame(columns=["Sol#", "close_reason", "AwardDate"])


# =========================================================
# STEP 4: NEW OPEN TO ENRICH (USE RECENT ONLY)
# =========================================================

print("=== STEP 4: NEW OPEN TO ENRICH (RECENT ONLY) ===")

DEBUG_STEP4 = True
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

    if DEBUG_STEP4:
        print("Step4 df_recent rows:", len(df_recent), "cols:", len(df_recent.columns))
        print("Step4 df_open_recent rows:", len(df_open_recent), "cols:", len(df_open_recent.columns))
        print("Step4 prev_open_set size:", len(prev_open_set))
        print("Step4 df_open_recent Sol# sample:", df_open_recent["Sol#"].head(5).tolist())
        print("Step4 prev_open sample:", list(sorted(list(prev_open_set)))[:5])

    is_prev = df_open_recent["Sol#"].isin(prev_open_set)
    is_new = ~is_prev

    if DEBUG_STEP4:
        print("Step4 counts prev:", int(is_prev.sum()), "new:", int(is_new.sum()))

    new_open_df = df_open_recent.loc[is_new].copy()
    print("New open rows (recent):", len(new_open_df))

    if ("PostedDate" in new_open_df.columns) and (len(new_open_df) > 0):
        new_open_df["PostedDate"] = pd.to_datetime(new_open_df["PostedDate"], errors="coerce", utc=True)
        new_open_df = new_open_df.sort_values("PostedDate", ascending=False)

    before_dups = len(new_open_df)
    new_open_df = new_open_df.drop_duplicates(subset=["Sol#"], keep="first").copy()
    print("New open unique Sol# (recent):", len(new_open_df), "dropped:", before_dups - len(new_open_df))

    front_cols = ["Sol#", "PostedDate", "ResponseDeadLine"]
    front = [c for c in front_cols if c in new_open_df.columns]
    rest = [c for c in new_open_df.columns if c not in front]
    main_up6 = new_open_df.loc[:, front + rest].copy()

    if "PostedDate" in main_up6.columns:
        main_up6["PostedDate"] = pd.to_datetime(main_up6["PostedDate"], errors="coerce", utc=True).astype(str).str[:10]
    if "ResponseDeadLine" in main_up6.columns:
        main_up6["ResponseDeadLine"] = main_up6["ResponseDeadLine"].astype(str).str[:10]

    if DEBUG_STEP4:
        print("Step4 main_up6 rows:", len(main_up6))

except Exception as e:
    safe_print_exception("New-open detection", e)
    main_up6 = pd.DataFrame()


# =========================================================
# STEP 5: CLEAN CONTACT FIELDS
# =========================================================

print("=== STEP 5: CLEAN CONTACT FIELDS ===")

def clean_number(num):
    if pd.isna(num):
        return None
    digits = re.sub(r"\D", "", str(num))
    if len(digits) >= 10:
        digits = digits[-10:]
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
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
        if pd.notna(a):
            vals.append(a)
        if pd.notna(b):
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


# =========================================================
# STEP 6: NAICS + PSC LOOKUPS
# =========================================================

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


# =========================================================
# STEP 7: DEFAULTS
# =========================================================

print("=== STEP 7: DEFAULTS ===")
if (isinstance(df7, pd.DataFrame) is True) and (df7.empty is False):
    df7["status"] = "open"
    df7["last_update"] = date.today().isoformat()
else:
    print("No new open rows, defaults skipped")


# =========================================================
# STEP 8: GPT SUMMARY ONLY + STEP 9: LOCAL CATEGORY (8)
# =========================================================

print("=== STEP 8: GPT SUMMARY ONLY + STEP 9: LOCAL CATEGORY (8) ===")

TITLE_COL = "Title"
DESC_COL = "Description"
OUT_SUM = "summary_1_sentence"
OUT_CAT = "category_alarm_8_raw"
COL_NAICS = "2022 NAICS Title"
COL_PSC = "PRODUCT AND SERVICE CODE NAME"


def row_has_summary(df_: pd.DataFrame, i: int) -> bool:
    if OUT_SUM in df_.columns:
        s = str(df_.at[i, OUT_SUM]).strip()
        if s == "":
            return False
        if s.startswith("ERROR:"):
            return False
        return True
    return False


gpt_stop_flag = False

if (isinstance(df7, pd.DataFrame) is True) and (df7.empty is False):

    if OUT_SUM not in df7.columns:
        df7[OUT_SUM] = ""
    if OUT_CAT not in df7.columns:
        df7[OUT_CAT] = ""

    if DISABLE_GPT is True:
        print("GPT disabled. Using fallback summaries.")
        for i in df7.index.tolist():
            if row_has_summary(df7, i):
                continue
            title = clean_text(df7.at[i, TITLE_COL], 500) if TITLE_COL in df7.columns else ""
            desc = clean_text(df7.at[i, DESC_COL], 1600) if DESC_COL in df7.columns else ""
            df7.at[i, OUT_SUM] = fallback_summary(title, desc)
    else:
        eligible_idxs = []
        for i in df7.index.tolist():
            if SKIP_IF_ALREADY_ENRICHED and row_has_summary(df7, i):
                continue
            eligible_idxs.append(i)

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
            if gpt_stop_flag is True:
                break

            records = []
            for i in batch:
                sol = norm_sol(df7.at[i, "Sol#"]) if "Sol#" in df7.columns else ""
                title = clean_text(df7.at[i, TITLE_COL], 500) if TITLE_COL in df7.columns else ""
                desc = clean_text(df7.at[i, DESC_COL], 1600) if DESC_COL in df7.columns else ""
                naics_v = clean_text(df7.at[i, COL_NAICS], 180) if COL_NAICS in df7.columns else ""
                psc_v = clean_text(df7.at[i, COL_PSC], 180) if COL_PSC in df7.columns else ""

                records.append({
                    "Sol#": sol,
                    "Title": title,
                    "Description": desc,
                    "naics": naics_v,
                    "psc": psc_v,
                })

            attempt = 0
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
                        print(f"[WARN] 429 rate limit. sleep {wait_s:.1f}s. attempt {attempt}/{MAX_RETRIES_429}")
                        time.sleep(wait_s)
                        if attempt >= MAX_RETRIES_429:
                            print("[WARN] 429 persisted. stopping GPT for this run.")
                            if STOP_GPT_ON_RPD_LIMIT is True:
                                gpt_stop_flag = True
                                out_map = {}
                                break
                    else:
                        safe_print_exception("GPT summary batch", e)
                        out_map = {}
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
                print(f"Batch {b_ix}/{len(batches)} done. rows {processed_rows}/{len(eligible_idxs)} rate {rps:.2f} rows/sec")

            if SLEEP_BETWEEN_BATCH_SEC > 0:
                time.sleep(SLEEP_BETWEEN_BATCH_SEC)

        print("GPT summary finished. rows processed:", processed_rows, "gpt_stop_flag:", gpt_stop_flag)

    if "PSC CODE" not in df7.columns:
        if "ClassificationCode" in df7.columns:
            df7["PSC CODE"] = df7["ClassificationCode"].astype(str)
        else:
            df7["PSC CODE"] = ""

    if "PRODUCT AND SERVICE CODE NAME" not in df7.columns:
        df7["PRODUCT AND SERVICE CODE NAME"] = ""

    if "2022 NAICS Title" not in df7.columns:
        df7["2022 NAICS Title"] = ""

    df7[OUT_CAT] = df7.apply(map_category_8_local, axis=1)

else:
    print("No new open rows, summary skipped, category skipped")


# =========================================================
# STEP 10: WRITE OUTPUT FILES
# =========================================================

print("=== STEP 10: WRITE OUTPUT FILES ===")

try:
    closed_out_df.to_csv(CLOSED_OUT_PATH, index=False)
    print("Wrote closed file:", str(CLOSED_OUT_PATH), "| rows:", len(closed_out_df))
except Exception as e:
    safe_print_exception("Write closed file", e)

try:
    if (isinstance(df7, pd.DataFrame) is True) and (df7.empty is False):
        df7.to_csv(NEW_OPEN_OUT_PATH, index=False)
        print("Wrote new enriched file:", str(NEW_OPEN_OUT_PATH), "| rows:", len(df7))
    else:
        pd.DataFrame().to_csv(NEW_OPEN_OUT_PATH, index=False)
        print("Wrote empty new enriched file:", str(NEW_OPEN_OUT_PATH), "| rows: 0")
except Exception as e:
    safe_print_exception("Write new enriched file", e)


# =========================================================
# STEP 11: PUSH OUTPUTS (RAILWAY MODE)
# =========================================================

print("=== STEP 11: PUSH OUTPUTS (RAILWAY MODE) ===")

if USE_RAILWAY_MODE:
    try:
        http_post_csv_multipart(
            UPLOAD_RESULTS_ENDPOINT,
            RAILWAY_API_SECRET,
            closed_out_df,
            CLOSED_REMOTE_FILENAME,
            timeout=600
        )
        print("Pushed closed file.")
    except Exception as e:
        safe_print_exception("Push closed file", e)

    try:
        df_to_push = df7 if isinstance(df7, pd.DataFrame) else pd.DataFrame()
        if UPLOAD_FRESH_LIMIT_25 and isinstance(df_to_push, pd.DataFrame) and (df_to_push.empty is False):
            print("UPLOAD_FRESH_LIMIT_25 enabled. Uploading only first 25 rows for testing.")
            df_to_push = df_to_push.head(25).copy()

        http_post_csv_multipart(
            UPLOAD_RESULTS_ENDPOINT,
            RAILWAY_API_SECRET,
            df_to_push,
            FRESH_REMOTE_FILENAME,
            timeout=600
        )
        print("Pushed fresh file.")
    except Exception as e:
        safe_print_exception("Push fresh file", e)
else:
    print("Local mode. Skipped pushes.")

print("=== DONE ===")


# In[ ]:





# In[ ]:





# In[ ]:





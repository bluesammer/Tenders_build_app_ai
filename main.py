#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[ ]:





# In[ ]:


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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =====================
# ENV
# =====================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
RAILWAY_API_SECRET = os.getenv("RAILWAY_API_SECRET", "").strip()

if OPENAI_API_KEY == "":
    raise RuntimeError("OPENAI_API_KEY missing")
if RAILWAY_API_SECRET == "":
    raise RuntimeError("RAILWAY_API_SECRET missing")

MASTER_ENDPOINT = "https://okynhjreumnekmeudwje.supabase.co/functions/v1/get-railway-csv"


# =====================
# PATHS
# =====================
DOWNLOAD_DIR = Path("downloads").resolve()
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

SAM_CSV_NAME = "ContractOpportunitiesFullCSV.csv"
SAM_CSV_PATH = DOWNLOAD_DIR / SAM_CSV_NAME

MASTER_FEED_NAME = "TenderBid_GPT_Enriched_chtgpt4.csv"
MASTER_LOCAL_PATH = DOWNLOAD_DIR / MASTER_FEED_NAME


# =====================
# HELPERS
# =====================
def read_csv_safely(p: Path) -> pd.DataFrame:
    encodings = ["utf-8", "cp1252", "latin1"]
    for enc in encodings:
        try:
            return pd.read_csv(p, encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            pass
    return pd.read_csv(p, encoding="utf-8", errors="replace", low_memory=False)

def wait_for_download(download_dir: Path, timeout_sec: int = 180) -> Path:
    start = time.time()
    while time.time() - start < timeout_sec:
        csvs = list(download_dir.glob("*.csv"))
        partials = list(download_dir.glob("*.crdownload")) + list(download_dir.glob("*.tmp"))

        if len(csvs) > 0 and len(partials) == 0:
            newest = max(csvs, key=lambda x: x.stat().st_mtime)
            if newest.stat().st_size > 0:
                return newest

        time.sleep(1)

    raise RuntimeError("Download timeout. No finished CSV found.")

def clean_text(x, limit: int) -> str:
    if pd.isna(x):
        return ""
    s = str(x)
    s = s.replace("\x00", " ")
    s = re.sub(r"[\x01-\x08\x0B\x0C\x0E-\x1F]", " ", s)
    s = s.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:limit]

def master_get_df() -> pd.DataFrame:
    resp = requests.get(
        MASTER_ENDPOINT,
        headers={"x-railway-secret": RAILWAY_API_SECRET},
        timeout=90,
    )

    if resp.status_code == 200 and resp.text.strip():
        return pd.read_csv(StringIO(resp.text), low_memory=False)

    return pd.DataFrame()

def master_put_df(df: pd.DataFrame) -> None:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    resp = requests.post(
        MASTER_ENDPOINT,
        headers={
            "x-railway-secret": RAILWAY_API_SECRET,
            "content-type": "text/csv",
        },
        data=csv_bytes,
        timeout=180,
    )
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"Master upload failed: {resp.status_code} {resp.text}")


# =====================
# STEP A: LOAD MASTER VIA SECURE ENDPOINT
# =====================
print("=== MASTER FEED LOAD (SECURE ENDPOINT) ===")
main_up = master_get_df()
if main_up.empty:
    print("Master missing or empty. Starting empty.")
else:
    print("Master loaded. Rows:", len(main_up))


# =====================
# STEP 1: SAM.GOV DOWNLOAD (KEEP THESE STEPS)
# =====================
print("=== SAM.gov Contract Opportunities Downloader ===")

for f in DOWNLOAD_DIR.glob("*.csv"):
    if f.name != MASTER_FEED_NAME:
        try:
            f.unlink()
        except Exception:
            pass

options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": str(DOWNLOAD_DIR),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
}
options.add_experimental_option("prefs", prefs)
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")

print("=== SAM.gov Contract Opportunities Downloader ===")
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 90)

try:
    print("STEP 1: Browser launched. Navigating to SAM.gov page...")
    driver.get("https://sam.gov/data-services/Contract%20Opportunities/datagov?privacy=Public")

    wait.until(lambda d: "SAM.gov" in d.title)
    print("STEP 2: Page title loaded:", driver.title)

    print("STEP 3: Waiting for dataset list...")
    wait.until(lambda d: "ContractOpportunitiesFullCSV" in d.page_source)
    print("STEP 3a: Dataset list detected")

    csv_elem = driver.find_element(By.XPATH, "//*[contains(text(),'ContractOpportunitiesFullCSV.csv')]")
    driver.execute_script("arguments[0].scrollIntoView(true);", csv_elem)
    time.sleep(1)

    print("STEP 4: Clicking CSV link...")
    driver.execute_script(
        """
        var el = arguments[0];
        var evt = new MouseEvent('click', {bubbles:true, cancelable:true, view:window});
        el.dispatchEvent(evt);
        """,
        csv_elem
    )

    print("STEP 5: Waiting for popup modal...")
    modal = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".sds-dialog-content.height-mobile"))
    )

    for _ in range(7):
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", modal)
        time.sleep(1)
        try:
            accept_button = driver.find_element(By.XPATH, "//button[@class='usa-button' and normalize-space()='Accept']")
            if accept_button.is_enabled():
                print("STEP 6: Accept button detected and enabled. Clicking...")
                accept_button.click()
                print("✅ STEP 7: Terms accepted successfully.")
                break
        except Exception:
            pass

    time.sleep(2)

    print("STEP 8: Clicking CSV link again to start actual download...")
    csv_elem2 = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//*[contains(text(),'ContractOpportunitiesFullCSV.csv')]"))
    )
    csv_elem2.click()

    print("STEP 9: Waiting for file to download...")
    downloaded = wait_for_download(DOWNLOAD_DIR, timeout_sec=240)
    print("✅ Download complete:", downloaded.name)

finally:
    driver.quit()
    print("=== DONE ===")


# =====================
# STEP 2: LOAD SAM CSV AND FILTER
# =====================
df = read_csv_safely(SAM_CSV_PATH)

df2 = df.copy()
df2["PostedDate"] = pd.to_datetime(df2["PostedDate"], errors="coerce", utc=True)
cutoff = pd.Timestamp.now(tz="UTC") - pd.DateOffset(months=6)
df3 = df2.loc[df2["PostedDate"].notna() & (df2["PostedDate"] >= cutoff)].copy()

cols_to_drop = [
    "ArchiveType",
    "OrganizationType",
    "NoticeId",
    "AdditionalInfoLink",
    "Award$",
    "Awardee",
    "AwardNumber",
    "PopCity",
    "PopCountry",
    "PopState",
    "PopStreetAddress",
    "PopZip",
    "PrimaryContactFax",
    "PrimaryContactTitle",
    "SecondaryContactFax",
    "SecondaryContactTitle",
    "SetASide",
    "SetASideCode",
    "CGAC", "FPDS Code", "AAC Code", "ZipCode", "CountryCode", "Type",
    "BaseType", "Office", "Department/Ind.Agency", "ArchiveDate"
]
df3 = df3.drop(columns=cols_to_drop, errors="ignore")


# =====================
# STEP 3: CLOSE AWARDED SOL#
# =====================
awarded_df = df3.loc[df3["AwardDate"].notna(), ["Sol#", "AwardDate"]].copy()

if (not main_up.empty) and ("Sol#" in main_up.columns):
    main_up["Sol#"] = main_up["Sol#"].astype(str)
    awarded_df["Sol#"] = awarded_df["Sol#"].astype(str)

    if "status" not in main_up.columns:
        main_up["status"] = "open"

    main_up.loc[main_up["Sol#"].isin(awarded_df["Sol#"]), "status"] = "closed"


# =====================
# STEP 4: FIND NEW OPEN BIDS NOT IN MASTER
# =====================
df3_open = df3.loc[df3["AwardDate"].isna()].copy()
df3_open["Sol#"] = df3_open["Sol#"].astype(str)

open_sol = set()
if (not main_up.empty) and ("Sol#" in main_up.columns) and ("status" in main_up.columns):
    open_sol = set(
        main_up.loc[main_up["status"].eq("open"), "Sol#"]
        .dropna()
        .astype(str)
        .unique()
    )

main_up3 = df3_open.loc[~df3_open["Sol#"].isin(open_sol)].copy()

front_cols = ["Sol#", "PostedDate", "ResponseDeadLine"]
front = [c for c in front_cols if c in main_up3.columns]
rest = [c for c in main_up3.columns if c not in front]
main_up4 = main_up3.loc[:, front + rest].copy()

main_up4["PostedDate"] = pd.to_datetime(main_up4["PostedDate"], errors="coerce", utc=True)
main_up4 = main_up4.sort_values("PostedDate", ascending=False)
main_up6 = main_up4.drop_duplicates(subset=["Sol#"], keep="first").copy()

main_up6["PostedDate"] = main_up6["PostedDate"].astype(str).str[:10]
if "ResponseDeadLine" in main_up6.columns:
    main_up6["ResponseDeadLine"] = main_up6["ResponseDeadLine"].astype(str).str[:10]


# =====================
# STEP 5: CLEAN PHONES, EMAILS
# =====================
def clean_number(num):
    if pd.isna(num):
        return None
    digits = re.sub(r"\D", "", str(num))
    if len(digits) >= 10:
        digits = digits[-10:]
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return None

df5 = main_up6.copy()

if "PrimaryContactPhone" in df5.columns:
    df5["primary_phone_clean"] = df5["PrimaryContactPhone"].apply(clean_number)
else:
    df5["primary_phone_clean"] = None

if "SecondaryContactPhone" in df5.columns:
    df5["secondary_phone_clean"] = df5["SecondaryContactPhone"].apply(clean_number)
else:
    df5["secondary_phone_clean"] = None

df5["phone_numbers"] = (
    df5[["primary_phone_clean", "secondary_phone_clean"]]
    .apply(lambda x: ", ".join([v for v in x if pd.notna(v)]), axis=1)
    .replace("", None)
)
df5["phone_numbers"] = df5["phone_numbers"].apply(lambda x: None if x == "(000) 000-0000" else x)
df5 = df5.drop(columns=["primary_phone_clean", "secondary_phone_clean"], errors="ignore")

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

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
    return ";".join(seen) if seen else None

p_email = "PrimaryContactEmail" if "PrimaryContactEmail" in df5.columns else None
s_email = "SecondaryContactEmail" if "SecondaryContactEmail" in df5.columns else None

def _row_emails(r):
    vals = []
    if p_email:
        vals.append(r.get(p_email))
    if s_email:
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
        "PrimaryContactFullname",
        "PrimaryContactEmail",
        "PrimaryContactPhone",
        "SecondaryContactFullname",
        "SecondaryContactEmail",
        "SecondaryContactPhone",
    ],
    errors="ignore",
)


# =====================
# STEP 6: ENRICH NAICS + PSC FROM YOUR GITHUB FILES
# =====================
naics = pd.read_csv(
    "https://raw.githubusercontent.com/bluesammer/tenders_codes/main/6-digit_2022_Codes.xlsx%20-%202022_6-digit_industries.csv"
)
naics["NaicsCode"] = pd.to_numeric(naics["2022 NAICS Code"], errors="coerce").astype("Int64")
naics_first = naics[["NaicsCode", "2022 NAICS Title"]].drop_duplicates(subset=["NaicsCode"], keep="first")

df6 = df5.merge(naics_first, on="NaicsCode", how="left")

code_PSC = pd.read_csv(
    "https://raw.githubusercontent.com/bluesammer/tenders_codes/main/PSC%20April%202025.xlsx%20-%20PSC%20for%20042025.csv"
)
code_PSC_first = code_PSC.drop_duplicates(subset=["PSC CODE"], keep="first")

df7 = df6.merge(
    code_PSC_first[["PSC CODE", "PRODUCT AND SERVICE CODE NAME"]],
    left_on="ClassificationCode",
    right_on="PSC CODE",
    how="left",
)


# =====================
# STEP 7: DEFAULTS
# =====================
df7["status"] = "open"
df7["last_update"] = date.today().isoformat()


# =====================
# STEP 8: GPT SUMMARY ENRICH
# =====================
TITLE_COL = "Title"
DESC_COL = "Description"
MODEL = "gpt-4o-mini"

RUN_FULL = True
TEST_ROWS = 5
RANDOM_SAMPLE = False
RANDOM_SEED = 42

SKIP_IF_ALREADY_ENRICHED = True

def row_already_enriched(df_: pd.DataFrame, i: int) -> bool:
    s = str(df_.at[i, "summary_1_sentence"]).strip()
    if s == "":
        return False
    if s.startswith("ERROR:"):
        return False
    return True

def validate_summary(summary: str) -> str:
    s = re.sub(r"\s+", " ", str(summary)).strip()
    if len(s) > 260:
        s = s[:260].rstrip()
    if "." in s:
        first = s.split(".", 1)[0].strip()
        if first:
            s = first + "."
    return s

def enrich_one(client: OpenAI, title: str, desc: str) -> dict:
    prompt = (
        "Return one JSON object only.\n"
        "Keys must be exactly: summary_1_sentence.\n\n"
        "summary_1_sentence rules:\n"
        "One sentence.\n"
        "Start with Provide or Deliver or Install or Maintain or Supply.\n"
        "Describe what is being purchased.\n"
        "Avoid procurement language.\n"
        "Do not mention RFQ, solicitation, BPA, BAA, FAR, DFARS, amendments, deadlines, NAICS, set-aside.\n\n"
        f"Title: {title}\n"
        f"Description: {desc}\n"
    )
    resp = client.responses.create(
        model=MODEL,
        input=prompt,
        text={"format": {"type": "json_object"}},
    )
    return json.loads(resp.output_text)

if "summary_1_sentence" not in df7.columns:
    df7["summary_1_sentence"] = ""

client = OpenAI(api_key=OPENAI_API_KEY)

idxs = list(df7.index) if RUN_FULL else df7.head(TEST_ROWS).index.tolist()
if RANDOM_SAMPLE and (not RUN_FULL):
    random.seed(RANDOM_SEED)
    idxs = random.sample(list(df7.index), min(TEST_ROWS, len(df7.index)))

processed = 0
skipped = 0

for i in idxs:
    if SKIP_IF_ALREADY_ENRICHED and row_already_enriched(df7, i):
        skipped += 1
        continue

    title = clean_text(df7.at[i, TITLE_COL], 800) if TITLE_COL in df7.columns else ""
    desc = clean_text(df7.at[i, DESC_COL], 4000) if DESC_COL in df7.columns else ""

    try:
        raw = enrich_one(client, title, desc)
        df7.at[i, "summary_1_sentence"] = validate_summary(raw.get("summary_1_sentence", ""))
        processed += 1
    except Exception as e:
        df7.at[i, "summary_1_sentence"] = f"ERROR: {type(e).__name__}: {e}"
        processed += 1

print("Summary enrich done. processed:", processed, "skipped:", skipped)


# =====================
# STEP 9: GPT CATEGORY_20 ENFORCED BY MASTER LIST
# =====================
OUT_COL = "category_20"
COL_SUMMARY = "summary_1_sentence"
COL_NAICS = "2022 NAICS Title"
COL_PSC = "PRODUCT AND SERVICE CODE NAME"

def build_categories(master_df: pd.DataFrame):
    if master_df.empty or "category_20" not in master_df.columns:
        return []
    cats = (
        master_df["category_20"]
        .dropna()
        .astype(str)
        .map(lambda x: x.strip())
    )
    cats = sorted(set([c for c in cats if c]))
    return cats

def row_needs_gpt(df_: pd.DataFrame, i: int, categories):
    v = df_.at[i, OUT_COL] if OUT_COL in df_.columns else None
    if v is None or pd.isna(v) or str(v).strip() == "":
        return True
    if str(v).strip() not in categories:
        return True
    return False

def validate_category(choice, categories):
    c = str(choice).strip()
    if c in categories:
        return c
    lower_map = {x.lower(): x for x in categories}
    if c.lower() in lower_map:
        return lower_map[c.lower()]
    return None

def classify_one(client_: OpenAI, categories, summary, naics, psc):
    prompt = (
        "Return one JSON object.\n"
        "Key must be category_20.\n"
        "Pick EXACTLY one value from this list:\n"
        f"{categories}\n\n"
        f"summary: {summary}\n"
        f"naics: {naics}\n"
        f"psc: {psc}"
    )
    resp = client_.responses.create(
        model=MODEL,
        input=prompt,
        text={"format": {"type": "json_object"}}
    )
    return json.loads(resp.output_text)

def classify_one_retry(client_: OpenAI, categories, summary, naics, psc):
    prompt = (
        "You MUST choose exactly one category from this list.\n"
        "Return JSON only.\n"
        f"{categories}\n\n"
        f"summary: {summary}\n"
        f"naics: {naics}\n"
        f"psc: {psc}"
    )
    resp = client_.responses.create(
        model=MODEL,
        input=prompt,
        text={"format": {"type": "json_object"}}
    )
    return json.loads(resp.output_text)

categories = build_categories(main_up)
if len(categories) == 0:
    print("category_20 list missing in master. Skipping category_20 GPT step.")
else:
    if OUT_COL not in df7.columns:
        df7[OUT_COL] = ""

    client.responses.create(model=MODEL, input="ping")

    processed = 0
    errors = 0
    start = time.time()

    for i in df7.index:
        if not row_needs_gpt(df7, i, categories):
            continue

        summary = clean_text(df7.at[i, COL_SUMMARY], 900)
        naics_v = clean_text(df7.at[i, COL_NAICS], 240) if COL_NAICS in df7.columns else ""
        psc_v = clean_text(df7.at[i, COL_PSC], 240) if COL_PSC in df7.columns else ""

        try:
            raw = classify_one(client, categories, summary, naics_v, psc_v)
            choice = raw.get("category_20", "")
            final = validate_category(choice, categories)

            if final is None:
                raw = classify_one_retry(client, categories, summary, naics_v, psc_v)
                choice = raw.get("category_20", "")
                final = validate_category(choice, categories)

            if final is None:
                df7.at[i, OUT_COL] = "ERROR"
                errors += 1
            else:
                df7.at[i, OUT_COL] = final

            processed += 1

        except Exception as e:
            df7.at[i, OUT_COL] = f"ERROR: {e}"
            processed += 1
            errors += 1

        if processed % 25 == 0:
            elapsed = time.time() - start
            rate = processed / elapsed if elapsed > 0 else 0
            print(f"Processed {processed} | Errors {errors} | Rate {rate:.2f}/sec")

    print("category_20 done. processed:", processed, "errors:", errors)


# =====================
# STEP 10: ALIGN COLUMNS, APPEND
# =====================
if main_up.empty:
    main_up = df7.copy()
else:
    missing_in_df7 = set(main_up.columns) - set(df7.columns)
    extra_in_df7 = set(df7.columns) - set(main_up.columns)

    for col in missing_in_df7:
        df7[col] = pd.NA

    df7 = df7.drop(columns=list(extra_in_df7), errors="ignore")
    df7 = df7[main_up.columns]

    try:
        df7 = df7.astype(main_up.dtypes.to_dict(), errors="ignore")
    except Exception as e:
        print("Type alignment warning:", e)

    rows_before = len(main_up)
    main_up = pd.concat([main_up, df7], ignore_index=True)
    rows_after = len(main_up)

    print("Rows appended:", rows_after - rows_before)
    print("New total rows:", rows_after)


# =====================
# STEP 11: SAVE LOCAL + PUSH MASTER VIA SECURE ENDPOINT
# =====================
main_up.to_csv(MASTER_LOCAL_PATH, index=False)
print("Saved master locally:", str(MASTER_LOCAL_PATH))

master_put_df(main_up)
print("Master upload done:", MASTER_FEED_NAME)


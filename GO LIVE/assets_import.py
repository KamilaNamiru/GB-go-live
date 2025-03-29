import pandas as pd
import os
import json
import numpy as np
import datetime
import warnings
from simple_salesforce import Salesforce
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=UserWarning)

DEFAULT_ACCOUNT_ID = "001J900000CASu4IAH"
DEFAULT_OUTPUT_DIR = "output"
OBJECT_API_NAME = "Asset"

# 🔐 Připojení do Salesforce
load_dotenv("credentials.env")
sf = Salesforce(
    username=os.getenv("SF_USERNAME"),
    password=os.getenv("SF_PASSWORD"),
    security_token=os.getenv("SF_TOKEN"),
    domain=os.getenv("SF_DOMAIN", "login")
)
print("✅ Připojeno k Salesforce.")

# 🧠 Funkce pro načtení SDL mappingu
def normalize_column_name(name):
    return name.strip().lower().replace(" ", "").replace("/", "").replace(".", "").replace("+", "")

def load_sdl_mapping(path):
    mapping = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                src, tgt = line.split("=", 1)
                mapping[normalize_column_name(src)] = tgt.strip()
    return mapping

# 🧼 Sanitizace hodnot pro Salesforce
def sanitize_record_values(rec):
    for key, value in rec.items():
        if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
            rec[key] = None
        elif isinstance(value, (pd.Timestamp, datetime.datetime)):
            rec[key] = value.strftime("%Y-%m-%d")
    return rec

# 📂 Načti vstupy
assets_file = "assets 28.3.2025 - Terminals.xlsx"
mapping_file = "AssetsMapping.sdl"
accounts_file = "accounts_imported_out.csv"
os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)

# 🧭 Načti mapping a vstupní data
mapping = load_sdl_mapping(mapping_file)
df = pd.read_excel(assets_file)
print("🧾 Sloupce v Excelu:", df.columns.tolist())
df.columns = df.columns.str.strip()


# 🏷️ Přejmenuj sloupce podle SDL
original_columns = df.columns.tolist()
normalized_columns = [normalize_column_name(col) for col in original_columns]
rename_dict = {orig: mapping.get(norm, orig) for orig, norm in zip(original_columns, normalized_columns)}
df.rename(columns=rename_dict, inplace=True)
print("📄 Sloupce po přejmenování:", df.columns.tolist())

# 🧹 PartnerWeb ORG ID
if "PartnerWeb_ORG_ID__c" in df.columns:
    df["PartnerWeb_ORG_ID__c"] = df["PartnerWeb_ORG_ID__c"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["PartnerWeb_ORG_ID__c"] = pd.to_numeric(df["PartnerWeb_ORG_ID__c"], errors="coerce")
else:
    raise KeyError("Sloupec 'PartnerWeb_ORG_ID__c' nebyl nalezen v datech.")

# 🔗 Párování s accouny
accounts_df = pd.read_csv(accounts_file)
df = df.merge(accounts_df[["Id", "PartnerWeb_ORG_ID__c"]], on="PartnerWeb_ORG_ID__c", how="left")
df["AccountId"] = df["Id"].fillna(DEFAULT_ACCOUNT_ID)
df.drop(columns=["Id"], inplace=True, errors="ignore")

# 🆔 Import ID
df = df.reset_index(drop=True)
df["Import_ID__c"] = df.index.map(lambda i: f"ASSET{str(i + 1).zfill(5)}")

# 📛 Název assetu = SerialNumber (nebo fallback na Import_ID__c)
if "SerialNumber" in df.columns:
    df["SerialNumber"] = df["SerialNumber"].astype(str).str.strip()
    df["Name"] = df["SerialNumber"].where(df["SerialNumber"].notna() & (df["SerialNumber"] != ""), df["Import_ID__c"])

# 📅 Převod datových polí
for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
    df[col] = df[col].dt.strftime("%Y-%m-%d")

# 🧽 Finální vyčištění
df = df.replace([np.nan, float("inf"), float("-inf"), "nan", "NaN"], None)
records = [sanitize_record_values(r) for r in df.to_dict(orient="records")]

# 💾 Debug JSON
with open(f"{DEFAULT_OUTPUT_DIR}/assets_debug.json", "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)
print("📃 Debug uložen do assets_debug.json")

# 🚀 Import do Salesforce (bez chunked)
print("🚀 Nahrávám assety do Salesforce...")
response = sf.bulk.__getattr__(OBJECT_API_NAME).upsert(records, external_id_field="Import_ID__c")

# 📊 Výsledky
success_count = sum(1 for r in response if r.get("success"))
failures = [r for r in response if not r.get("success")]
print(f"✅ Úspěšně nahráno: {success_count}")
print(f"❌ Selhalo: {len(failures)}")

# 🧾 Výpis chyb
error_rows = []
for i, fail in enumerate(failures):
    original_record = df.iloc[i].copy()
    error_info = fail.get("errors", [{}])[0]
    original_record["Chyba_kód"] = error_info.get("statusCode")
    original_record["Chyba_zpráva"] = error_info.get("message")
    error_rows.append(original_record)

if error_rows:
    pd.DataFrame(error_rows).to_csv(f"{DEFAULT_OUTPUT_DIR}/assets_import_errors.csv", index=False)
    print("❌ Chyby uloženy do assets_import_errors.csv")

# 📁 Finální export
df.to_csv(f"{DEFAULT_OUTPUT_DIR}/assets_mapped.csv", index=False)
print("✅ Hotovo! Vše uloženo do složky output/")
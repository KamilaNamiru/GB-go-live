import pandas as pd
import os
import json
import numpy as np
import datetime
import warnings  # ← sem s tím
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from more_itertools import chunked
warnings.filterwarnings("ignore", category=UserWarning)


DEFAULT_OUTPUT_DIR = "output"
OBJECT_API_NAME = "Invoice__c"

# 🔐 Načtení přihlašovacích údajů
load_dotenv("credentials.env")
sf = Salesforce(
    username=os.getenv("SF_USERNAME"),
    password=os.getenv("SF_PASSWORD"),
    security_token=os.getenv("SF_TOKEN"),
    domain=os.getenv("SF_DOMAIN", "login")
)
print("✅ Připojeno k Salesforce.")

# 🔁 Načtení mapování ze SDL
def normalize_column_name(name):
    return name.strip().lower().replace(" ", "").replace("/", "").replace(".", "").replace("+", "").replace("č", "c").replace("ř", "r")

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

# 🧼 Sanitizace záznamu
def sanitize_record_values(rec):
    for key, value in rec.items():
        if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
            rec[key] = None
        elif isinstance(value, (pd.Timestamp, datetime.datetime)):
            rec[key] = value.strftime("%Y-%m-%d")
    return rec

# 📥 Vstupní soubory
invoices_file = "invoices  28.3..xlsx"
mapping_file = "InvoicesMapping.sdl"
output_dir = DEFAULT_OUTPUT_DIR
os.makedirs(output_dir, exist_ok=True)

# 📑 Načti mapping a soubor
mapping = load_sdl_mapping(mapping_file)
df = pd.read_excel(invoices_file)
print("🧾 Sloupce v Excelu:")
for col in df.columns:
    print(f"- '{col}'")
original_columns = df.columns.tolist()
normalized_columns = [normalize_column_name(col) for col in original_columns]
rename_dict = {orig: mapping.get(norm, orig) for orig, norm in zip(original_columns, normalized_columns)}
df.rename(columns=rename_dict, inplace=True)

# 🔄 Párování Accountů podle zdroje (Source)
accounts_df = pd.read_csv("accounts_imported_out.csv")
accounts_df["Helios_ID__c"] = pd.to_numeric(accounts_df["Helios_ID__c"], errors="coerce")
accounts_df["PartnerWeb_ORG_ID__c"] = pd.to_numeric(accounts_df["PartnerWeb_ORG_ID__c"], errors="coerce")
df["Org_Id__c"] = pd.to_numeric(df["Org_Id__c"], errors="coerce")

merged = []
if "Source_Name__c" in df.columns:
    helios_df = df[df["Source_Name__c"].str.lower() == "helios"].copy()
    partnerweb_df = df[df["Source_Name__c"].str.lower() == "partnerweb"].copy()

    helios_df = helios_df.merge(accounts_df[["Id", "Helios_ID__c"]], left_on="Org_Id__c", right_on="Helios_ID__c", how="left")
    partnerweb_df = partnerweb_df.merge(accounts_df[["Id", "PartnerWeb_ORG_ID__c"]], left_on="Org_Id__c", right_on="PartnerWeb_ORG_ID__c", how="left")

    merged = pd.concat([helios_df, partnerweb_df], ignore_index=True)
else:
    merged = df.copy()
    merged["Id"] = None

# nastavení fallback AccountId
merged["Billing_Account__c"] = merged["Id"]
df = merged.drop(columns=["Id", "Helios_ID__c", "PartnerWeb_ORG_ID__c"], errors="ignore")

# 🧪 Prázdné Helios_invoice__c → True
if "Helios_invoice__c" in df.columns:
    df["Helios_invoice__c"] = df["Helios_invoice__c"].isna()

# 🧾 Formátování částek a číselných polí
numeric_fields = [
    "HM_Celkem_bez_z_lohy__c",
    "Total_Amount__c",
    "Max_no_of_Terminals_in_Month__c"
]
for col in numeric_fields:
    if col in df.columns:
        df[col] = (
            df[col].astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(" ", "", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

# 🔁 Přemapování statusů ze čísel na API hodnoty picklistu
status_map = {
    0: "STATE_FOR_REVIEW",
    1: "STATE_APPROVED",
    2: "STATE_PAID",
    3: "STATE_FAILED"
}
if "Status__c" in df.columns:
    df["Status__c"] = df["Status__c"].map(status_map)

# 🆔 Generuj Import_ID__c
if "Name" in df.columns:
    df["Import_ID__c"] = df["Name"].astype(str).str.strip()
else:
    df = df.reset_index(drop=True)
    df["Import_ID__c"] = df.index.map(lambda i: f"INV{str(i + 1).zfill(4)}")

df = df.drop_duplicates(subset=["Import_ID__c"])

# 🗓️ Převod datetime
for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
    df[col] = df[col].dt.strftime("%Y-%m-%d")

# 🧽 Náhrada NaN a sanitizace
df = df.replace([np.nan, float("inf"), float("-inf"), "nan", "NaN"], None)

records = df.to_dict(orient="records")
records = [sanitize_record_values(r) for r in records]

# 💾 Debug JSON
with open(f"{output_dir}/invoices_debug.json", "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)
print("💾 Debug uložen do invoices_debug.json")

# 📤 Upsert do Salesforce
print("📤 Nahrávám faktury do Salesforce...")
responses = []
chunks = list(chunked(records, 10000))
for i, chunk in enumerate(chunks):
    print(f"📦 Nahrávám batch {i+1}/{len(chunks)}...")
    resp = sf.bulk.__getattr__(OBJECT_API_NAME).upsert(chunk, external_id_field='Import_ID__c')
    responses.extend(resp)
response = responses

# 📊 Výsledky
success_count = sum(1 for r in response if r.get("success"))
failures = [r for r in response if not r.get("success")]
print(f"✅ Úspěšně nahráno: {success_count}")
print(f"❌ Selhalo: {len(failures)}")

# 🧾 Zápis chyb
error_rows = []
for i, fail in enumerate(failures):
    original_record = df.iloc[i].copy()
    error_info = fail.get("errors", [{}])[0]
    original_record["Chyba_kód"] = error_info.get("statusCode")
    original_record["Chyba_zpráva"] = error_info.get("message")
    error_rows.append(original_record)

if error_rows:
    pd.DataFrame(error_rows).to_csv(f"{output_dir}/invoices_import_errors.csv", index=False)
    print("🛑 Chyby uloženy do invoices_import_errors.csv")

# 📦 Výstupní soubor se záznamy
df.to_csv(f"{output_dir}/invoices_mapped.csv", index=False)
print("✅ Hotovo! Vše uložené do složky output/")

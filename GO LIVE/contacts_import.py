import pandas as pd
import os
import json
import numpy as np
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

DEFAULT_ACCOUNT_ID = "001J900000CASp3IAH"

# 🔍 Načtení mappingu ze SDL
def load_sdl_mapping(path):
    mapping = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                source, target = line.split("=", 1)
                mapping[source.strip()] = target.strip()
    return mapping

# 🧼 Sanitizace záznamu pro JSON a Salesforce
import datetime

def sanitize_record_values(rec):
    for key, value in rec.items():
        if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
            rec[key] = None
        elif isinstance(value, (pd.Timestamp, datetime.datetime)):
            rec[key] = value.strftime("%Y-%m-%d")
    return rec

# 🔐 Přihlášení do Salesforce
load_dotenv("credentials.env")
sf = Salesforce(
    username=os.getenv("SF_USERNAME"),
    password=os.getenv("SF_PASSWORD"),
    security_token=os.getenv("SF_TOKEN"),
    domain=os.getenv("SF_DOMAIN", "login")
)
print("✅ Připojeno k Salesforce.")

# 📂 Cesty
accounts_file = "accounts_imported_out.csv"
contacts_file = "contacts 28.3..xlsx"
mapping_file = "ContactsMapping.sdl"
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# 🧠 Načti mapping
mapping = load_sdl_mapping(mapping_file)

# 📥 Načti accouny
accounts_df = pd.read_csv(accounts_file)
accounts_df = accounts_df.drop(columns=['Unnamed: 15', 'Organization ID.1', 'Country code'], errors='ignore')
print(f"✅ Načteno {len(accounts_df)} accountů ze souboru '{accounts_file}'")

# Deduplicate podle PartnerWeb_ORG_ID__c
accounts_df = accounts_df.drop_duplicates(subset=["PartnerWeb_ORG_ID__c"])

# 📥 Načti kontakty a přejmenuj sloupce
contacts_df = pd.read_excel(contacts_file)
contacts_df.columns = contacts_df.columns.str.strip()
contacts_df.rename(columns=mapping, inplace=True)
print(f"✅ Načteno {len(contacts_df)} kontaktů ze souboru '{contacts_file}'")
columns_to_ignore = ["Unnamed: 15", "Organization ID", "Organization ID.1", "Country code", "ID"]
contacts_df = contacts_df.drop(columns=[col for col in columns_to_ignore if col in contacts_df.columns])

# 🔗 Merge přes Org_ID__c vs PartnerWeb_ORG_ID__c
contacts_df["Org_ID__c"] = pd.to_numeric(contacts_df["Org_ID__c"], errors="coerce")
accounts_df["PartnerWeb_ORG_ID__c"] = pd.to_numeric(accounts_df["PartnerWeb_ORG_ID__c"], errors="coerce")

merged_df = contacts_df.merge(
    accounts_df[["Id", "PartnerWeb_ORG_ID__c"]],
    how="left",
    left_on="Org_ID__c",
    right_on="PartnerWeb_ORG_ID__c"
)

print(f"🔄 Spojeno: {len(merged_df)} kontaktů, z toho {merged_df['Id'].notna().sum()} namatchováno a {merged_df['Id'].isna().sum()} bez AccountId")

# 🏷️ AccountId + výstup
merged_df["AccountId"] = merged_df["Id"].fillna(DEFAULT_ACCOUNT_ID)
errors_df = merged_df[merged_df["Id"].isna()]
export_df = merged_df.drop(columns=["Id", "PartnerWeb_ORG_ID__c"])

# 📅 Převod datetime sloupců na string
for col in export_df.select_dtypes(include=["datetime64[ns]"]).columns:
    export_df[col] = export_df[col].dt.strftime("%Y-%m-%d")

# 🧽 Vyčištění a převod na záznamy
export_df = export_df.replace([np.nan, float("inf"), float("-inf"), "nan", "NaN"], None)
records = export_df.to_dict(orient="records")
records = [sanitize_record_values(r) for r in records]

# 💾 Debug JSON
with open(f"{output_dir}/contacts_debug.json", "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)
print("💾 Debug uložen do contacts_debug.json")

# 📤 Upsert do SF
print("📤 Nahrávám kontakty do Salesforce...")
response = sf.bulk.Contact.insert(records)

# 📊 Výsledky
success_count = sum(1 for r in response if r.get("success"))
failures = [r for r in response if not r.get("success")]
print(f"✅ Úspěšně nahráno: {success_count}")
print(f"❌ Selhalo: {len(failures)}")

# 🧾 Zápis chyb
error_rows = []
for i, fail in enumerate(failures):
    error_info = fail.get("errors", [{}])[0]
    failed_row = export_df.iloc[i].copy()
    failed_row["Chyba_kód"] = error_info.get("statusCode")
    failed_row["Chyba_zpráva"] = error_info.get("message")
    error_rows.append(failed_row)

if error_rows:
    pd.DataFrame(error_rows).to_csv(f"{output_dir}/contacts_import_errors.csv", index=False)
    print("🛑 Chyby uloženy do contacts_import_errors.csv")

# 📄 Finální výstupy
export_df.to_csv(f"{output_dir}/contacts_mapped.csv", index=False)
errors_df.to_csv(f"{output_dir}/contacts_errors.csv", index=False)
print("✅ Hotovo! Vše uložené do složky output/")
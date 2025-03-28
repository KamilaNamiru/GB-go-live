import pandas as pd
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import os
import json
import numpy as np

# 🔐 Načtení přihlašovacích údajů
load_dotenv("credentials.env")

sf = Salesforce(
    username=os.getenv("SF_USERNAME"),
    password=os.getenv("SF_PASSWORD"),
    security_token=os.getenv("SF_TOKEN"),
    domain=os.getenv("SF_DOMAIN", "login")
)

print("✅ Připojeno k Salesforce.")

# 📥 Načtení Excelu
df = pd.read_excel("accounts 28.3. .xlsx")

# 🧹 Čištění hlavičky
df.columns = df.iloc[0]
df = df.drop(index=0).reset_index(drop=True)

# ✅ Přejmenování sloupců (custom field mapping + billing adresa)
df = df.rename(columns={
    "E-mail": "E_mail__c",
    "PartnerWeb Org ID": "PartnerWeb_ORG_ID__c",
    "Helios ID": "Helios_ID__c",
    "Name": "Name",
    "Phone": "Phone",
    "Blocked": "Blocked__c",
    "Blocked at": "Blocked_on_date__c",
    "Created at last invoice": "Last_jnvoice_date__c",
    "Currency": "Currency__c",
    "State": "State__c",
    "Verified": "Verified__c",
    "Street address": "BillingStreet",
    "ZIP": "BillingPostalCode",
    "City": "BillingCity",
    "Country": "BillingCountry"
})

# ✅ Duplikace adresy pro Shipping
df["ShippingStreet"] = df["BillingStreet"]
df["ShippingPostalCode"] = df["BillingPostalCode"]
df["ShippingCity"] = df["BillingCity"]
df["ShippingCountry"] = df["BillingCountry"]

# ✅ Odstranění NaN a převod na string
df = df.fillna("").infer_objects(copy=False)
for col in df.columns:
    df[col] = df[col].astype(str)

# ✅ Čištění adres – odstranění nebezpečných znaků
for col in ["BillingStreet", "BillingCity", "ShippingStreet", "ShippingCity"]:
    df[col] = df[col].str.replace(r'[\"\\]', '', regex=True)

# ✅ Odstranění nových řádků z polí
df["Name"] = df["Name"].str.replace(r'[\r\n\t]', ' ', regex=True)
df["BillingStreet"] = df["BillingStreet"].str.replace(r'[\r\n\t]', ' ', regex=True)

# ✅ Čištění telefonních čísel – odstranění mezer
df["Phone"] = df["Phone"].str.replace(" ", "")

# ✅ Převod Blocked__c a Verified__c na boolean
df["Blocked__c"] = df["Blocked__c"].apply(lambda x: True if x in ["1", "true", "True"] else False)
df["Verified__c"] = df["Verified__c"].apply(lambda x: True if x in ["1", "true", "True"] else False)

# 🔢 Generování Import_ID__c
IMPORT_ID_PREFIX = "ACC"
df = df.reset_index(drop=True)
df["Import_ID__c"] = df.index.map(lambda i: f"{IMPORT_ID_PREFIX}{str(i + 1).zfill(4)}")

# ✅ Nahrazení NaN, inf, -inf, a 'nan' stringů hodnotou None
df = df.replace([np.nan, float("inf"), float("-inf"), "nan", "NaN"], None)

# ✅ Převod date polí na správný formát YYYY-MM-DD nebo None
date_fields = ["Last_jnvoice_date__c", "Blocked_on_date__c"]
for field in date_fields:
    df[field] = pd.to_datetime(df[field], errors="coerce").dt.strftime("%Y-%m-%d")
    df[field] = df[field].replace("NaT", None)

# 📤 Příprava záznamů
records = df[[
    "Name",
    "Phone",
    "E_mail__c",
    "BillingStreet",
    "BillingPostalCode",
    "BillingCity",
    "BillingCountry",
    "ShippingStreet",
    "ShippingPostalCode",
    "ShippingCity",
    "ShippingCountry",
    "Blocked__c",
    "Blocked_on_date__c",
    "Last_jnvoice_date__c",
    "Currency__c",
    "State__c",
    "Verified__c",
    "PartnerWeb_ORG_ID__c",
    "Helios_ID__c",
    "Import_ID__c"
]].to_dict(orient="records")

# ✅ Čištění NaN/inf/None hodnot po převodu z DataFrame
def sanitize_record_values(rec):
    for key, value in rec.items():
        if isinstance(value, float):
            if np.isnan(value) or np.isinf(value):
                rec[key] = None
    return rec

records = [sanitize_record_values(rec) for rec in records]

# 💾 Uložení všech záznamů do souboru pro analýzu
with open("debug_records.json", "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)
print("💾 Uloženo do debug_records.json")

print("\n🔍 Kontrola serializovatelnosti všech záznamů...")
invalid_records = []

for i, record in enumerate(records):
    try:
        json.dumps(record)
    except Exception as e:
        invalid_records.append((i, record, str(e)))

if invalid_records:
    print(f"\n❌ Nalezeno {len(invalid_records)} nenaserializovatelných záznamů!")
    for i, (index, rec, err) in enumerate(invalid_records[:10]):
        print(f"\n❌ Chybný záznam č. {index}")
        print(json.dumps(rec, indent=2, ensure_ascii=False))
        print("📛 Chyba:", err)
else:
    print("✅ Všechny záznamy jsou serializovatelné.")
# 🔄 Upsert všech záznamů přes BULK API
records = json.loads(json.dumps(records, default=str))
response = sf.bulk.Account.upsert(records, external_id_field='Import_ID__c')

# 📊 Vyhodnocení výsledků
success_count = sum(1 for r in response if r.get("success"))
update_count = sum(1 for r in response if r.get("success") and not r.get("created"))
failures = [r for r in response if not r.get("success")]

print(f"\n✅ Úspěšně nahráno: {success_count}")
print(f"🔁 Z toho aktualizováno: {update_count}")
print(f"❌ Selhalo: {len(failures)}")

for i, fail in enumerate(failures[:10]):
    print(f"\n❌ Chyba č. {i+1}")
    print("  ID:", fail.get('id'))
    print("  Success:", fail.get('success'))
    print("  Errors:", fail.get('errors'))
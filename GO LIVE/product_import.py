import pandas as pd
from simple_salesforce import Salesforce
import os
from dotenv import load_dotenv
import sys

# === Načtení .env souboru ===
load_dotenv("credentials.env")

username = os.getenv("SF_USERNAME")
password = os.getenv("SF_PASSWORD")
token = os.getenv("SF_TOKEN")
domain = os.getenv("SF_DOMAIN", "login")  # "test" pro sandbox

if not all([username, password, token]):
    print("❌ Chybí přihlašovací údaje v souboru credentials.env.")
    sys.exit(1)

# === CONFIG ===
EXCEL_FILE = "produkty 28.3..xlsx"
OUTPUT_FILE = "produkty_28.3_OUT.csv"
SHEET_NAME = "Sheet1"
IMPORT_ID_FIELD = "Import_ID__c"
SALESFORCE_OBJECT = "Product2"
IMPORT_ID_PREFIX = "PROD-"
MAPPING_FILE = "ProductMapping.sdl"

# === Přihlášení do Salesforce ===
try:
    sf = Salesforce(
        username=username,
        password=password,
        security_token=token,
        domain=domain
    )
    print("✅ Přihlášení do Salesforce úspěšné.")
except Exception as e:
    print(f"❌ Přihlášení selhalo: {e}")
    sys.exit(1)

# === Načtení Excelu ===
df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME).fillna("")

# === Odstranění sloupců s konfigurací ===
for col in ["Product Configuration", "Product Configurations"]:
    if col in df.columns:
        df = df.drop(columns=[col])

# === Debug: sloupce v Excelu ===
print("📋 Sloupce v původním Excelu:")
print(df.columns)

# === Načtení SDL mappingu ===
def parse_sdl(path):
    mapping = {}
    with open(path, "r") as f:
        for line in f:
            if "=" in line:
                src, target = line.strip().split("=")
                mapping[src.strip()] = target.strip()
    return mapping

sdl_mapping = parse_sdl(MAPPING_FILE)
print("🗺️ Načtený mapping:")
print(sdl_mapping)

df = df.rename(columns=sdl_mapping)

if "IsActive" in df.columns:
    df["IsActive"] = df["IsActive"].astype(str).str.strip().str.lower().map({
        "true": True,
        "1.0": True,
        "yes": True,
        "ano": True,
        "x": True,
        "false": False,
        "0": False,
        "no": False,
        "ne": False,
        "": False,
        "0.0": False,
        "nan": False
    }).fillna(False)

    print("🔁 Sloupec IsActive byl přetypován na boolean:")
    print(df["IsActive"].value_counts(dropna=False))

# === Debug: sloupce po mappingu ===
print("✅ Sloupce po mappingu:")
print(df.columns)

# === Automatický převod ostatních boolean-like polí ===
def is_boolean_like(series):
    unique_values = set(series.dropna().unique())
    return unique_values.issubset({0, 1, 0.0, 1.0, "0", "1", True, False})

for col in df.columns:
    if col != "IsActive" and is_boolean_like(df[col]):
        print(f"🔁 Přetypovávám {col} na boolean (auto)")
        df[col] = df[col].apply(
            lambda x: True if str(x).strip().lower() in ["1", "true", "yes", "ano", "x"]
            else False
        )

# === Generování Import_ID__c ===
def generate_import_id(index):
    return f"{IMPORT_ID_PREFIX}{str(index + 1).zfill(3)}"

if IMPORT_ID_FIELD not in df.columns:
    df[IMPORT_ID_FIELD] = ""

for i, row in df.iterrows():
    if not row[IMPORT_ID_FIELD]:
        df.at[i, IMPORT_ID_FIELD] = generate_import_id(i)

# === Nahrání do Salesforce (všechny záznamy) ===
inserted_ids = []
for _, row in df.iterrows():
    data = row.to_dict()
    data.pop("Product_Configuration__c", None)

    print("📤 Odesílám záznam:")
    print(data)

    try:
        result = sf.__getattr__(SALESFORCE_OBJECT).create(data)
        inserted_ids.append(result["id"])
    except Exception as e:
        print(f"❌ Chyba při vkládání: {data.get('Name')} – {e}")
        inserted_ids.append("")

# === Doplnění Salesforce ID a výstup ===
df["Salesforce_ID"] = inserted_ids + [""] * (len(df) - len(inserted_ids))

print("✅ Salesforce ID byla přidána:")
print(df[["Name", "Salesforce_ID"]].head())

# === Výstupní soubor do CSV ===
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print(f"✅ Hotovo! Výstupní soubor: {OUTPUT_FILE}")
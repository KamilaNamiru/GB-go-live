import pandas as pd
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import os

# 🔐 Načtení přihlašovacích údajů
load_dotenv("credentials.env")

sf = Salesforce(
    username=os.getenv("SF_USERNAME"),
    password=os.getenv("SF_PASSWORD"),
    security_token=os.getenv("SF_TOKEN"),
    domain=os.getenv("SF_DOMAIN", "login")
)

print("✅ Připojeno k Salesforce.")

# 📥 Načtení dat
kusovnik = pd.read_excel("kusovníky 28.3..xlsx", dtype={"Strom": str})
produkty = pd.read_csv("produkty_28.3_OUT.csv")

# 🧼 Čištění
kusovnik["Reg.č. Produktu"] = kusovnik["Reg.č. Produktu"].astype(str).str.strip().str.replace('"', '')
kusovnik["Reg. č. kusu"] = kusovnik["Reg. č. kusu"].astype(str).str.strip().str.replace('"', '')

# 🛠️ Oprava Tree Number (z datumu zpět na text)
def fix_tree_value(value):
    try:
        if isinstance(value, pd.Timestamp):
            return value.strftime("%-d.%-m.")
        elif str(value).startswith("202"):
            return None
        else:
            return str(value).strip()
    except:
        return str(value).strip()

kusovnik["Strom"] = kusovnik["Strom"].apply(fix_tree_value)

# 🔁 Mapování kódů → ID + názvy
produkty["ProductCode"] = produkty["ProductCode"].astype(str).str.strip().str.replace('"', '')
product_map_id = produkty.set_index("ProductCode")["Salesforce_ID"].to_dict()
product_map_name = produkty.set_index("ProductCode")["Name"].to_dict()

# 🧱 Vytvoření DataFrame
df = pd.DataFrame({
    "Parent_Product_Code__c": kusovnik["Reg.č. Produktu"],
    "Product_Code__c": kusovnik["Reg. č. kusu"],
    "Quantity__c": kusovnik["Množství (MNF)"],
    "Measure_of_Quantity__c": kusovnik["MJ evidence"],
    "Tree_Number__c": kusovnik["Strom"]
})

# 🔗 Mapování ID
df["Parent_Product__c"] = df["Parent_Product_Code__c"].map(product_map_id)
df["Product__c"] = df["Product_Code__c"].map(product_map_id)

# 🏷️ Name = název komponenty
df["Name"] = df["Product_Code__c"].map(product_map_name)

# ✅ Filtrování validních záznamů
df_valid = df[
    df["Parent_Product__c"].notnull() &
    df["Product__c"].notnull() &
    df["Name"].notnull()
].copy()

# 🔢 Generování Import_ID__c (číselná řada)
IMPORT_ID_PREFIX = "PS"

def generate_import_id(index):
    return f"{IMPORT_ID_PREFIX}{str(index + 1).zfill(4)}"  # PS0001, PS0002, ...

df_valid = df_valid.reset_index(drop=True)
df_valid["Import_ID__c"] = df_valid.index.map(generate_import_id)

print(f"\n📦 Připraveno k upsertu: {len(df_valid)} záznamů\n")

# 📤 Převod na list záznamů
records = df_valid[[
    "Name",
    "Parent_Product__c",
    "Product__c",
    "Parent_Product_Code__c",
    "Product_Code__c",
    "Quantity__c",
    "Measure_of_Quantity__c",
    "Tree_Number__c",
    "Import_ID__c"
]].to_dict(orient="records")

# 🔄 Hromadný upsert přes Bulk API podle Import_ID__c
response = sf.bulk.Product_Structure__c.upsert(records, external_id_field='Import_ID__c')

# 📊 Vyhodnocení výsledků
success = sum(1 for r in response if r.get("success"))
failures = [r for r in response if not r.get("success")]

print(f"\n✅ Úspěšně upsertováno: {success}")
print(f"❌ Selhalo: {len(failures)}")

# 🧾 Výpis několika chyb
for i, fail in enumerate(failures[:10]):
    print(f"\n❌ Chyba č. {i+1}")
    print("  Errors:", fail.get("errors"))
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from supabase import create_client

# =========================
# SUPABASE CONFIG (ENV VARS)
# =========================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# SCRAPING
# =========================
url = "https://www.newegg.com/Cell-Phones-Unlocked/SubCategory/ID-2961"

response = requests.get(url, timeout=20)
response.encoding = "utf-8"
soup = BeautifulSoup(response.text, "html.parser")

rows = []
scrape_time = datetime.now().isoformat()

for item in soup.select(".item-cell"):
    title_tag = item.select_one(".item-title")
    price_tag = item.select_one(".price-current")

    if not title_tag or not price_tag:
        continue

    title = title_tag.get_text(strip=True)
    price_text = price_tag.get_text(strip=True)

    rows.append({
        "title": title,
        "price_text": price_text,
        "scraped_at": scrape_time
    })

df = pd.DataFrame(rows)

# =========================
# DATA CLEANING
# =========================
df["price"] = (
    df["price_text"]
    .str.replace(r"[^0-9.]", "", regex=True)
)

df["price"] = pd.to_numeric(df["price"], errors="coerce")

# Drop rows with invalid prices
df.dropna(subset=["price"], inplace=True)
df.reset_index(drop=True, inplace=True)

# =========================
# TRANSFORM
# =========================
df["serial_number"] = df.index + 1

output_df = df[["serial_number", "title", "price"]].copy()
output_df.rename(columns={"title": "item title/description"}, inplace=True)

# =========================
# SAVE CSV (OPTIONAL)
# =========================
filename = "verify_output.csv"

if os.path.exists(filename):
    output_df.to_csv(filename, mode="a", header=False, index=False)
else:
    output_df.to_csv(filename, index=False)

# =========================
# LOAD TO SUPABASE
# =========================
# Clear table first
supabase.table("phones").delete().neq("title", "placeholder").execute()

rows_to_insert = []

for _, row in output_df.iterrows():
    rows_to_insert.append({
        "title": str(row["item title/description"]),
        "price_eur": float(row["price"]),
        "scraped_at": scrape_time,
        "data_serial_number": int(row["serial_number"])
    })

supabase.table("phones").insert(rows_to_insert).execute()

print(f"Inserted {len(rows_to_insert)} rows into Supabase")
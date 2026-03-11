"""
This script creates intentionally messy datasets for the Data Cleaning Skills Matrix.
Each dataset is designed to showcase a specific data cleaning skill.
VERSION 2.0 - Now with Tidy Data, JSON Flattening, Categorical Harmonization, and Encoding!
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import random
import os
from collections import OrderedDict

# Set random seed for reproducibility (so you get the same data I designed)
np.random.seed(42)
random.seed(42)

# Create output directory if it doesn't exist
os.makedirs('../data/raw', exist_ok=True)
os.makedirs('../data/raw/json_files', exist_ok=True)

print("🧹🧹🧹 GENERATING COMPREHENSIVE DATA CLEANING DATASETS")
print("="*70)
print("Version 2.0 - Now with 9 specialized cleaning challenges!")
print("="*70)

# ============================================================================
# DATASET 1: Missing Data - The Survey Autopsy
# ============================================================================
print("\n📊 1. Creating 'The Survey Autopsy' (Missing Data)...")

# Create 500 survey responses
n_rows = 500
survey_data = {
    'respondent_id': [f'R{str(i).zfill(4)}' for i in range(1, n_rows+1)],
    'age': np.random.randint(18, 80, n_rows),
    'gender': np.random.choice(['Male', 'Female', 'Non-binary'], n_rows, p=[0.48, 0.48, 0.04]),
    'satisfaction_score': np.random.randint(1, 6, n_rows),  # 1-5 scale
    'annual_income': np.random.normal(60000, 20000, n_rows).astype(int),
    'feedback': [f"Sample feedback {i}" for i in range(n_rows)],
    'completion_date': [datetime.now() - timedelta(days=random.randint(0, 365)) for _ in range(n_rows)]
}
df_survey = pd.DataFrame(survey_data)

# Introduce intentional missingness patterns

# MCAR - random 5% of ages are missing (completely random)
mcar_mask = np.random.random(n_rows) < 0.05
df_survey.loc[mcar_mask, 'age'] = np.nan

# MAR - older people skip income question (depends on age)
mar_mask = (df_survey['age'].fillna(0) > 60) & (np.random.random(n_rows) < 0.3)
df_survey.loc[mar_mask, 'annual_income'] = np.nan

# MNAR - unhappy people skip feedback (depends on satisfaction, which is missing!)
mnar_mask = (df_survey['satisfaction_score'] <= 2) & (np.random.random(n_rows) < 0.7)
df_survey.loc[mnar_mask, 'feedback'] = np.nan

# Add inconsistent missing value codes (like 'N/A' instead of NaN)
df_survey.loc[np.random.choice(n_rows, 10, replace=False), 'gender'] = 'N/A'
df_survey.loc[np.random.choice(n_rows, 8, replace=False), 'gender'] = 'Unknown'
df_survey.loc[np.random.choice(n_rows, 5, replace=False), 'satisfaction_score'] = 99  # 99 means "didn't answer"

df_survey.to_csv('../data/raw/01_survey_autopsy.csv', index=False)
print(f"   ✅ Created with {df_survey.isna().sum().sum()} missing values")

# ============================================================================
# DATASET 2: Deduplication - The CRM Merge
# ============================================================================
print("\n👥 2. Creating 'The CRM Merge' (Deduplication)...")

# Create base customer data
customers = []
first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emma', 'Robert', 'Lisa', 'William', 'Maria']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Austin']

for i in range(100):
    first = random.choice(first_names)
    last = random.choice(last_names)
    customers.append({
        'customer_id': f'CUST_{str(i).zfill(5)}',
        'full_name': f"{first} {last}",
        'email': f"{first.lower()}.{last.lower()}@email.com",
        'phone': f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        'city': random.choice(cities),
        'signup_date': (datetime.now() - timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d')
    })

df_crm = pd.DataFrame(customers)

# Create a second dataset representing a different system export
df_legacy = df_crm.copy()

# Introduce duplicates with variations (typos, different formats)
duplicate_count = 30
for i in range(duplicate_count):
    idx = random.randint(0, len(customers)-1)
    cust = customers[idx].copy()
    
    # Add typos or format changes
    if random.random() < 0.5:
        cust['full_name'] = cust['full_name'].replace('a', 'e').replace('o', 'u')
    else:
        parts = cust['full_name'].split()
        cust['full_name'] = f"{parts[1]}, {parts[0]}"  # Last, First format
    
    cust['email'] = cust['email'].replace('.', '')  # Remove dots from email
    df_legacy = pd.concat([df_legacy, pd.DataFrame([cust])], ignore_index=True)

# Create 10 exact duplicates (identical records)
exact_dupes = df_crm.sample(10)
df_legacy = pd.concat([df_legacy, exact_dupes], ignore_index=True)

# Shuffle the data
df_legacy = df_legacy.sample(frac=1).reset_index(drop=True)

df_crm.to_csv('../data/raw/02_crm_main.csv', index=False)
df_legacy.to_csv('../data/raw/02_crm_legacy_system.csv', index=False)
print(f"   ✅ Created main CRM ({len(df_crm)} records) and legacy system ({len(df_legacy)} records)")

# ============================================================================
# DATASET 3: Schema & Types - The System Migration
# ============================================================================
print("\n🔄 3. Creating 'The System Migration' (Schema & Types)...")

n_transactions = 200
proper_data = {
    'transaction_id': [f'TXN_{i}' for i in range(1, n_transactions+1)],
    'date': [datetime.now() - timedelta(days=random.randint(0, 365)) for _ in range(n_transactions)],
    'amount': np.random.uniform(10, 1000, n_transactions).round(2),
    'is_completed': np.random.choice([True, False], n_transactions),
    'customer_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], n_transactions),
    'notes': [f"Note {i}" for i in range(n_transactions)]
}
df_proper = pd.DataFrame(proper_data)

# Now corrupt it with intentional type problems
df_messy = df_proper.copy()

# 1. Mix date formats (4 different formats!)
date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d-%b-%Y', '%Y%m%d']
df_messy['date'] = [d.strftime(random.choice(date_formats)) for d in df_messy['date']]

# 2. Currency with symbols and commas (stored as text)
df_messy['amount'] = ['$' + f"{amt:,.2f}" for amt in df_messy['amount']]

# 3. Boolean as mixed strings (Yes/No, True/False, etc.)
bool_mappings = ['Yes/No', 'True/False', '1/0', 'Y/N']
mapping = random.choice(bool_mappings)
if mapping == 'Yes/No':
    df_messy['is_completed'] = df_messy['is_completed'].map({True: 'Yes', False: 'No'})
elif mapping == 'True/False':
    df_messy['is_completed'] = df_messy['is_completed'].map({True: 'True', False: 'False'})
elif mapping == '1/0':
    df_messy['is_completed'] = df_messy['is_completed'].map({True: '1', False: '0'})
else:
    df_messy['is_completed'] = df_messy['is_completed'].map({True: 'Y', False: 'N'})

# 4. Add 'NULL' as a string (not actual NaN)
null_mask = np.random.random(n_transactions) < 0.05
df_messy.loc[null_mask, 'customer_tier'] = 'NULL'

df_messy.to_csv('../data/raw/03_migration_mess.csv', index=False)
print(f"   ✅ Created with mixed date formats, currency strings, and inconsistent booleans")

# ============================================================================
# DATASET 4: Outliers - The Sensor Log
# ============================================================================
print("\n🌡️ 4. Creating 'The Sensor Log' (Outliers)...")

# Create time series of temperature readings (every 5 minutes)
timestamps = [datetime.now() - timedelta(minutes=5*i) for i in range(1000)]
temperatures = []

for i in range(1000):
    # Base temperature around -22°C (normal freezer temp)
    base_temp = -22 + np.random.normal(0, 1)
    
    # Add some realistic patterns
    if 200 < i < 300:  # Door open event - real anomaly (temperature rises)
        base_temp = -15 + np.random.normal(0, 0.5)
    elif 600 < i < 620:  # Sensor glitch (alternating extreme values)
        base_temp = 100 if i % 2 == 0 else -50
    else:
        base_temp = -22 + np.random.normal(0, 0.5)
    
    temperatures.append(round(base_temp, 2))

df_sensor = pd.DataFrame({
    'timestamp': timestamps,
    'temperature_celsius': temperatures,
    'sensor_id': 'SENSOR_01',
    'battery_level': np.random.uniform(20, 100, 1000).round(1)
})

# Add some extreme outliers (impossible readings)
df_sensor.loc[150, 'temperature_celsius'] = 150  # Way too hot
df_sensor.loc[450, 'temperature_celsius'] = -100  # Way too cold
df_sensor.loc[750, 'temperature_celsius'] = 85  # Also impossible

# Add some missing timestamps (data gaps)
df_sensor = df_sensor.drop(index=[200, 201, 202, 203]).reset_index(drop=True)

df_sensor.to_csv('../data/raw/04_sensor_log.csv', index=False)
print(f"   ✅ Created with {len(df_sensor)} temperature readings, including sensor glitches and real anomalies")

# ============================================================================
# DATASET 5: Regex & Text - The Web Scrape
# ============================================================================
print("\n🌐 5. Creating 'The Web Scrape' (Regex & Text)...")

job_listings = [
    "Job: Senior Python Developer at TechCorp - Salary $120k - $150k - Remote - Contact: hiring@techcorp.com",
    "We're hiring a Data Scientist! Location: NYC. Salary: 130,000 USD. Email resume to careers@dataco.io",
    "Frontend Developer (React) needed in Austin, TX. Pay: $110K/year. Apply at https://company.com/careers",
    "Entry Level Position: Junior Data Analyst. $65,000 - San Francisco, CA. Send CV to hr@startup.co",
    "Backend Engineer - Java/Spring - Chicago IL - 120k-140k + equity - jobs@bigtech.com",
    "REMOTE: Machine Learning Engineer. Salary $150k. 5+ years exp. Apply at https://apply.airecruiting.io/mlengineer",
    "Marketing Manager - $85K - Miami FL - Contact: sarah@marketingagency.com for more info",
    "DevOps Engineer with AWS experience. $140-160K. Hybrid in Boston. Call 555-123-4567",
    "Product Manager (Tech) - Series A Startup - $130K + options - careers@startup.io",
    "Senior Data Engineer @ FinanceCo. Salary: $175,000. New York, NY. Python/SQL required.",
]

# Create multiple versions to have more rows
all_listings = []
for _ in range(30):  # Repeat to get 300+ rows
    for listing in job_listings:
        # Add slight variations
        if random.random() < 0.3:
            listing = listing.replace('$', '').replace('k', ' thousand')
        if random.random() < 0.2:
            listing = listing.lower()
        all_listings.append(listing)

# Shuffle
random.shuffle(all_listings)

df_jobs = pd.DataFrame({
    'raw_scraped_text': all_listings,
    'scrape_date': datetime.now().strftime('%Y-%m-%d')
})

df_jobs.to_csv('../data/raw/05_web_scrape.csv', index=False)
print(f"   ✅ Created {len(df_jobs)} messy job listings for text extraction")

# ============================================================================
# DATASET 6: Tidy Data & Reshaping - The "Wide to Long" Challenge
# ============================================================================
print("\n📈 6. Creating 'The Reshaping Challenge' (Tidy Data)...")

# Create a typical "wide" format dataset that clients often provide
companies = ['TechCorp', 'DataInc', 'AnalyticsLLC', 'BizSolutions', 'CloudCo']
years = [2019, 2020, 2021, 2022, 2023]
metrics = ['Revenue', 'Expenses', 'Profit', 'Employees']

# Create wide format data (years as columns!)
wide_data = []
for i, company in enumerate(companies):
    row = {'company': company}
    for year in years:
        for metric in metrics:
            # Create realistic values
            if metric == 'Revenue':
                value = np.random.uniform(1e6, 10e6, 1)[0].round(2)
            elif metric == 'Expenses':
                value = np.random.uniform(0.5e6, 8e6, 1)[0].round(2)
            elif metric == 'Profit':
                # Profit = Revenue - Expenses (will calculate properly later)
                value = np.random.uniform(0.1e6, 5e6, 1)[0].round(2)
            else:  # Employees
                value = np.random.randint(50, 500)
            
            # Create column name like "Revenue_2020"
            col_name = f"{metric}_{year}"
            row[col_name] = value
    
    # Add some intentional inconsistency - different column naming for one year
    row['Profits_2023'] = row.pop('Profit_2023')  # Rename for one year only
    
    wide_data.append(row)

df_wide = pd.DataFrame(wide_data)

# Save as CSV
df_wide.to_csv('../data/raw/06_wide_format_financials.csv', index=False)
print(f"   ✅ Created wide format data with {len(df_wide.columns)} columns (company + 5 years × 4 metrics)")

# Also create an Excel file with multiple sheets to simulate real client mess
with pd.ExcelWriter('../data/raw/06_messy_excel_report.xlsx') as writer:
    df_wide.to_excel(writer, sheet_name='Raw Data', index=False)
    
    # Add a summary sheet with merged cells (common mess)
    summary_data = pd.DataFrame({
        'Note': ['This report was generated manually', 'Numbers are in thousands', 'Contact finance@company.com for questions']
    })
    summary_data.to_excel(writer, sheet_name='Summary', index=False)

print("   ✅ Also created messy Excel file with multiple sheets")

# ============================================================================
# DATASET 7: Nested JSON Flattening - The API Response Challenge
# ============================================================================
print("\n🔷 7. Creating 'The JSON Flattening Challenge' (Semi-structured Data)...")

# Create nested JSON structure mimicking an e-commerce API response
orders = []
for i in range(1, 101):  # 100 orders
    order = {
        "order_id": f"ORD_{str(i).zfill(5)}",
        "order_date": (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
        "customer": {
            "customer_id": f"CUST_{random.randint(1, 50)}",
            "name": f"Customer {i}",
            "email": f"customer{i}@email.com",
            "address": {
                "street": f"{random.randint(1, 999)} Main St",
                "city": random.choice(["New York", "Boston", "Chicago", "Seattle"]),
                "zipcode": f"{random.randint(10000, 99999)}"
            }
        },
        "items": []
    }
    
    # Add 1-5 items per order (nested inside each order)
    for j in range(random.randint(1, 5)):
        item = {
            "product_id": f"PROD_{random.randint(100, 999)}",
            "product_name": random.choice(["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]),
            "quantity": random.randint(1, 3),
            "price": round(random.uniform(10, 1000), 2),
            "seller": {
                "seller_id": f"SELL_{random.randint(10, 99)}",
                "seller_name": random.choice(["TechSeller", "GadgetShop", "OfficeSupplyCo"])
            }
        }
        order["items"].append(item)
    
    orders.append(order)

# Save as a single JSON file
with open('../data/raw/07_nested_orders.json', 'w') as f:
    json.dump(orders, f, indent=2)

# Also create individual JSON files (common in real scraping projects)
os.makedirs('../data/raw/json_files/orders', exist_ok=True)
for i, order in enumerate(orders[:20]):  # Save 20 individual files
    with open(f'../data/raw/json_files/orders/order_{i+1}.json', 'w') as f:
        json.dump(order, f, indent=2)

print(f"   ✅ Created {len(orders)} nested JSON orders with customer and item details")
print(f"   ✅ Created 20 individual JSON files in json_files/orders/")

# ============================================================================
# DATASET 8: Categorical Harmonization - The "State Names" Mess
# ============================================================================
print("\n🗺️ 8. Creating 'The Categorical Harmonization' (Fuzzy Matching)...")

# Create dataset with inconsistent categorical values
n_records = 500

# Different ways people write state names
state_variations = {
    'California': ['CA', 'Calif', 'Cal', 'California', 'CALIFORNIA', 'Ca.', 'US-CA'],
    'New York': ['NY', 'N.Y.', 'New York', 'NEW YORK', 'NYC', 'N.Y', 'New York State'],
    'Texas': ['TX', 'Tex', 'Texas', 'TEXAS', 'Tx.', 'TX'],
    'Florida': ['FL', 'Fla', 'Florida', 'FLORIDA', 'Fl.'],
    'Illinois': ['IL', 'Ill', 'Illinois', 'ILLINOIS', 'Il.'],
}

# Create list of all possible variations
all_variations = []
for state, variations in state_variations.items():
    all_variations.extend(variations)

# Generate records with random state variations
state_column = [random.choice(all_variations) for _ in range(n_records)]

# Add some typos
for i in range(50):
    if random.random() < 0.3:
        state_column[i] = state_column[i] + ' '  # extra space
    if random.random() < 0.2:
        state_column[i] = state_column[i].replace('a', 'e')  # common typo

df_categorical = pd.DataFrame({
    'customer_id': [f'CUST_{str(i).zfill(5)}' for i in range(1, n_records+1)],
    'purchase_amount': np.random.uniform(10, 1000, n_records).round(2),
    'purchase_date': [datetime.now() - timedelta(days=random.randint(0, 365)) for _ in range(n_records)],
    'state': state_column,
    'product_category': np.random.choice(['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books'], n_records)
})

# Add inconsistent product categories too
category_mess = {
    'Electronics': ['Electronics', 'Elect', 'ELEC', 'electronic', 'Electronics ', 'Gadgets'],
    'Clothing': ['Clothing', 'Apparel', 'clothes', 'CLOTH', 'Wearables', 'Fashion'],
}
for i in range(100):
    if random.random() < 0.4:
        original = df_categorical.loc[i, 'product_category']
        if original in category_mess:
            df_categorical.loc[i, 'product_category'] = random.choice(category_mess[original])

df_categorical.to_csv('../data/raw/08_categorical_mess.csv', index=False)
print(f"   ✅ Created {n_records} records with {len(set(state_column))} unique state name variations")

# ============================================================================
# DATASET 9: Encoding & Special Character Recovery (FIXED VERSION)
# ============================================================================
print("\n🔤 9. Creating 'The Encoding Nightmare' (Character Recovery)...")

# Create text with international characters (accents, non-Latin scripts)
international_names = [
    "José González", "François Hollande", "Müller GmbH", "Åsa Larsson", 
    "中国客户", "客户名称", "Élève", "Français", "Dvořák", "Łukasz",
    "São Paulo", "München", "Côte d'Azur", "Peña", "García"
]

# Create matching length arrays (all 15 items)
cities = ["México", "Paris", "Berlin", "Stockholm", "北京", 
          "上海", "Paris", "Lyon", "Praha", "Warszawa",
          "São Paulo", "Berlin", "Paris", "Madrid", "Barcelona"]

descriptions = ["Café", "Résumé", "Façade", "Ångström", "公司", 
                "项目", "École", "Théâtre", "Český", "Muzyka",
                "São Paulo", "München", "Côte", "Peña", "García"]

# Verify all lists have same length
print(f"   Length check: names={len(international_names)}, cities={len(cities)}, descriptions={len(descriptions)}")

# Create dataset with proper Unicode
df_encoding_proper = pd.DataFrame({
    'id': range(1, len(international_names) + 1),
    'name': international_names,
    'city': cities,
    'description': descriptions
})

# Save with proper UTF-8 encoding
df_encoding_proper.to_csv('../data/raw/09_unicode_proper.csv', index=False, encoding='utf-8')

# NOW - create the CORRUPTED version that clients actually receive
# Simulate opening UTF-8 file with wrong encoding (Latin-1)

def corrupt_encoding(text):
    """Simulate opening UTF-8 file with Latin-1 encoding to create mojibake"""
    if isinstance(text, str):
        # Encode as UTF-8, then decode as Latin-1 to create mojibake
        # This turns "é" into "Ã©" which is a common corruption
        try:
            return text.encode('utf-8').decode('latin-1')
        except:
            return text
    return text

# Apply corruption to each column
df_encoding_corrupted = df_encoding_proper.copy()
for col in df_encoding_corrupted.columns:
    if col != 'id':  # Don't corrupt IDs
        df_encoding_corrupted[col] = df_encoding_corrupted[col].apply(corrupt_encoding)

# Save the corrupted version
df_encoding_corrupted.to_csv('../data/raw/09_encoding_corrupted.csv', index=False)
print(f"   ✅ Created proper UTF-8 file and CORRUPTED version with mojibake")
print(f"   ✅ Example: 'José' becomes '{df_encoding_corrupted['name'].iloc[0]}'")
# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("✅✅✅ ALL DATASETS CREATED SUCCESSFULLY!")
print("="*70)
print("\n📁 Location: data/raw/")
print("\nYour 9 datasets:")
print("  1. 01_survey_autopsy.csv     - Missing Data (MCAR/MAR/MNAR)")
print("  2. 02_crm_main.csv            - Deduplication (Main CRM)")
print("  3. 02_crm_legacy_system.csv   - Deduplication (Legacy System)")
print("  4. 03_migration_mess.csv      - Schema & Type Coercion")
print("  5. 04_sensor_log.csv          - Outlier Detection")
print("  6. 05_web_scrape.csv          - Regex Text Extraction")
print("  7. 06_wide_format_financials.csv - Tidy Data/Reshaping")
print("  8. 07_nested_orders.json       - JSON Flattening (API Data)")
print("  9. 08_categorical_mess.csv     - Categorical Harmonization")
print(" 10. 09_unicode_proper.csv       - Encoding (Clean version)")
print(" 11. 09_encoding_corrupted.csv   - Encoding (Corrupted version)")
print("\n📁 Bonus files:")
print("  - json_files/orders/          - 20 individual JSON files")
print("  - 06_messy_excel_report.xlsx  - Multi-sheet Excel with merged cells")
print("\n🚀 Next steps:")
print("  1. jupyter notebook")
print("  2. Create notebooks for each dataset")
print("  3. Follow the template structure")
print("="*70)
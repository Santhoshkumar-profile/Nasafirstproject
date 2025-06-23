#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import time
import json
from datetime import datetime, timedelta

API_KEY = 'Fp4RAAGWVxuvp7TgejE4aPjoaTYlzMqqR0Kqmewe'  
BASE_URL = 'https://api.nasa.gov/neo/rest/v1/feed'
START_DATE = datetime.strptime("2024-01-01", "%Y-%m-%d")
DAYS_STEP = 7
TOTAL_RECORDS = 10000

all_asteroids = []

def fetch_neo_feed(start_date, end_date):
    params = {
        'start_date': start_date.strftime("%Y-%m-%d"),
        'end_date': end_date.strftime("%Y-%m-%d"),
        'api_key': API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {start_date} → {end_date}: {e}")
        return None

current_start = START_DATE

while len(all_asteroids) < TOTAL_RECORDS:
    current_end = current_start + timedelta(days=DAYS_STEP - 1)

    print(f"Fetching data from {current_start.date()} to {current_end.date()}...")
    data = fetch_neo_feed(current_start, current_end)

    if data and 'near_earth_objects' in data:
        # Flatten daily asteroid lists
        for date in data['near_earth_objects']:
            all_asteroids.extend(data['near_earth_objects'][date])

        print(f"→ Total collected: {len(all_asteroids)}")

        # Use 'links' → 'next' to go forward 7 days
        current_start += timedelta(days=DAYS_STEP)
        time.sleep(1)
    else:
        print("Stopping due to API error or empty response.")
        break

# ✅ Trim to exactly 10,000 if needed
all_asteroids = all_asteroids[:TOTAL_RECORDS]

# 💾 Save to JSON file
with open("neo_feed_10000.json", "w") as f:
    json.dump(all_asteroids, f)

print("✅ Data collection complete. 10,000 records saved.")


# In[2]:


import json

# Load the raw data
with open('neo_asteroids_raw_10000.json', 'r') as f:
    raw_data = json.load(f)

print(f"✅ Loaded {len(all_asteroids)} asteroid records")


# In[3]:


len(all_asteroids)


# In[8]:


import json
from datetime import datetime

# Load the raw data
with open('neo_asteroids_raw_10000.json', 'r') as f:
    all_asteroids = json.load(f)

cleaned_asteroids = []
close_approaches = []

for asteroid in all_asteroids:
    try:
        asteroid_id = int(asteroid.get('id', 0))
        neo_reference_id = int(asteroid.get('neo_reference_id', 0))
        name = asteroid.get('name', 'Unknown')
        absolute_magnitude_h = asteroid.get('absolute_magnitude_h')

        # Skip if essential fields are missing
        if absolute_magnitude_h is None or 'estimated_diameter' not in asteroid:
            print(f"⚠️ Skipping asteroid {asteroid.get('id')} due to missing magnitude or diameter")
            continue

        diameter_data = asteroid['estimated_diameter'].get('kilometers')
        if not diameter_data:
            print(f"⚠️ Skipping asteroid {asteroid.get('id')} due to missing diameter data")
            continue

        estimated_diameter_min_km = float(diameter_data.get('estimated_diameter_min', 0))
        estimated_diameter_max_km = float(diameter_data.get('estimated_diameter_max', 0))
        is_hazardous = asteroid.get('is_potentially_hazardous_asteroid', False)

        cleaned_asteroids.append({
            'id': asteroid_id,
            'neo_reference_id': neo_reference_id,
            'name': name,
            'absolute_magnitude_h': float(absolute_magnitude_h),
            'estimated_diameter_min_km': estimated_diameter_min_km,
            'estimated_diameter_max_km': estimated_diameter_max_km,
            'is_potentially_hazardous_asteroid': is_hazardous
        })

        for approach in asteroid.get('close_approach_data', []):
            try:
                approach_date = datetime.strptime(approach['close_approach_date'], '%Y-%m-%d').date()
                velocity_kmph = float(approach.get('relative_velocity', {}).get('kilometers_per_hour', 0))
                miss_distance_km = float(approach.get('miss_distance', {}).get('kilometers', 0))
                miss_distance_lunar = float(approach.get('miss_distance', {}).get('lunar', 0))
                orbiting_body = approach.get('orbiting_body', 'Unknown')

                close_approaches.append({
                    'asteroid_id': asteroid_id,
                    'close_approach_date': approach_date,
                    'relative_velocity_kmph': velocity_kmph,
                    'miss_distance_km': miss_distance_km,
                    'miss_distance_lunar': miss_distance_lunar,
                    'orbiting_body': orbiting_body
                })

            except Exception as e:
                print(f"⚠️ Skipping close approach for asteroid {asteroid_id} due to error: {e}")

    except Exception as e:
        print(f"⚠️ Skipping asteroid {asteroid.get('id', 'unknown')} due to error: {e}")

print(f"\n✅ Extracted {len(cleaned_asteroids)} asteroid base records")
print(f"✅ Extracted {len(close_approaches)} close approach records")


# In[9]:


import json

# Save cleaned asteroid data
with open("neo_cleaned_asteroids.json", "w") as f:
    json.dump(cleaned_asteroids, f, indent=2, default=str)

# Save close approach data
with open("neo_close_approaches.json", "w") as f:
    json.dump(close_approaches, f, indent=2, default=str)

print("✅ Cleaned data saved to JSON files")


# In[10]:


# pip install pymysql #


# In[11]:


import pymysql

# Step 1: Connect to MySQL
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='root@123',
    database='nasa_neo',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)
cursor = conn.cursor()
print("✅ Connection established.")


# In[12]:


# Create tables
create_asteroids_table = """
CREATE TABLE IF NOT EXISTS asteroids (
    id BIGINT PRIMARY KEY,
    neo_reference_id BIGINT,
    name VARCHAR(255),
    absolute_magnitude_h FLOAT,
    estimated_diameter_min_km FLOAT,
    estimated_diameter_max_km FLOAT,
    is_potentially_hazardous_asteroid BOOLEAN
);
"""

create_close_approaches_table = """
CREATE TABLE IF NOT EXISTS close_approaches (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    asteroid_id BIGINT,
    close_approach_date DATE,
    relative_velocity_kmph FLOAT,
    miss_distance_km FLOAT,
    miss_distance_lunar FLOAT,
    orbiting_body VARCHAR(50),
    FOREIGN KEY (asteroid_id) REFERENCES asteroids(id)
);
"""

cursor.execute(create_asteroids_table)
cursor.execute(create_close_approaches_table)
conn.commit()
print("✅ Tables created successfully.")


# In[13]:


import json
import pymysql
from datetime import datetime

# Step 1: Load JSON data
with open("neo_cleaned_asteroids.json", "r") as f:
    cleaned_asteroids = json.load(f)

with open("neo_close_approaches.json", "r") as f:
    close_approaches = json.load(f)

# Step 2: Connect to MySQL
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='root@123',
    database='nasa_neo',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)
cursor = conn.cursor()

# Step 3: Insert data into asteroids table
asteroid_insert_query = """
INSERT IGNORE INTO asteroids (
    id, neo_reference_id, name, absolute_magnitude_h,
    estimated_diameter_min_km, estimated_diameter_max_km, is_potentially_hazardous_asteroid
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

for asteroid in cleaned_asteroids:
    cursor.execute(asteroid_insert_query, (
        asteroid['id'],
        asteroid['neo_reference_id'],
        asteroid['name'],
        asteroid['absolute_magnitude_h'],
        asteroid['estimated_diameter_min_km'],
        asteroid['estimated_diameter_max_km'],
        asteroid['is_potentially_hazardous_asteroid']
    ))

# Step 4: Insert data into close_approaches table
close_approach_insert_query = """
INSERT INTO close_approaches (
    asteroid_id, close_approach_date, relative_velocity_kmph,
    miss_distance_km, miss_distance_lunar, orbiting_body
) VALUES (%s, %s, %s, %s, %s, %s)
"""

for approach in close_approaches:
    cursor.execute(close_approach_insert_query, (
        approach['asteroid_id'],
        approach['close_approach_date'],
        approach['relative_velocity_kmph'],
        approach['miss_distance_km'],
        approach['miss_distance_lunar'],
        approach['orbiting_body']
    ))

conn.commit()
cursor.close()
conn.close()
print("✅ Data inserted into MySQL successfully.")


# In[15]:


import streamlit as st
import pymysql
import pandas as pd
from datetime import date

# --- Database Connection ---
def get_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='root@123',
        database='nasa_neo',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# --- Query Options ---
QUERY_OPTIONS = {
    "1. Asteroid approach count": """
        SELECT a.name, COUNT(ca.id) AS approach_count
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        GROUP BY a.name
        ORDER BY approach_count DESC;
    """,

    "2. Average velocity per asteroid": """
        SELECT a.name, ROUND(AVG(ca.relative_velocity_kmph), 2) AS avg_velocity_kmph
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        GROUP BY a.name
        ORDER BY avg_velocity_kmph DESC;
    """,

    "3. Top 10 fastest asteroids": """
        SELECT a.name, MAX(ca.relative_velocity_kmph) AS max_velocity
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        GROUP BY a.name
        ORDER BY max_velocity DESC
        LIMIT 10;
    """,

    "4. Hazardous asteroids >3 approaches": """
        SELECT a.name, COUNT(*) AS approaches
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        WHERE a.is_potentially_hazardous_asteroid = 1
        GROUP BY a.name
        HAVING approaches > 3
        ORDER BY approaches DESC;
    """,

    "5. Month with most approaches": """
        SELECT MONTH(close_approach_date) AS month, COUNT(*) AS total_approaches
        FROM close_approaches
        GROUP BY month
        ORDER BY total_approaches DESC;
    """,

    "6. Fastest approach ever": """
        SELECT a.name, ca.close_approach_date, ca.relative_velocity_kmph
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        ORDER BY ca.relative_velocity_kmph DESC
        LIMIT 1;
    """,

    "7. Sort by max estimated diameter": """
        SELECT name, estimated_diameter_max_km
        FROM asteroids
        ORDER BY estimated_diameter_max_km DESC;
    """,

    "8. Closest approach getting nearer": """
        SELECT ca.asteroid_id, a.name, ca.close_approach_date, ca.miss_distance_km
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        WHERE ca.asteroid_id = (
            SELECT asteroid_id
            FROM close_approaches
            GROUP BY asteroid_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        )
        ORDER BY ca.close_approach_date;
    """,

    "9. Closest approach per asteroid": """
        SELECT a.name, ca.close_approach_date, ca.miss_distance_km
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        WHERE (a.id, ca.miss_distance_km) IN (
            SELECT asteroid_id, MIN(miss_distance_km)
            FROM close_approaches
            GROUP BY asteroid_id
        );
    """,

    "10. Asteroids > 50000 km/h": """
        SELECT DISTINCT a.name, ca.relative_velocity_kmph
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        WHERE ca.relative_velocity_kmph > 50000;
    """,

    "11. Approaches per month": """
        SELECT DATE_FORMAT(close_approach_date, '%Y-%m') AS month, COUNT(*) AS total_approaches
        FROM close_approaches
        GROUP BY month
        ORDER BY month;
    """,

    "12. Highest brightness (lowest magnitude)": """
        SELECT name, absolute_magnitude_h
        FROM asteroids
        ORDER BY absolute_magnitude_h ASC
        LIMIT 1;
    """,

    "13. Hazardous vs Non-hazardous count": """
        SELECT is_potentially_hazardous_asteroid, COUNT(*) AS total
        FROM asteroids
        GROUP BY is_potentially_hazardous_asteroid;
    """,

    "14. Asteroids closer than Moon (<1 LD)": """
        SELECT a.name, ca.close_approach_date, ca.miss_distance_lunar
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        WHERE ca.miss_distance_lunar < 1
        ORDER BY ca.miss_distance_lunar ASC;
    """,

    "15. Asteroids within 0.05 AU": """
        SELECT a.name, ca.close_approach_date, ca.miss_distance_km
        FROM close_approaches ca
        JOIN asteroids a ON ca.asteroid_id = a.id
        WHERE ca.miss_distance_km < 7480000  -- approx 0.05 AU
        ORDER BY ca.miss_distance_km ASC;
    """
}


# --- Streamlit App ---
st.title("🚀 NASA NEO Asteroid Explorer")

with st.sidebar:
    st.header("🛰️ Query Explorer")
    query_name = st.selectbox("Choose a predefined query:", list(QUERY_OPTIONS.keys()))

    st.header("🔍 Dynamic Filters")
    date_range = st.date_input("Close Approach Date Range", [date(2024, 1, 1), date(2024, 12, 31)])
    max_au = st.slider("Max Astronomical Units (approximate)", 0.0, 0.1, 0.05)
    max_lunar = st.slider("Max Lunar Distance", 0.0, 5.0, 1.0)
    min_velocity = st.slider("Min Relative Velocity (km/h)", 0.0, 150000.0, 0.0)
    min_diameter = st.slider("Min Estimated Diameter (km)", 0.0, 5.0, 0.0)
    max_diameter = st.slider("Max Estimated Diameter (km)", 0.0, 10.0, 5.0)
    hazard_filter = st.selectbox("Hazardous Only?", ["All", "Yes", "No"])

# --- Execute Selected Query ---
conn = get_connection()
cursor = conn.cursor()

if query_name:
    st.subheader("📊 Query Results")
    sql = QUERY_OPTIONS[query_name]
    cursor.execute(sql)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    st.dataframe(df)

# --- Apply Filtered Search ---
st.subheader("🔬 Filtered Asteroid Records")

filter_sql = f"""
    SELECT a.name, ca.close_approach_date, ca.relative_velocity_kmph, 
           ca.miss_distance_km, ca.miss_distance_lunar, 
           a.estimated_diameter_min_km, a.estimated_diameter_max_km,
           a.is_potentially_hazardous_asteroid
    FROM close_approaches ca
    JOIN asteroids a ON ca.asteroid_id = a.id
    WHERE ca.close_approach_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
      AND ca.miss_distance_km < {max_au * 149600000} -- AU to KM
      AND ca.miss_distance_lunar < {max_lunar}
      AND ca.relative_velocity_kmph >= {min_velocity}
      AND a.estimated_diameter_min_km >= {min_diameter}
      AND a.estimated_diameter_max_km <= {max_diameter}
"""

if hazard_filter == "Yes":
    filter_sql += " AND a.is_potentially_hazardous_asteroid = TRUE"
elif hazard_filter == "No":
    filter_sql += " AND a.is_potentially_hazardous_asteroid = FALSE"

cursor.execute(filter_sql)
filtered_rows = cursor.fetchall()
df_filtered = pd.DataFrame(filtered_rows)
st.dataframe(df_filtered)

conn.close()


# In[ ]:





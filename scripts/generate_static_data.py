# Create a script to generate static JSON data
import sqlite3
import json
import os
import sys
from datetime import datetime

# Fix Unicode encoding for Windows console
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        # Python < 3.7
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

print("Generating static JSON data for frontend...")

db_path = os.path.join('..', 'backend', 'data.db')
output_dir = os.path.join('..', 'frontend', 'public', 'data')

# Create output directory
os.makedirs(output_dir, exist_ok=True)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# 1. Create jobs index (minimal data for listing)
cursor.execute("""
    SELECT 
        id,
        source_id,
        job_title,
        company_name,
        province,
        city,
        recommended_degree,
        quota,
        registered,
        application_deadline,
        LENGTH(job_description) as desc_length
    FROM jobs
    ORDER BY application_deadline ASC, scraped_at DESC
""")

jobs = cursor.fetchall()

# Convert to optimized format (short keys for smaller files)
jobs_optimized = []
for job in jobs:
    jobs_optimized.append({
        'i': job['id'],  # id
        'sid': job['source_id'],  # source_id
        't': job['job_title'],  # title
        'c': job['company_name'],  # company
        'p': job['province'],  # province
        'l': job['city'],  # location (city)
        'd': job['recommended_degree'],  # degree
        'q': job['quota'],  # quota
        'r': job['registered'],  # registered
        'dl': job['application_deadline'],  # deadline
        'len': job['desc_length']  # description length
    })

# Save jobs index
jobs_index_path = os.path.join(output_dir, 'jobs-index.json')
with open(jobs_index_path, 'w', encoding='utf-8') as f:
    json.dump(jobs_optimized, f, separators=(',', ':'), ensure_ascii=False)

print(f"✅ Created jobs index: {len(jobs_optimized)} jobs")
print(f"   File size: {os.path.getsize(jobs_index_path) / 1024:.1f} KB")

# 2. Create full job details (separate files for lazy loading)
details_dir = os.path.join(output_dir, 'jobs')
os.makedirs(details_dir, exist_ok=True)

cursor.execute("SELECT id, * FROM jobs")
all_jobs = cursor.fetchall()

for job in all_jobs:
    job_id = job['id']
    job_data = dict(job)
    
    # Remove some fields we don't need in frontend
    for field in ['scraped_at', 'categorized_at', 'last_updated']:
        if field in job_data:
            del job_data[field]
    
    # Save individual job file
    job_file = os.path.join(details_dir, f"{job_id}.json")
    with open(job_file, 'w', encoding='utf-8') as f:
        json.dump(job_data, f, ensure_ascii=False)

print(f"✅ Created {len(all_jobs)} individual job files")

# 3. Create province index
cursor.execute("SELECT DISTINCT province FROM jobs ORDER BY province")
provinces = [row['province'] for row in cursor.fetchall()]

province_index_path = os.path.join(output_dir, 'provinces.json')
with open(province_index_path, 'w', encoding='utf-8') as f:
    json.dump(provinces, f, ensure_ascii=False)

print(f"✅ Created province index: {len(provinces)} provinces")

# 4. Create city index (by province)
cursor.execute("SELECT province, city, COUNT(*) as count FROM jobs GROUP BY province, city ORDER BY province, city")
cities_by_province = {}

for row in cursor.fetchall():
    province = row['province']
    city = row['city']
    count = row['count']
    
    if province not in cities_by_province:
        cities_by_province[province] = []
    
    cities_by_province[province].append({
        'city': city,
        'count': count
    })

cities_path = os.path.join(output_dir, 'cities-by-province.json')
with open(cities_path, 'w', encoding='utf-8') as f:
    json.dump(cities_by_province, f, ensure_ascii=False)

print(f"✅ Created city index for {len(cities_by_province)} provinces")

# 5. Create job statistics
cursor.execute("SELECT COUNT(*) as total_jobs FROM jobs")
total_jobs = cursor.fetchone()['total_jobs']

cursor.execute("SELECT COUNT(DISTINCT company_name) as total_companies FROM jobs")
total_companies = cursor.fetchone()['total_companies']

cursor.execute("SELECT COUNT(DISTINCT province) as total_provinces FROM jobs")
total_provinces = cursor.fetchone()['total_provinces']

cursor.execute("SELECT SUM(quota) as total_quota, SUM(registered) as total_registered FROM jobs")
stats = cursor.fetchone()
total_quota = stats['total_quota'] or 0
total_registered = stats['total_registered'] or 0

# Top companies
cursor.execute("""
    SELECT company_name, COUNT(*) as job_count
    FROM jobs 
    GROUP BY company_name 
    ORDER BY job_count DESC 
    LIMIT 20
""")
top_companies = [{"name": row['company_name'], "count": row['job_count']} for row in cursor.fetchall()]

# Job categories (simplified)
cursor.execute("""
    SELECT 
        CASE 
            WHEN job_title LIKE '%guru%' OR job_title LIKE '%teacher%' OR job_title LIKE '%pendidik%' THEN 'Education'
            WHEN job_title LIKE '%dokter%' OR job_title LIKE '%perawat%' OR job_title LIKE '%medis%' THEN 'Medical'
            WHEN job_title LIKE '%teknik%' OR job_title LIKE '%engineer%' OR job_title LIKE '%teknisi%' THEN 'Engineering'
            WHEN job_title LIKE '%komputer%' OR job_title LIKE '%IT%' OR job_title LIKE '%programmer%' OR job_title LIKE '%pranata%' THEN 'IT/Technology'
            WHEN job_title LIKE '%administrasi%' OR job_title LIKE '%admin%' OR job_title LIKE '%tata usaha%' THEN 'Administration'
            WHEN job_title LIKE '%laboran%' OR job_title LIKE '%laboratorium%' THEN 'Laboratory'
            ELSE 'Other'
        END as category,
        COUNT(*) as count
    FROM jobs
    GROUP BY category
    ORDER BY count DESC
""")
categories = [{"category": row['category'], "count": row['count']} for row in cursor.fetchall()]

stats_data = {
    'total_jobs': total_jobs,
    'total_companies': total_companies,
    'total_provinces': total_provinces,
    'total_quota': total_quota,
    'total_registered': total_registered,
    'top_companies': top_companies,
    'categories': categories,
    'generated_at': datetime.now().isoformat()
}

stats_path = os.path.join(output_dir, 'stats.json')
with open(stats_path, 'w', encoding='utf-8') as f:
    json.dump(stats_data, f, ensure_ascii=False)

print(f"✅ Created statistics file")

conn.close()

print("\n" + "="*60)
print("🎯 STATIC DATA GENERATION COMPLETE!")
print("="*60)
print(f"📁 Output directory: {output_dir}")
print(f"📊 Total files generated: {len(all_jobs) + 5}")
print(f"📦 Total data size: {sum(os.path.getsize(os.path.join(dirpath, filename)) for dirpath, dirnames, filenames in os.walk(output_dir) for filename in filenames) / 1024 / 1024:.2f} MB")
print("\n✅ Ready for frontend development!")
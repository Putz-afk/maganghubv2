import requests
import json
import sqlite3
import os
from datetime import datetime

print("🔍 Starting MagangHub Simple Scraper")
print("=" * 50)

# Database path
db_path = os.path.join('..', 'backend', 'data.db')
print(f"Database path: {db_path}")
print(f"Database exists: {os.path.exists(db_path)}")

def parse_program_studi(program_studi_json):
    """Convert JSON string to readable format"""
    try:
        if not program_studi_json:
            return ""
        programs = json.loads(program_studi_json)
        if isinstance(programs, list):
            titles = [item.get('title', '') for item in programs if 'title' in item]
            return ", ".join(titles)
        return ""
    except:
        return program_studi_json

def save_job_to_db(job_data):
    """Save a single job to database"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Extract data
        perusahaan = job_data.get('perusahaan', {})
        jadwal = job_data.get('jadwal', {})
        
        # Parse program studi
        program_studi = parse_program_studi(job_data.get('program_studi', ''))
        
        # Parse jenjang (education level)
        jenjang = ""
        try:
            jenjang_json = job_data.get('jenjang', '')
            if jenjang_json:
                jenjang_list = json.loads(jenjang_json)
                if isinstance(jenjang_list, list):
                    jenjang = ", ".join(jenjang_list)
        except:
            pass
        
        # Combine program studi and jenjang
        recommended_degree = f"{program_studi}"
        if jenjang:
            recommended_degree += f" ({jenjang})"
        
        # Extract deadline date only
        deadline = jadwal.get('tanggal_batas_pendaftaran', '')
        if deadline:
            deadline = deadline.split()[0]  # Get only date part
        
        # Insert job
        cursor.execute('''
            INSERT OR REPLACE INTO jobs (
                source_id, company_name, province, city, recommended_degree,
                job_title, job_description, application_deadline,
                quota, registered, position_type, government_agency,
                sub_government_agency, angkatan, tahun, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            job_data['id_posisi'],
            perusahaan.get('nama_perusahaan', ''),
            perusahaan.get('nama_provinsi', ''),
            perusahaan.get('nama_kabupaten', ''),
            recommended_degree.strip(),
            job_data['posisi'],
            job_data.get('deskripsi_posisi', ''),
            deadline,
            job_data.get('jumlah_kuota', 1),
            job_data.get('jumlah_terdaftar', 0),
            job_data['posisi'],
            job_data.get('government_agency', {}).get('government_agency_name', ''),
            job_data.get('sub_government_agency', {}).get('sub_government_agency_name', ''),
            jadwal.get('angkatan', ''),
            jadwal.get('tahun', ''),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        conn.commit()
        return True
        
    except sqlite3.Error as e:
        print(f"  ❌ Database error: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        print(f"  ❌ Unexpected error: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def fetch_jobs_from_api(province_code="73", limit=5):
    """Fetch jobs from API"""
    print(f"\n📥 Fetching {limit} jobs from province {province_code}...")
    
    url = "https://maganghub.kemnaker.go.id/be/v1/api/list/vacancies-aktif"
    params = {
        'order_by': 'jumlah_terdaftar',
        'order_direction': 'ASC',
        'page': 1,
        'limit': limit,
        'per_page': limit,
        'kode_provinsi': province_code
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        jobs = data.get('data', [])
        
        print(f"✅ Fetched {len(jobs)} jobs from API")
        return jobs
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch jobs: {e}")
        return []

def show_database_stats():
    """Show current database statistics"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Count total jobs
        cursor.execute("SELECT COUNT(*) FROM jobs")
        total_jobs = cursor.fetchone()[0]
        
        # Count jobs by province
        cursor.execute("SELECT province, COUNT(*) FROM jobs GROUP BY province")
        by_province = cursor.fetchall()
        
        print(f"\n📊 Database Statistics:")
        print(f"  Total jobs: {total_jobs}")
        
        if by_province:
            print(f"  Jobs by province:")
            for province, count in by_province:
                print(f"    - {province}: {count}")
        
        # Show latest jobs
        cursor.execute('''
            SELECT job_title, company_name, province 
            FROM jobs 
            ORDER BY id DESC 
            LIMIT 3
        ''')
        latest_jobs = cursor.fetchall()
        
        print(f"\n  Latest jobs added:")
        for title, company, province in latest_jobs:
            print(f"    • {title[:40]}... - {company} ({province})")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"  ❌ Error getting stats: {e}")

def main():
    """Main function"""
    print("\n1. Testing database connection...")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM jobs")
        count = cursor.fetchone()[0]
        conn.close()
        print(f"   Current jobs in database: {count}")
    except:
        print("   ❌ Cannot connect to database")
        return
    
    print("\n2. Fetching jobs from API...")
    jobs = fetch_jobs_from_api(province_code="73", limit=5)
    
    if not jobs:
        print("   ❌ No jobs fetched, stopping.")
        return
    
    print("\n3. Saving jobs to database...")
    saved_count = 0
    for i, job in enumerate(jobs, 1):
        job_title = job.get('posisi', 'Unknown')[:40]
        print(f"   {i}. Processing: {job_title}...")
        
        if save_job_to_db(job):
            saved_count += 1
            print(f"     ✅ Saved successfully")
        else:
            print(f"     ❌ Failed to save")
    
    print(f"\n✅ Saved {saved_count} out of {len(jobs)} jobs")
    
    print("\n4. Showing updated database stats...")
    show_database_stats()
    
    print("\n" + "=" * 50)
    print("✨ Simple scraper completed successfully!")

if __name__ == "__main__":
    main()
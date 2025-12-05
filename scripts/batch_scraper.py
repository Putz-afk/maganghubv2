import requests
import json
import sqlite3
import os
import time
from datetime import datetime

class BatchScraper:
    def __init__(self):
        self.base_url = "https://maganghub.kemnaker.go.id/be/v1/api/list/vacancies-aktif"
        self.db_path = os.path.join('..', 'backend', 'data.db')
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        print("Batch Scraper initialized.")
        print(f"Database: {self.db_path}")
    
    def parse_program_studi(self, program_studi_json):
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
    
    def parse_jenjang(self, jenjang_json):
        """Convert JSON string to readable format"""
        try:
            if not jenjang_json:
                return ""
            jenjang_list = json.loads(jenjang_json)
            if isinstance(jenjang_list, list):
                return ", ".join(jenjang_list)
            return ""
        except:
            return jenjang_json
    
    def save_job(self, job_data):
        """Save a single job to database"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            perusahaan = job_data.get('perusahaan', {})
            jadwal = job_data.get('jadwal', {})
            government_agency = job_data.get('government_agency', {})
            sub_government_agency = job_data.get('sub_government_agency', {})
            
            program_studi = self.parse_program_studi(job_data.get('program_studi', ''))
            jenjang = self.parse_jenjang(job_data.get('jenjang', ''))
            
            recommended_degree = f"{program_studi}"
            if jenjang:
                recommended_degree += f" ({jenjang})"
            
            deadline = jadwal.get('tanggal_batas_pendaftaran', '')
            if deadline:
                deadline = deadline.split()[0]
            
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
                government_agency.get('government_agency_name', ''),
                sub_government_agency.get('sub_government_agency_name', ''),
                jadwal.get('angkatan', ''),
                jadwal.get('tahun', ''),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            conn.commit()
            return True
            
        except sqlite3.Error as e:
            print(f"  Database error: {e}")
            if conn:
                conn.rollback()
            return False
        except Exception as e:
            print(f"  Unexpected error: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def fetch_page(self, province_code="73", page=1, limit=100):
        """Fetch a single page of jobs"""
        params = {
            'order_by': 'jumlah_terdaftar',
            'order_direction': 'ASC',
            'page': page,
            'limit': limit,
            'per_page': limit,
            'kode_provinsi': province_code
        }
        
        try:
            response = self.session.get(self.base_url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching page {page}: {e}")
            return None
    
    def scrape_province(self, province_code="73", province_name="SULAWESI SELATAN", max_pages=2):
        """Scrape multiple pages from a province"""
        print(f"\n📥 Scraping {province_name} (Code: {province_code})...")
        
        total_saved = 0
        page = 1
        
        while page <= max_pages:
            print(f"  Page {page}...")
            
            data = self.fetch_page(province_code, page, limit=100)
            if not data or 'data' not in data:
                print(f"  No data on page {page}")
                break
            
            jobs = data.get('data', [])
            if not jobs:
                print(f"  No jobs on page {page}")
                break
            
            # Save jobs
            saved_in_page = 0
            for job in jobs:
                if self.save_job(job):
                    saved_in_page += 1
            
            total_saved += saved_in_page
            print(f"  Saved {saved_in_page} jobs from page {page}")
            
            # Check pagination
            meta = data.get('meta', {})
            pagination = meta.get('pagination', {})
            if pagination.get('current_page', 0) >= pagination.get('last_page', 0):
                print(f"  Reached last page for {province_name}")
                break
            
            page += 1
            time.sleep(1)  # Be respectful
            
        print(f"  ✅ Total saved for {province_name}: {total_saved}")
        return total_saved
    
    def show_stats(self):
        """Show current database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM jobs")
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT province, COUNT(*) FROM jobs GROUP BY province ORDER BY COUNT(*) DESC")
            by_province = cursor.fetchall()
            
            cursor.execute("SELECT COUNT(DISTINCT company_name) FROM jobs")
            unique_companies = cursor.fetchone()[0]
            
            print("\n" + "="*50)
            print("📊 DATABASE STATISTICS")
            print("="*50)
            print(f"Total jobs: {total}")
            print(f"Unique companies: {unique_companies}")
            
            print("\nJobs by province:")
            for province, count in by_province:
                print(f"  {province}: {count}")
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"Error getting stats: {e}")

def main():
    print("="*60)
    print("MAGANGHUB BATCH SCRAPER")
    print("="*60)
    
    scraper = BatchScraper()
    
    # Show current stats
    scraper.show_stats()
    
    # Ask what to do
    print("\nOptions:")
    print("1. Scrape more jobs from Sulawesi Selatan (2 pages)")
    print("2. Scrape from DKI Jakarta (province 31)")
    print("3. Show current stats")
    print("4. Exit")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == "1":
        scraper.scrape_province("73", "SULAWESI SELATAN", max_pages=2)
        scraper.show_stats()
    elif choice == "2":
        scraper.scrape_province("31", "DKI JAKARTA", max_pages=2)
        scraper.show_stats()
    elif choice == "3":
        scraper.show_stats()
    else:
        print("Exiting.")

if __name__ == "__main__":
    main()
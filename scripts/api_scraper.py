import sqlite3
import requests
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

class MagangHubAPIScraper:
    def __init__(self, db_path: str = "../backend/data.db"):
        self.base_url = "https://maganghub.kemnaker.go.id/be/v1/api/list/vacancies-aktif"
        self.db_path = Path(db_path)
        self.session = requests.Session()
        
        # Set headers to mimic browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://maganghub.kemnaker.go.id/'
        }
    
    def fetch_page(self, page: int = 1, per_page: int = 100, province_code: Optional[str] = None):
        """Fetch a single page from the API"""
        params = {
            'order_by': 'jumlah_terdaftar',
            'order_direction': 'ASC',
            'page': page,
            'limit': per_page,
            'per_page': per_page
        }
        
        if province_code:
            params['kode_provinsi'] = province_code
        
        try:
            response = self.session.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            return None
    
    def parse_program_studi(self, program_studi_json: str) -> str:
        """Parse the program_studi JSON array into a string"""
        try:
            if not program_studi_json:
                return ""
            
            programs = json.loads(program_studi_json)
            if isinstance(programs, list):
                # Extract titles from each object
                titles = [item.get('title', '') for item in programs if 'title' in item]
                return ", ".join(titles)
            return ""
        except json.JSONDecodeError:
            return program_studi_json
    
    def parse_jenjang(self, jenjang_json: str) -> str:
        """Parse the jenjang JSON array into a string"""
        try:
            if not jenjang_json:
                return ""
            
            jenjang_list = json.loads(jenjang_json)
            if isinstance(jenjang_list, list):
                return ", ".join(jenjang_list)
            return ""
        except json.JSONDecodeError:
            return jenjang_json
    
    def save_job_to_db(self, conn, job_data: Dict):
        """Save a single job to database"""
        try:
            # Parse nested fields
            perusahaan = job_data.get('perusahaan', {})
            jadwal = job_data.get('jadwal', {})
            government_agency = job_data.get('government_agency', {})
            sub_government_agency = job_data.get('sub_government_agency', {})
            
            # Parse program studi and jenjang
            program_studi = self.parse_program_studi(job_data.get('program_studi', ''))
            jenjang = self.parse_jenjang(job_data.get('jenjang', ''))
            
            # Combine program studi and jenjang for recommended_degree
            recommended_degree = f"{program_studi} ({jenjang})".strip()
            if recommended_degree == "()":
                recommended_degree = ""
            
            # Extract deadline - use only date part
            deadline = jadwal.get('tanggal_batas_pendaftaran')
            if deadline:
                deadline = deadline.split()[0]  # Get only YYYY-MM-DD
            
            conn.execute('''
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
                recommended_degree,
                job_data['posisi'],
                job_data.get('deskripsi_posisi', ''),
                deadline,
                job_data.get('jumlah_kuota', 1),
                job_data.get('jumlah_terdaftar', 0),
                job_data['posisi'],  # position_type (same as title for now)
                government_agency.get('government_agency_name', ''),
                sub_government_agency.get('sub_government_agency_name', ''),
                jadwal.get('angkatan', ''),
                jadwal.get('tahun', ''),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            return True
            
        except Exception as e:
            print(f"Error saving job {job_data.get('id_posisi')}: {e}")
            return False
    
    def scrape_all_provinces(self, max_pages: int = 1000, delay: float = 1.0):
        """Scrape all jobs from all provinces"""
        conn = sqlite3.connect(self.db_path)
        
        total_jobs = 0
        current_page = 1
        
        while current_page <= max_pages:
            print(f"Fetching page {current_page}...")
            
            data = self.fetch_page(page=current_page, per_page=100)
            
            if not data or 'data' not in data:
                print(f"No data on page {current_page}, stopping.")
                break
            
            jobs = data['data']
            if not jobs:
                print(f"No jobs on page {current_page}, stopping.")
                break
            
            # Save each job
            for job in jobs:
                if self.save_job_to_db(conn, job):
                    total_jobs += 1
            
            # Check pagination
            meta = data.get('meta', {})
            pagination = meta.get('pagination', {})
            
            print(f"Page {current_page}: Saved {len(jobs)} jobs (Total: {total_jobs})")
            
            # Check if there are more pages
            if pagination.get('current_page', 0) >= pagination.get('last_page', 0):
                print("Reached last page.")
                break
            
            current_page += 1
            
            # Be respectful - delay between requests
            time.sleep(delay)
        
        conn.commit()
        conn.close()
        print(f"Scraping complete! Saved {total_jobs} jobs.")
    
    def scrape_by_province(self, province_code: str, province_name: str, max_pages: int = 50):
        """Scrape jobs for a specific province"""
        conn = sqlite3.connect(self.db_path)
        
        total_jobs = 0
        current_page = 1
        
        print(f"Scraping jobs for {province_name} (Code: {province_code})...")
        
        while current_page <= max_pages:
            print(f"  Page {current_page}...")
            
            data = self.fetch_page(page=current_page, per_page=100, province_code=province_code)
            
            if not data or 'data' not in data:
                print(f"  No data on page {current_page}, stopping.")
                break
            
            jobs = data['data']
            if not jobs:
                print(f"  No jobs on page {current_page}, stopping.")
                break
            
            # Save each job
            for job in jobs:
                if self.save_job_to_db(conn, job):
                    total_jobs += 1
            
            print(f"  Page {current_page}: Saved {len(jobs)} jobs (Total: {total_jobs})")
            
            # Check pagination
            meta = data.get('meta', {})
            pagination = meta.get('pagination', {})
            
            if pagination.get('current_page', 0) >= pagination.get('last_page', 0):
                print(f"  Reached last page for {province_name}.")
                break
            
            current_page += 1
            time.sleep(1.5)  # Respectful delay for same province
        
        conn.commit()
        conn.close()
        print(f"  Completed {province_name}: {total_jobs} jobs")
        return total_jobs
    
    def get_province_list(self):
        """Get list of provinces (you might need to scrape this from the main page)"""
        # You can manually create this list from observing the website
        provinces = [
            ("11", "ACEH"),
            ("12", "SUMATERA UTARA"),
            ("13", "SUMATERA BARAT"),
            ("14", "RIAU"),
            ("15", "JAMBI"),
            ("16", "SUMATERA SELATAN"),
            ("17", "BENGKULU"),
            ("18", "LAMPUNG"),
            ("19", "KEPULAUAN BANGKA BELITUNG"),
            ("21", "KEPULAUAN RIAU"),
            ("31", "DKI JAKARTA"),
            ("32", "JAWA BARAT"),
            ("33", "JAWA TENGAH"),
            ("34", "DI YOGYAKARTA"),
            ("35", "JAWA TIMUR"),
            ("36", "BANTEN"),
            ("51", "BALI"),
            ("52", "NUSA TENGGARA BARAT"),
            ("53", "NUSA TENGGARA TIMUR"),
            ("61", "KALIMANTAN BARAT"),
            ("62", "KALIMANTAN TENGAH"),
            ("63", "KALIMANTAN SELATAN"),
            ("64", "KALIMANTAN TIMUR"),
            ("65", "KALIMANTAN UTARA"),
            ("71", "SULAWESI UTARA"),
            ("72", "SULAWESI TENGAH"),
            ("73", "SULAWESI SELATAN"),
            ("74", "SULAWESI TENGGARA"),
            ("75", "GORONTALO"),
            ("76", "SULAWESI BARAT"),
            ("81", "MALUKU"),
            ("82", "MALUKU UTARA"),
            ("91", "PAPUA BARAT"),
            ("92", "PAPUA"),
            # Add more as needed
        ]
        return provinces
    
    def scrape_all_provinces_parallel(self):
        """Scrape all provinces one by one (not truly parallel to be respectful)"""
        provinces = self.get_province_list()
        total_all_jobs = 0
        
        for province_code, province_name in provinces:
            jobs_count = self.scrape_by_province(province_code, province_name)
            total_all_jobs += jobs_count
            print(f"--- Waiting 3 seconds before next province ---")
            time.sleep(3)
        
        print(f"Total jobs scraped from all provinces: {total_all_jobs}")
        return total_all_jobs

if __name__ == "__main__":
    scraper = MagangHubAPIScraper()
    
    # Choose your scraping strategy:
    
    # Option 1: Scrape ALL jobs (no province filter)
    # scraper.scrape_all_provinces(max_pages=1000, delay=2.0)
    
    # Option 2: Scrape by province (more controlled)
    scraper.scrape_all_provinces_parallel()
    
    # Option 3: Test with single province
    # scraper.scrape_by_province("73", "SULAWESI SELATAN")
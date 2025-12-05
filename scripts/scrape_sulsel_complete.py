import requests
import json
import sqlite3
import os
import time
from datetime import datetime

print("="*60)
print("COMPLETE SULAWESI SELATAN SCRAPER")
print("="*60)

class CompleteScraper:
    def __init__(self):
        self.base_url = "https://maganghub.kemnaker.go.id/be/v1/api/list/vacancies-aktif"
        self.db_path = os.path.join('..', 'backend', 'data.db')
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        
        print(f"Database: {self.db_path}")
        print(f"Database exists: {os.path.exists(self.db_path)}")
    
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
        except (json.JSONDecodeError, TypeError):
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
        except (json.JSONDecodeError, TypeError):
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
    
    def get_total_pages(self, province_code="73"):
        """Get total number of pages for a province"""
        print("\n📊 Checking total pages...")
        
        # Fetch just 1 item to get pagination info
        url = self.base_url
        params = {
            'order_by': 'jumlah_terdaftar',
            'order_direction': 'ASC',
            'page': 1,
            'limit': 1,
            'per_page': 1,
            'kode_provinsi': province_code
        }
        
        try:
            response = self.session.get(url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if 'meta' in data and 'pagination' in data['meta']:
                pagination = data['meta']['pagination']
                total_jobs = pagination.get('total', 0)
                per_page = pagination.get('per_page', 100)
                
                # Calculate total pages (ceil division)
                total_pages = (total_jobs + per_page - 1) // per_page
                
                print(f"Total jobs in province {province_code}: {total_jobs}")
                print(f"Jobs per page: {per_page}")
                print(f"Total pages needed: {total_pages}")
                
                return total_pages, total_jobs
            else:
                print("Could not get pagination info")
                return 1, 0
                
        except Exception as e:
            print(f"Error getting page count: {e}")
            return 1, 0
    
    def scrape_all_pages(self, province_code="73", province_name="SULAWESI SELATAN"):
        """Scrape ALL pages from a province"""
        print(f"\n🎯 Starting complete scrape for {province_name}")
        print("="*50)
        
        # First, get total pages
        total_pages, total_jobs = self.get_total_pages(province_code)
        
        if total_jobs == 0:
            print("No jobs found for this province.")
            return 0
        
        print(f"\n📥 Will scrape {total_pages} pages ({total_jobs} jobs total)")
        print("This will take several minutes. Please wait...")
        
        total_saved = 0
        page = 1
        
        while page <= total_pages:
            print(f"\n📄 Page {page}/{total_pages}...")
            
            # Fetch page
            params = {
                'order_by': 'jumlah_terdaftar',
                'order_direction': 'ASC',
                'page': page,
                'limit': 100,
                'per_page': 100,
                'kode_provinsi': province_code
            }
            
            try:
                response = self.session.get(self.base_url, params=params, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                jobs = data.get('data', [])
                
                print(f"  Fetched {len(jobs)} jobs")
                
                # Save jobs
                saved_in_page = 0
                for i, job in enumerate(jobs, 1):
                    if self.save_job(job):
                        saved_in_page += 1
                    
                    # Show progress every 10 jobs
                    if i % 20 == 0:
                        print(f"    Processed {i}/{len(jobs)} jobs")
                
                total_saved += saved_in_page
                
                # Show progress
                progress = (page / total_pages) * 100
                print(f"  ✅ Saved {saved_in_page} jobs from this page")
                print(f"  📈 Progress: {progress:.1f}% ({page}/{total_pages} pages)")
                print(f"  🏆 Total saved so far: {total_saved}/{total_jobs}")
                
                # Check if we've reached the end
                if 'meta' in data and 'pagination' in data['meta']:
                    pagination = data['meta']['pagination']
                    if pagination.get('current_page', 0) >= pagination.get('last_page', 0):
                        print(f"\n  🎯 Reached last page!")
                        break
                
                page += 1
                
                # Be respectful - wait between requests
                if page <= total_pages:
                    print(f"  ⏳ Waiting 2 seconds before next page...")
                    time.sleep(2)
                
            except Exception as e:
                print(f"  ❌ Error on page {page}: {e}")
                print(f"  ⏳ Waiting 5 seconds before retry...")
                time.sleep(5)
                
                # Try one more time
                print(f"  🔄 Retrying page {page}...")
                try:
                    response = self.session.get(self.base_url, params=params, headers=self.headers, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    jobs = data.get('data', [])
                    
                    print(f"  Retry successful! Fetched {len(jobs)} jobs")
                    
                    # Save jobs from retry
                    saved_in_page = 0
                    for i, job in enumerate(jobs, 1):
                        if self.save_job(job):
                            saved_in_page += 1
                    
                    total_saved += saved_in_page
                    print(f"  ✅ Saved {saved_in_page} jobs from retry")
                    
                except Exception as retry_error:
                    print(f"  ❌ Retry failed: {retry_error}")
                    print(f"  ⏭️  Skipping page {page}")
                
                page += 1
        
        print(f"\n✅ COMPLETE! Saved {total_saved} out of {total_jobs} jobs from {province_name}")
        return total_saved
    
    def show_detailed_stats(self):
        """Show detailed database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Basic counts
            cursor.execute("SELECT COUNT(*) as total_jobs FROM jobs")
            total_jobs = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT company_name) as total_companies FROM jobs")
            total_companies = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT province) as total_provinces FROM jobs")
            total_provinces = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(quota) as total_quota, SUM(registered) as total_registered FROM jobs")
            sums = cursor.fetchone()
            total_quota = sums[0] or 0
            total_registered = sums[1] or 0
            
            print("\n" + "="*60)
            print("📊 DETAILED DATABASE STATISTICS")
            print("="*60)
            print(f"Total jobs: {total_jobs}")
            print(f"Total companies: {total_companies}")
            print(f"Total provinces: {total_provinces}")
            print(f"Total quota (positions available): {total_quota}")
            print(f"Total registered (applicants): {total_registered}")
            print(f"Application rate: {total_registered}/{total_quota} = {(total_registered/total_quota*100 if total_quota > 0 else 0):.1f}%")
            
            # Jobs by province
            cursor.execute("""
                SELECT province, COUNT(*) as count 
                FROM jobs 
                GROUP BY province 
                ORDER BY count DESC
            """)
            provinces = cursor.fetchall()
            
            print(f"\nJobs by province:")
            for province, count in provinces:
                print(f"  {province}: {count} jobs")
            
            # Top companies
            cursor.execute("""
                SELECT company_name, COUNT(*) as job_count 
                FROM jobs 
                GROUP BY company_name 
                ORDER BY job_count DESC 
                LIMIT 10
            """)
            top_companies = cursor.fetchall()
            
            print(f"\nTop 10 companies with most jobs:")
            for i, (company, count) in enumerate(top_companies, 1):
                print(f"  {i}. {company}: {count} jobs")
            
            # Job title categories
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN job_title LIKE '%guru%' OR job_title LIKE '%teacher%' OR job_title LIKE '%pendidik%' THEN 'Education'
                        WHEN job_title LIKE '%dokter%' OR job_title LIKE '%perawat%' OR job_title LIKE '%medis%' THEN 'Medical'
                        WHEN job_title LIKE '%teknik%' OR job_title LIKE '%engineer%' OR job_title LIKE '%teknisi%' THEN 'Engineering'
                        WHEN job_title LIKE '%administrasi%' OR job_title LIKE '%admin%' OR job_title LIKE '%tata usaha%' THEN 'Administration'
                        WHEN job_title LIKE '%komputer%' OR job_title LIKE '%IT%' OR job_title LIKE '%programmer%' THEN 'IT/Technology'
                        ELSE 'Other'
                    END as category,
                    COUNT(*) as count
                FROM jobs
                GROUP BY category
                ORDER BY count DESC
            """)
            categories = cursor.fetchall()
            
            print(f"\nJobs by category:")
            for category, count in categories:
                print(f"  {category}: {count} jobs")
            
            conn.close()
            
        except sqlite3.Error as e:
            print(f"Error getting stats: {e}")

def main():
    scraper = CompleteScraper()
    
    # Show current stats
    print("\n📈 CURRENT DATABASE STATUS:")
    scraper.show_detailed_stats()
    
    # Ask for confirmation
    print("\n" + "="*60)
    print("⚠️  WARNING: This will scrape ALL jobs from Sulawesi Selatan")
    print("It will take several minutes (5-10 minutes).")
    print("="*60)
    
    confirm = input("\nDo you want to continue? (yes/no): ").strip().lower()
    
    if confirm in ['yes', 'y']:
        # Scrape all pages
        scraper.scrape_all_pages("73", "SULAWESI SELATAN")
        
        # Show final stats
        scraper.show_detailed_stats()
        
        print("\n✅ All done! You can now start the backend API with:")
        print("   cd ..\\backend")
        print("   python main_improved.py")
    else:
        print("Scraping cancelled.")

if __name__ == "__main__":
    main()
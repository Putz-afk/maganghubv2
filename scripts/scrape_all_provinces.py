#!/usr/bin/env python3
"""
All-Province Job Scraper with Auto-Update every 5 hours
Scrapes from MagangHub API for all Indonesian provinces
"""

import requests
import json
import sqlite3
import os
import time
import subprocess
import sys
import math
from datetime import datetime
from pathlib import Path
try:
    import schedule
    SCHEDULE_AVAILABLE = True
except ImportError:
    SCHEDULE_AVAILABLE = False

from concurrent.futures import ThreadPoolExecutor, as_completed


class AllProvinceScraper:
    def __init__(self):
        self.base_url = "https://maganghub.kemnaker.go.id/be/v1/api/list/vacancies-aktif"
        # Resolve database path relative to script location
        script_dir = Path(__file__).parent.absolute()
        self.db_path = script_dir.parent / 'backend' / 'data.db'
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
            # Don't set Accept-Encoding - requests handles decompression automatically
        }
        
        # Ensure database directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database if needed
        self._ensure_database_initialized()
        
        # All Indonesian province codes (kode_provinsi)
        self.provinces = {
            # '11': 'ACEH',
            # '12': 'SUMATERA UTARA',
            # '13': 'SUMATERA BARAT',
            # '14': 'RIAU',
            # '15': 'JAMBI',
            # '16': 'SUMATERA SELATAN',
            # '17': 'BENGKULU',
            # '18': 'LAMPUNG',
            # '19': 'KEPULAUAN BANGKA BELITUNG',
            # '21': 'KEPULAUAN RIAU',
            '31': 'JAWA BARAT',
            '32': 'JAWA TENGAH',
            '33': 'DAERAH ISTIMEWA YOGYAKARTA',
            '34': 'JAWA TIMUR',
            '35': 'BANTEN',
            '36': 'BALI',
            # '51': 'NUSA TENGGARA BARAT',
            # '52': 'NUSA TENGGARA TIMUR',
            # '61': 'KALIMANTAN BARAT',
            # '62': 'KALIMANTAN TENGAH',
            # '63': 'KALIMANTAN SELATAN',
            # '64': 'KALIMANTAN TIMUR',
            # '65': 'KALIMANTAN UTARA',
            '71': 'SULAWESI UTARA',
            '72': 'SULAWESI TENGAH',
            '73': 'SULAWESI SELATAN',
            '74': 'SULAWESI TENGGARA',
            '75': 'GORONTALO',
            '76': 'SULAWESI BARAT',
            # '81': 'MALUKU',
            # '82': 'MALUKU UTARA',
            # '91': 'PAPUA BARAT',
            # '94': 'PAPUA',
        }
        
        print(f"Database: {self.db_path}")
        print(f"Will scrape {len(self.provinces)} provinces")
    
    def _ensure_database_initialized(self):
        """Ensure database table exists"""
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            # Check if jobs table exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='jobs'
            """)
            if not cursor.fetchone():
                # Create jobs table if it doesn't exist
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_id TEXT UNIQUE,
                        company_name TEXT NOT NULL,
                        province TEXT,
                        city TEXT,
                        recommended_degree TEXT,
                        job_title TEXT NOT NULL,
                        job_description TEXT,
                        application_deadline DATE,
                        quota INTEGER DEFAULT 1,
                        registered INTEGER DEFAULT 0,
                        position_type TEXT,
                        government_agency TEXT,
                        sub_government_agency TEXT,
                        angkatan TEXT,
                        tahun TEXT,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        categorized_at TIMESTAMP,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
                print("✅ Created jobs table")
            conn.close()
        except sqlite3.Error as e:
            print(f"⚠️  Warning: Could not initialize database: {e}")
    
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
            # Validate required fields
            if 'id_posisi' not in job_data:
                print(f"⚠️  Warning: Missing 'id_posisi' in job data, skipping")
                return False
            if 'posisi' not in job_data:
                print(f"⚠️  Warning: Missing 'posisi' in job data (ID: {job_data.get('id_posisi', 'unknown')}), skipping")
                return False
            
            conn = sqlite3.connect(str(self.db_path))
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
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def _prepare_connection(self) -> sqlite3.Connection:
        """Create a SQLite connection tuned for faster bulk inserts."""
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA mmap_size=30000000000;")
        except sqlite3.Error:
            pass
        return conn

    def _insert_jobs_batch(self, cursor, jobs):
        """Batch-insert jobs using a single prepared statement within an open transaction."""
        if not jobs:
            return 0

        saved = 0
        for job_data in jobs:
            try:
                if 'id_posisi' not in job_data or 'posisi' not in job_data:
                    continue

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
                saved += 1
            except sqlite3.Error:
                continue
        return saved
    
    def _create_session(self):
        """Create a new session for thread-safe requests"""
        session = requests.Session()
        # Don't set Accept-Encoding in headers - let requests handle it automatically
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        })
        return session
    
    def get_total_pages(self, province_code, session=None, retries=3):
        """Get total number of pages for a province with retry logic"""
        if session is None:
            session = self._create_session()
            
        params = {
            'order_by': 'jumlah_terdaftar',
            'order_direction': 'ASC',
            'page': 1,
            'limit': 1,
            'per_page': 1,
            'kode_provinsi': province_code
        }
        
        for attempt in range(retries):
            try:
                response = session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                # requests automatically decompresses gzip/deflate/br
                # Just parse JSON directly - don't check response.text first
                try:
                    data = response.json()
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    if attempt < retries - 1:
                        time.sleep(1 * (attempt + 1))
                        continue
                    print(f"  ⚠️  JSON decode error for province {province_code}: {e}")
                    print(f"  Status: {response.status_code}, Content-Encoding: {response.headers.get('Content-Encoding', 'none')}")
                    return 1, 0, session
                
                if 'meta' in data and 'pagination' in data['meta']:
                    pagination = data['meta']['pagination']
                    total_jobs = pagination.get('total', 0)
                    # When checking with per_page=1, we get per_page=1 back, but we'll use 100 when scraping
                    per_page = 100  # Always use 100 for actual scraping
                    total_pages = (total_jobs + per_page - 1) // per_page if total_jobs > 0 else 0
                    if total_jobs == 0 and attempt == retries - 1:
                        # Debug: see what we got
                        print(f"  ⚠️  Province {province_code} returned 0 jobs. Response keys: {list(data.keys())}")
                        if 'meta' in data:
                            print(f"  Meta keys: {list(data['meta'].keys())}")
                    return total_pages, total_jobs, session
                else:
                    # Debug: print what we got
                    print(f"  ⚠️  No pagination data for province {province_code}, keys: {list(data.keys())}")
                    if 'meta' in data:
                        print(f"  Meta content: {data['meta']}")
                    return 1, 0, session
                    
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(1 * (attempt + 1))
                    continue
                # Only print error on final attempt
                print(f"  ⚠️  Error getting page count for province {province_code}: {type(e).__name__}: {e}")
                return 1, 0, session
    
    def scrape_province(self, province_code, province_name):
        """Scrape all pages from a single province"""
        print(f"\n🎯 Scraping {province_name} (Code: {province_code})")
        
        # Create a separate session for this thread
        session = self._create_session()
        total_pages, total_jobs, session = self.get_total_pages(province_code, session)
        
        if total_jobs == 0:
            print(f"  ℹ️  No jobs found")
            session.close()
            return 0
        
        print(f"  📥 {total_pages} pages ({total_jobs} jobs)")
        
        total_saved = 0
        page = 1

        # Single connection per province + explicit transactions
        conn = self._prepare_connection()
        cursor = conn.cursor()
        
        while page <= total_pages:
            params = {
                'order_by': 'jumlah_terdaftar',
                'order_direction': 'ASC',
                'page': page,
                'limit': 100,
                'per_page': 100,
                'kode_provinsi': province_code
            }
            
            try:
                response = session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                # requests automatically decompresses - just parse JSON directly
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    print(f"  ⚠️  Invalid JSON on page {page}: {e}, retrying...")
                    time.sleep(2)
                    continue
                
                jobs = data.get('data', [])
                
                # Batch insert in a transaction
                conn.execute("BEGIN")
                saved_in_page = self._insert_jobs_batch(cursor, jobs)
                conn.commit()
                
                total_saved += saved_in_page
                page += 1
                
                if page <= total_pages:
                    time.sleep(0.5)  # Slightly longer delay to be more respectful
                
            except Exception as e:
                print(f"  ⚠️  Error on page {page}: {e}")
                time.sleep(2)
                
                try:
                    response = session.get(self.base_url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    jobs = data.get('data', [])
                    conn.execute("BEGIN")
                    saved_in_page = self._insert_jobs_batch(cursor, jobs)
                    conn.commit()
                    total_saved += saved_in_page
                    page += 1
                except Exception as retry_error:
                    print(f"  Retry failed: {retry_error}")
                    page += 1

        # Close database connection
        try:
            conn.close()
        except Exception:
            pass
        
        # Close session
        try:
            session.close()
        except Exception:
            pass
        
        print(f"  ✅ Saved {total_saved} jobs from {province_name}")
        return total_saved
    
    def scrape_all_provinces(self):
        """Scrape all provinces"""
        print("\n" + "="*60)
        print(f"🌍 SCRAPING ALL PROVINCES")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        total_jobs_saved = 0

        # Try sequential first to avoid API blocking, then we can add parallel back
        # Limit concurrency to be polite to the API. Reduced to 3 to avoid rate limiting.
        use_parallel = False  # Set to True to enable parallel scraping
        
        if use_parallel:
            max_workers = 3
            futures = {}
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for code, name in sorted(self.provinces.items()):
                    futures[executor.submit(self.scrape_province, code, name)] = (code, name)

                for future in as_completed(futures):
                    code, name = futures[future]
                    try:
                        saved = future.result()
                        total_jobs_saved += saved
                    except Exception as e:
                        print(f"❌ Province {name} ({code}) failed: {e}")
        else:
            # Sequential scraping - safer but slower
            for code, name in sorted(self.provinces.items()):
                try:
                    saved = self.scrape_province(code, name)
                    total_jobs_saved += saved
                except Exception as e:
                    print(f"❌ Province {name} ({code}) failed: {e}")
        
        print("\n" + "="*60)
        print(f"✅ ALL PROVINCES SCRAPED")
        print(f"Total jobs saved: {total_jobs_saved}")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
        return total_jobs_saved
    
    def show_stats(self):
        """Show database statistics"""
        try:
            conn = sqlite3.connect(str(self.db_path))
            c = conn.cursor()
            
            c.execute("SELECT COUNT(*) FROM jobs")
            total = c.fetchone()[0]
            
            c.execute("SELECT COUNT(DISTINCT province) FROM jobs")
            provinces = c.fetchone()[0]
            
            c.execute("SELECT AVG(registered) FROM jobs")
            avg_reg = c.fetchone()[0] or 0
            
            print(f"\n📊 Database Statistics:")
            print(f"   Total jobs: {total}")
            print(f"   Provinces: {provinces}")
            print(f"   Avg applicants: {avg_reg:.1f}")
            
            conn.close()
        except Exception as e:
            print(f"Error getting stats: {e}")

def update_workflow(skip_scrape=False):
    """Full update: scrape → deduplicate → generate → build
    
    Args:
        skip_scrape: If True, skip scraping and only run deduplicate → generate → build
    """
    print("\n" + "="*80)
    if skip_scrape:
        print("🔄 STARTING UPDATE WORKFLOW (SKIP SCRAPING)")
    else:
        print("🔄 STARTING UPDATE WORKFLOW")
    print("="*80)
    
    try:
        # Step 1: Scrape (optional)
        if not skip_scrape:
            scraper = AllProvinceScraper()
            scraper.scrape_all_provinces()
            scraper.show_stats()
        else:
            print("\n⏭️  Skipping scraping (using existing data)")
            # Show current stats
            scraper = AllProvinceScraper()
            scraper.show_stats()
        
        # Step 2: Deduplicate
        print("\n▶️  Deduplicating...")
        script_dir = Path(__file__).parent.absolute()
        project_root = script_dir.parent
        deduplicate_path = project_root / 'deduplicate.py'
        if deduplicate_path.exists():
            try:
                result = subprocess.run(
                    [sys.executable, str(deduplicate_path)],
                    cwd=str(project_root),
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    timeout=300
                )
                if result.returncode != 0:
                    print(f"⚠️  Deduplication warning: {result.stderr}")
                else:
                    print(result.stdout)
            except subprocess.TimeoutExpired:
                print("⚠️  Deduplication timed out")
            except Exception as e:
                print(f"⚠️  Deduplication error: {e}")
        else:
            print(f"⚠️  Warning: {deduplicate_path} not found, skipping deduplication")
        
        # Step 3: Generate static data
        print("\n▶️  Generating static data...")
        script_dir = Path(__file__).parent.absolute()
        generate_script = script_dir / 'generate_static_data.py'
        if generate_script.exists():
            try:
                result = subprocess.run(
                    [sys.executable, str(generate_script)],
                    cwd=str(script_dir),
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    timeout=600
                )
                if result.returncode != 0:
                    print(f"⚠️  Generate static data warning: {result.stderr}")
                else:
                    print(result.stdout)
            except subprocess.TimeoutExpired:
                print("⚠️  Generate static data timed out")
            except Exception as e:
                print(f"⚠️  Generate static data error: {e}")
        else:
            print(f"⚠️  Warning: {generate_script} not found, skipping static data generation")
        
        # Step 4: Build frontend
        print("\n▶️  Building frontend...")
        script_dir = Path(__file__).parent.absolute()
        frontend_dir = script_dir.parent / 'frontend'
        if frontend_dir.exists() and (frontend_dir / 'package.json').exists():
            try:
                result = subprocess.run(
                    ['npm', 'run', 'build'],
                    cwd=str(frontend_dir),
                    capture_output=True,
                    text=True,
                    timeout=600
                )
                if result.returncode != 0:
                    print(f"⚠️  Frontend build warning: {result.stderr}")
                else:
                    print("✅ Frontend build completed")
            except subprocess.TimeoutExpired:
                print("⚠️  Frontend build timed out")
            except FileNotFoundError:
                print("⚠️  Warning: npm not found, skipping frontend build")
            except Exception as e:
                print(f"⚠️  Frontend build error: {e}")
        else:
            print(f"⚠️  Warning: Frontend directory not found, skipping build")
        
        print("\n" + "="*80)
        print("✅ UPDATE WORKFLOW COMPLETE!")
        print(f"   Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
    except Exception as e:
        print(f"❌ Update workflow failed: {e}")

def scheduled_update():
    """Run update every 5 hours"""
    if not SCHEDULE_AVAILABLE:
        print("\n❌ ERROR: 'schedule' module not installed!")
        print("Install it with: pip install schedule")
        return
    
    print("\n" + "="*80)
    print("⏰ AUTO-UPDATE SCHEDULER STARTED")
    print("   Updates every 5 hours")
    print(f"   First update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Schedule update every 5 hours
    schedule.every(5).hours.do(update_workflow)
    
    # Run initial update
    update_workflow()
    
    # Keep scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute if update is needed

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--auto":
        # Run with auto-update every 5 hours
        try:
            scheduled_update()
        except KeyboardInterrupt:
            print("\n\n⏹️  Auto-update stopped by user")
    elif len(sys.argv) > 1 and sys.argv[1] == "--no-scrape":
        # Run without scraping (just deduplicate → generate → build)
        update_workflow(skip_scrape=True)
    else:
        # Run once (full workflow with scraping)
        update_workflow()

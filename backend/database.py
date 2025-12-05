def init_database():
    """Initialize database with tables"""
    conn = get_db_connection()
    
    # Create jobs table with new fields
    conn.execute('''
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
        
        -- NEW FIELDS from API
        quota INTEGER DEFAULT 1,
        registered INTEGER DEFAULT 0,
        position_type TEXT,  -- e.g., "Dokter", "PLP", etc.
        government_agency TEXT,
        sub_government_agency TEXT,
        angkatan TEXT,  -- Batch number
        tahun TEXT,     -- Year
        
        -- Timestamps
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        categorized_at TIMESTAMP,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create job_tags table (unchanged)
    conn.execute('''
    CREATE TABLE IF NOT EXISTS job_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        tag_type TEXT NOT NULL,
        tag_name TEXT NOT NULL,
        confidence_score FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    )
    ''')
    
    # Create indexes
    conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_province ON jobs(province)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_city ON jobs(province, city)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_deadline ON jobs(application_deadline)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_tags ON job_tags(tag_type, tag_name)')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully!")
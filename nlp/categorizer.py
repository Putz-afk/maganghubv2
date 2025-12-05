# Add to the JobCategorizer class

class JobCategorizer:
    def __init__(self, db_path: str = "../backend/data.db"):
        # ... existing code ...
        
        # Enhanced categories for Indonesian job market
        self.candidate_labels = [
            "backend-developer",
            "frontend-developer",
            "fullstack-developer",
            "data-analyst",
            "mobile-developer",
            "ui-ux-designer",
            "devops-engineer",
            "qa-engineer",
            "business-analyst",
            "digital-marketing",
            
            # Indonesian government/specialized roles
            "dokter",
            "perawat",
            "laboran",
            "guru",
            "dosen",
            "peneliti",
            "administrasi",
            "keuangan",
            "humas",
            "teknik",
            "arsitek",
            "pengawas",
            "konsultan"
        ]
        
        # Map Indonesian job titles to categories
        self.title_keywords = {
            "dokter": "dokter",
            "perawat": "perawat",
            "laboran": "laboran",
            "guru": "guru",
            "dosen": "dosen",
            "peneliti": "peneliti",
            "administrasi": "administrasi",
            "keuangan": "keuangan",
            "humas": "humas",
            "teknik": "teknik",
            "arsitek": "arsitek",
            "pengawas": "pengawas",
            "konsultan": "konsultan",
            
            # IT roles
            "programmer": "backend-developer",
            "developer": "backend-developer",
            "software": "backend-developer",
            "web": "frontend-developer",
            "mobile": "mobile-developer",
            "data": "data-analyst",
            "analyst": "data-analyst",
            "designer": "ui-ux-designer",
            "devops": "devops-engineer",
            "qa": "qa-engineer",
            "quality": "qa-engineer"
        }
    
    def categorize_by_title(self, job_title: str):
        """Categorize based on job title keywords"""
        job_title_lower = job_title.lower()
        
        for keyword, category in self.title_keywords.items():
            if keyword in job_title_lower:
                return category
        
        return None
    
    def process_all_jobs(self, limit: int = 100):
        """Process all uncategorized jobs with enhanced logic"""
        conn = sqlite3.connect(self.db_path)
        
        # Get uncategorized jobs
        cursor = conn.execute("""
            SELECT id, job_title, job_description, position_type
            FROM jobs 
            WHERE categorized_at IS NULL
            LIMIT ?
        """, (limit,))
        
        jobs = cursor.fetchall()
        print(f"Found {len(jobs)} jobs to categorize")
        
        for job_id, job_title, job_description, position_type in jobs:
            print(f"Processing job {job_id}: {job_title[:50]}...")
            
            # Try to categorize by title first (faster and more accurate for known roles)
            title_category = self.categorize_by_title(job_title)
            
            if title_category:
                # Use title-based categorization
                conn.execute("""
                    INSERT INTO job_tags (job_id, tag_type, tag_name, confidence_score)
                    VALUES (?, 'role', ?, ?)
                """, (job_id, title_category, 0.9))
                print(f"  → Categorized as '{title_category}' by title")
            else:
                # Fall back to NLP for unknown titles
                categories = self.categorize_job(job_description)
                
                for label, score in zip(categories['labels'], categories['scores']):
                    conn.execute("""
                        INSERT INTO job_tags (job_id, tag_type, tag_name, confidence_score)
                        VALUES (?, 'role', ?, ?)
                    """, (job_id, label, score))
                
                print(f"  → Added {len(categories['labels'])} roles by NLP")
            
            # Extract skills from description
            skills = self.extract_skills(job_description)
            for skill in skills:
                conn.execute("""
                    INSERT INTO job_tags (job_id, tag_type, tag_name)
                    VALUES (?, 'skill', ?)
                """, (job_id, skill))
            
            # Mark as categorized
            conn.execute(
                "UPDATE jobs SET categorized_at = CURRENT_TIMESTAMP WHERE id = ?",
                (job_id,)
            )
            
            if skills:
                print(f"  → Added {len(skills)} skills")
        
        conn.commit()
        conn.close()
        print("Categorization complete!")
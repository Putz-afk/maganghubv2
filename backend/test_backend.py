from fastapi import FastAPI
import sqlite3
from typing import List, Optional
from pydantic import BaseModel

# Create app
app = FastAPI(title="MagangHub API", version="1.0")

# Pydantic model for Job
class Job(BaseModel):
    id: int
    job_title: str
    company_name: str
    province: str
    city: str
    recommended_degree: str
    quota: int
    registered: int
    application_deadline: Optional[str] = None

@app.get("/")
def read_root():
    return {"message": "MagangHub API", "status": "running"}

@app.get("/jobs", response_model=List[Job])
def get_jobs(limit: int = 10):
    """Get list of jobs"""
    conn = sqlite3.connect('data.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, job_title, company_name, province, city, 
               recommended_degree, quota, registered, application_deadline
        FROM jobs 
        ORDER BY id 
        LIMIT ?
    ''', (limit,))
    
    jobs = cursor.fetchall()
    conn.close()
    
    return [dict(job) for job in jobs]

@app.get("/jobs/{job_id}", response_model=Job)
def get_job(job_id: int):
    """Get a single job by ID"""
    conn = sqlite3.connect('data.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, job_title, company_name, province, city, 
               recommended_degree, quota, registered, application_deadline
        FROM jobs 
        WHERE id = ?
    ''', (job_id,))
    
    job = cursor.fetchone()
    conn.close()
    
    if job is None:
        return {"error": "Job not found"}
    
    return dict(job)

@app.get("/stats")
def get_stats():
    """Get database statistics"""
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    
    # Total jobs
    cursor.execute("SELECT COUNT(*) FROM jobs")
    total_jobs = cursor.fetchone()[0]
    
    # Jobs by province
    cursor.execute("SELECT province, COUNT(*) as count FROM jobs GROUP BY province")
    by_province = cursor.fetchall()
    
    # Unique companies
    cursor.execute("SELECT COUNT(DISTINCT company_name) FROM jobs")
    unique_companies = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "total_jobs": total_jobs,
        "unique_companies": unique_companies,
        "jobs_by_province": [{"province": p, "count": c} for p, c in by_province]
    }

if __name__ == "__main__":
    import uvicorn
    print("Starting MagangHub API server...")
    print("Go to: http://127.0.0.1:8000")
    print("API Docs: http://127.0.0.1:8000/docs")
    uvicorn.run(app, host="127.0.0.1", port=8000)
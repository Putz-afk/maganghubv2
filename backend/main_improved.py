from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from typing import List, Optional
from pydantic import BaseModel, ConfigDict
from datetime import date
import json

app = FastAPI(
    title="MagangHub v2 API",
    description="API for Indonesia's fastest internship platform",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
def get_db_connection():
    conn = sqlite3.connect('data.db')
    conn.row_factory = sqlite3.Row
    return conn

# Pydantic models
class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    source_id: str
    job_title: str
    company_name: str
    province: str
    city: str
    recommended_degree: Optional[str] = None
    job_description: Optional[str] = None
    application_deadline: Optional[date] = None
    quota: int
    registered: int
    position_type: Optional[str] = None
    government_agency: Optional[str] = None
    sub_government_agency: Optional[str] = None
    scraped_at: str

class JobDetailResponse(JobResponse):
    angkatan: Optional[str] = None
    tahun: Optional[str] = None

class TagResponse(BaseModel):
    tag_type: str
    tag_name: str
    confidence_score: Optional[float] = None

class StatsResponse(BaseModel):
    total_jobs: int
    total_companies: int
    total_provinces: int
    total_quota: int
    total_registered: int
    jobs_by_province: List[dict]
    top_companies: List[dict]

# Health check
@app.get("/")
def read_root():
    return {
        "service": "MagangHub v2 API",
        "version": "2.0.0",
        "status": "running",
        "endpoints": [
            "/jobs - List jobs with filters",
            "/jobs/{id} - Get specific job",
            "/stats - Get statistics",
            "/provinces - List provinces",
            "/tags - Get job tags",
            "/docs - API documentation"
        ]
    }

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": "2024-01-01T00:00:00Z"}

# List jobs with filtering
@app.get("/jobs", response_model=List[JobResponse])
def get_jobs(
    province: Optional[str] = Query(None, description="Filter by province"),
    city: Optional[str] = Query(None, description="Filter by city"),
    search: Optional[str] = Query(None, description="Search in job title or company name"),
    degree: Optional[str] = Query(None, description="Filter by recommended degree"),
    limit: int = Query(20, ge=1, le=100, description="Number of results per page"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """Get list of jobs with optional filtering"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build query
    query = """
        SELECT 
            id, source_id, job_title, company_name, province, city,
            recommended_degree, job_description, application_deadline,
            quota, registered, position_type, government_agency,
            sub_government_agency, scraped_at
        FROM jobs 
        WHERE 1=1
    """
    params = []
    
    if province:
        query += " AND province LIKE ?"
        params.append(f"%{province}%")
    
    if city:
        query += " AND city LIKE ?"
        params.append(f"%{city}%")
    
    if search:
        query += " AND (job_title LIKE ? OR company_name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    
    if degree:
        query += " AND recommended_degree LIKE ?"
        params.append(f"%{degree}%")
    
    # Add ordering and pagination
    query += " ORDER BY application_deadline ASC, scraped_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    jobs = cursor.fetchall()
    conn.close()
    
    return [dict(job) for job in jobs]

# Get specific job
@app.get("/jobs/{job_id}", response_model=JobDetailResponse)
def get_job(job_id: int):
    """Get detailed information about a specific job"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM jobs WHERE id = ?
    """, (job_id,))
    
    job = cursor.fetchone()
    conn.close()
    
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return dict(job)

# Get statistics
@app.get("/stats", response_model=StatsResponse)
def get_stats():
    """Get database statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Basic counts
    cursor.execute("SELECT COUNT(*) as total_jobs FROM jobs")
    total_jobs = cursor.fetchone()['total_jobs']
    
    cursor.execute("SELECT COUNT(DISTINCT company_name) as total_companies FROM jobs")
    total_companies = cursor.fetchone()['total_companies']
    
    cursor.execute("SELECT COUNT(DISTINCT province) as total_provinces FROM jobs")
    total_provinces = cursor.fetchone()['total_provinces']
    
    cursor.execute("SELECT SUM(quota) as total_quota, SUM(registered) as total_registered FROM jobs")
    sums = cursor.fetchone()
    total_quota = sums['total_quota'] or 0
    total_registered = sums['total_registered'] or 0
    
    # Jobs by province
    cursor.execute("""
        SELECT province, COUNT(*) as count 
        FROM jobs 
        GROUP BY province 
        ORDER BY count DESC
    """)
    jobs_by_province = [{"province": row['province'], "count": row['count']} 
                        for row in cursor.fetchall()]
    
    # Top companies
    cursor.execute("""
        SELECT company_name, COUNT(*) as job_count, SUM(quota) as total_quota
        FROM jobs 
        GROUP BY company_name 
        ORDER BY job_count DESC 
        LIMIT 10
    """)
    top_companies = [{"company_name": row['company_name'], 
                      "job_count": row['job_count'], 
                      "total_quota": row['total_quota'] or 0} 
                     for row in cursor.fetchall()]
    
    conn.close()
    
    return StatsResponse(
        total_jobs=total_jobs,
        total_companies=total_companies,
        total_provinces=total_provinces,
        total_quota=total_quota,
        total_registered=total_registered,
        jobs_by_province=jobs_by_province,
        top_companies=top_companies
    )

# List provinces
@app.get("/provinces")
def get_provinces():
    """Get list of all provinces with job counts"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT province, COUNT(*) as job_count
        FROM jobs 
        GROUP BY province 
        ORDER BY job_count DESC
    """)
    
    provinces = cursor.fetchall()
    conn.close()
    
    return [{"province": row['province'], "job_count": row['job_count']} 
            for row in provinces]

# Get job tags
@app.get("/jobs/{job_id}/tags", response_model=List[TagResponse])
def get_job_tags(job_id: int):
    """Get tags for a specific job"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT tag_type, tag_name, confidence_score
        FROM job_tags 
        WHERE job_id = ?
        ORDER BY confidence_score DESC
    """, (job_id,))
    
    tags = cursor.fetchall()
    conn.close()
    
    return [dict(tag) for tag in tags]

if __name__ == "__main__":
    import uvicorn
    print("Starting MagangHub v2 API Server...")
    print("API URL: http://127.0.0.1:8000")
    print("Docs: http://127.0.0.1:8000/docs")
    uvicorn.run(app, host="127.0.0.1", port=8000)
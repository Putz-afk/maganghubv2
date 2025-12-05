from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import date
import sqlite3
from ..database import get_db_connection
from ..models import JobCreate, Job, JobTagCreate

router = APIRouter(prefix="/jobs", tags=["jobs"])

@router.get("/", response_model=List[Job])
def get_jobs(
    province: Optional[str] = None,
    city: Optional[str] = None,
    role: Optional[str] = None,
    limit: int = 50,
    skip: int = 0
):
    """Get jobs with optional filtering"""
    conn = get_db_connection()
    
    query = "SELECT * FROM jobs WHERE 1=1"
    params = []
    
    if province:
        query += " AND province = ?"
        params.append(province)
    
    if city:
        query += " AND city = ?"
        params.append(city)
    
    # If filtering by role (from tags)
    if role:
        query += """ AND id IN (
            SELECT job_id FROM job_tags 
            WHERE tag_type = 'role' AND tag_name = ?
        )"""
        params.append(role)
    
    query += " ORDER BY scraped_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, skip])
    
    cursor = conn.execute(query, params)
    jobs = cursor.fetchall()
    conn.close()
    
    return [dict(job) for job in jobs]

@router.get("/{job_id}", response_model=Job)
def get_job(job_id: int):
    """Get a specific job by ID"""
    conn = get_db_connection()
    cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    job = cursor.fetchone()
    conn.close()
    
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return dict(job)

@router.post("/", response_model=Job)
def create_job(job: JobCreate):
    """Create a new job listing"""
    conn = get_db_connection()
    
    cursor = conn.execute("""
        INSERT INTO jobs (
            source_id, company_name, province, city, recommended_degree,
            job_title, job_description, application_deadline
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job.source_id, job.company_name, job.province, job.city,
        job.recommended_degree, job.job_title, job.job_description,
        job.application_deadline
    ))
    
    job_id = cursor.lastrowid
    conn.commit()
    
    # Fetch the created job
    cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    new_job = cursor.fetchone()
    conn.close()
    
    return dict(new_job)

@router.get("/{job_id}/tags")
def get_job_tags(job_id: int):
    """Get tags for a specific job"""
    conn = get_db_connection()
    cursor = conn.execute(
        "SELECT * FROM job_tags WHERE job_id = ?",
        (job_id,)
    )
    tags = cursor.fetchall()
    conn.close()
    
    return [dict(tag) for tag in tags]
from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime

class JobBase(BaseModel):
    source_id: Optional[str] = None
    company_name: str
    province: str
    city: str
    recommended_degree: str
    job_title: str
    job_description: str
    application_deadline: Optional[date] = None
    
    # New fields
    quota: Optional[int] = 1
    registered: Optional[int] = 0
    position_type: Optional[str] = None
    government_agency: Optional[str] = None
    sub_government_agency: Optional[str] = None
    angkatan: Optional[str] = None
    tahun: Optional[str] = None

class JobCreate(JobBase):
    pass

class Job(JobBase):
    id: int
    scraped_at: datetime
    categorized_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    
    class Config:
        orm_mode = True
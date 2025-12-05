from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .database import init_database
from .routers import jobs

# Initialize database
init_database()

# Create FastAPI app
app = FastAPI(
    title="MagangHub v2 API",
    description="API untuk platform magang tercepat di Indonesia",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(jobs.router)

@app.get("/")
def read_root():
    return {
        "message": "MagangHub v2 API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "maganghubv2-api"}
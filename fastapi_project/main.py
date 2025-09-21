"""
FastAPI application for ETL Cali Alcaldía.
Compatible with Railway deployment using DATABASE_URL.
"""

import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add paths for imports
sys.path.append(str(Path(__file__).parent.parent / "database_management" / "core"))
sys.path.append(str(Path(__file__).parent.parent / "load_app"))

try:
    from config import get_database_config, test_connection
    from bulk_load_data import load_all_available_data
    logger.info("✅ Database modules imported successfully")
except ImportError as e:
    logger.error(f"❌ Error importing modules: {e}")

# Initialize FastAPI app
app = FastAPI(
    title="ETL Cali Alcaldía API",
    description="API para el sistema ETL de datos de la Alcaldía de Cali",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    database_connected: bool
    environment: str

class DataLoadRequest(BaseModel):
    data_types: Optional[List[str]] = None
    clear_existing: bool = False
    batch_size: int = 100

class DataLoadResponse(BaseModel):
    status: str
    message: str
    results: Optional[Dict[str, Any]] = None
    timestamp: datetime

# Global state
background_tasks_status = {}

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint."""
    return {
        "message": "ETL Cali Alcaldía API v2.0.0",
        "status": "active",
        "docs": "/docs"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint for Railway."""
    try:
        config = get_database_config()
        db_connected = test_connection(config)
        
        # Detect environment
        environment = "railway" if os.getenv("DATABASE_URL") else "local"
        
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(),
            database_connected=db_connected,
            environment=environment
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.now(),
            database_connected=False,
            environment="unknown"
        )

@app.get("/config/database")
async def get_db_config():
    """Get database configuration (safe, no passwords)."""
    try:
        config = get_database_config()
        
        # Safe config without sensitive data
        safe_config = {
            "host": config.host if not config.database_url else "Railway PostgreSQL",
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "schema": config.schema,
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow,
            "timeout": config.timeout,
            "enable_postgis": config.enable_postgis,
            "postgis_version": config.postgis_version,
            "using_database_url": bool(config.database_url)
        }
        
        return {
            "status": "success",
            "config": safe_config,
            "timestamp": datetime.now()
        }
        
    except Exception as e:
        logger.error(f"Error getting config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/data/load", response_model=DataLoadResponse)
async def load_data(
    request: DataLoadRequest,
    background_tasks: BackgroundTasks
):
    """
    Load data to database.
    Supports both synchronous and background execution.
    """
    try:
        task_id = f"load_{datetime.now().isoformat()}"
        
        # Start background task for data loading
        background_tasks.add_task(
            execute_data_load,
            task_id=task_id,
            data_types=request.data_types,
            clear_existing=request.clear_existing,
            batch_size=request.batch_size
        )
        
        background_tasks_status[task_id] = {
            "status": "started",
            "timestamp": datetime.now(),
            "data_types": request.data_types or ["all"],
            "clear_existing": request.clear_existing
        }
        
        return DataLoadResponse(
            status="accepted",
            message=f"Data loading started. Task ID: {task_id}",
            timestamp=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error starting data load: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def execute_data_load(
    task_id: str,
    data_types: Optional[List[str]] = None,
    clear_existing: bool = False,
    batch_size: int = 100
):
    """Execute data loading in background."""
    try:
        background_tasks_status[task_id]["status"] = "running"
        
        if not data_types or "all" in data_types:
            # Load all available data
            results = load_all_available_data(clear_existing=clear_existing)
        else:
            # Load specific data types (would need implementation)
            results = {"message": "Specific data type loading not implemented yet"}
        
        background_tasks_status[task_id].update({
            "status": "completed",
            "results": results,
            "completed_at": datetime.now()
        })
        
    except Exception as e:
        logger.error(f"Background task {task_id} failed: {e}")
        background_tasks_status[task_id].update({
            "status": "failed",
            "error": str(e),
            "failed_at": datetime.now()
        })

@app.get("/data/load/status/{task_id}")
async def get_load_status(task_id: str):
    """Get status of a data loading task."""
    if task_id not in background_tasks_status:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "task_id": task_id,
        "status": background_tasks_status[task_id],
        "timestamp": datetime.now()
    }

@app.get("/data/load/status")
async def get_all_load_status():
    """Get status of all data loading tasks."""
    return {
        "tasks": background_tasks_status,
        "total_tasks": len(background_tasks_status),
        "timestamp": datetime.now()
    }

@app.post("/database/test")
async def test_database_connection():
    """Test database connection."""
    try:
        config = get_database_config()
        success = test_connection(config)
        
        return {
            "status": "success" if success else "failed",
            "connected": success,
            "config_summary": {
                "host": config.host if not config.database_url else "Railway PostgreSQL",
                "database": config.database,
                "user": config.user,
                "using_database_url": bool(config.database_url)
            },
            "timestamp": datetime.now()
        }
        
    except Exception as e:
        logger.error(f"Database test failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/extraction/schedule")
async def get_extraction_schedule():
    """Get current extraction schedule configuration."""
    return {
        "message": "Extraction scheduling to be implemented",
        "options": [
            "Railway Cron Jobs",
            "GitHub Actions",
            "External API triggers"
        ],
        "timestamp": datetime.now()
    }

@app.post("/extraction/trigger")
async def trigger_extraction():
    """Manually trigger data extraction."""
    return {
        "message": "Manual extraction trigger to be implemented",
        "status": "not_implemented",
        "timestamp": datetime.now()
    }

# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Global exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    # For local development
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    )
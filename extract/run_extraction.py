import os
from datetime import datetime
from typing import List
import pandas as pd
from sqlalchemy import create_engine
import logging

from extract.adzuna_extractor import AdzunaExtractor
from extract.remoteok_extractor import RemoteOKExtractor
from extract.themuse_extractor import TheMuseExtractor
from extract.base_extractor import JobPosting

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_full_extraction(
    search_terms: List[str] = None,
    output_to_db: bool = True,
    output_to_csv: bool = False
) -> pd.DataFrame:
    """
    Run extraction from all sources.
    
    Args:
        search_terms: Job titles/keywords to search
        output_to_db: Save to PostgreSQL
        output_to_csv: Save to CSV file
        
    Returns:
        Combined DataFrame of all jobs
    """
    
    if search_terms is None:
        search_terms = [
            "data analyst",
            "business analyst", 
            "data scientist",
            "analytics engineer",
            "business intelligence"
        ]
    
    all_jobs = []
    
    # Extract from each source
    extractors = [
        RemoteOKExtractor(),    # Free, no auth
        TheMuseExtractor(),      # Free, no auth
    ]
    
    # Add Adzuna if credentials exist
    if os.getenv("ADZUNA_APP_ID"):
        extractors.append(AdzunaExtractor())
    
    for extractor in extractors:
        try:
            jobs = extractor.extract(
                search_terms=search_terms,
                max_pages=5
            )
            all_jobs.extend(jobs)
            logger.info(f"✅ {extractor.source_name}: {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"❌ {extractor.source_name} failed: {e}")
    
    # Convert to DataFrame
    df = pd.DataFrame([vars(job) for job in all_jobs])
    
    # Add extraction metadata
    df["extraction_date"] = datetime.utcnow().date()
    df["extraction_id"] = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    logger.info(f"📊 Total jobs extracted: {len(df)}")
    
    # Output
    if output_to_db:
        _save_to_database(df)
    
    if output_to_csv:
        filename = f"data/raw/jobs_{df['extraction_id'].iloc[0]}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")
    
    return df


def _save_to_database(df: pd.DataFrame):
    """Save DataFrame to PostgreSQL."""
    
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://jobmarket:your_password_here@localhost:5432/job_market"
    )
    
    # Render postgres URLs sometimes start with postgres:// but sqlalchemy needs postgresql://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
        
    engine = create_engine(database_url)
    
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
    
    # Save to raw schema
    df.to_sql(
        "raw_job_postings",
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )
    
    logger.info(f"💾 Saved {len(df)} rows to database")


if __name__ == "__main__":
    df = run_full_extraction(output_to_db=True, output_to_csv=False)
    print(df.head())

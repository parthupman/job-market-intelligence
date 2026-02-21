import os
from datetime import datetime
from typing import List
import pandas as pd
from sqlalchemy import create_engine
import logging

from adzuna_extractor import AdzunaExtractor
from remoteok_extractor import RemoteOKExtractor
from themuse_extractor import TheMuseExtractor
from base_extractor import JobPosting

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
    if os.getenv("ADZUNA_APP_ID") and os.getenv("ADZUNA_APP_KEY"):
        extractors.append(AdzunaExtractor())
    else:
        logger.warning("Adzuna credentials not found in env vars. Skipping Adzuna extraction.")
    
    for extractor in extractors:
        try:
            jobs = extractor.extract(
                search_terms=search_terms,
                max_pages=2 # Limit to 2 pages for testing
            )
            all_jobs.extend(jobs)
            logger.info(f"✅ {extractor.source_name}: {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"❌ {extractor.source_name} failed: {e}")
    
    if not all_jobs:
        logger.warning("No jobs extracted from any source.")
        return pd.DataFrame()

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
        # Save to the data/raw directory relative to the project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        output_dir = os.path.join(project_root, "data", "raw")
        os.makedirs(output_dir, exist_ok=True)

        filename = os.path.join(output_dir, f"jobs_{df['extraction_id'].iloc[0]}.csv")
        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved to {filename}")
    
    return df


def _save_to_database(df: pd.DataFrame):
    """Save DataFrame to PostgreSQL."""
    
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://jobmarket:testpass@localhost:5432/job_market"
    )
    
    try:
        engine = create_engine(database_url)
        
        # Save to raw schema
        df.to_sql(
            "raw_job_postings",
            engine,
            schema="raw",
            if_exists="append",
            index=False
        )
        
        logger.info(f"💾 Saved {len(df)} rows to database")
    except Exception as e:
        logger.error(f"Failed to save to database: {e}. Is postgres running?")


if __name__ == "__main__":
    from dotenv import load_dotenv
    # Load environment variables from .env file if it exists
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(project_root, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)

    df = run_full_extraction(output_to_db=False, output_to_csv=True)
    if not df.empty:
      print(df.head())

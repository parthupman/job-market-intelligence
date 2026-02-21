from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
import time
from dataclasses import dataclass
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class JobPosting:
    """Standardized job posting schema across all sources."""
    source: str
    source_id: str
    title: str
    company: str
    location: str
    salary_min: Optional[float]
    salary_max: Optional[float]
    salary_currency: Optional[str]
    description: str
    url: str
    posted_date: datetime
    extracted_at: datetime
    raw_data: Dict[str, Any]


class BaseExtractor(ABC):
    """Abstract base class for all job extractors."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.session = requests.Session()
        self.request_count = 0
        self.last_request_time = None
        
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the name of the data source."""
        pass
    
    @property
    @abstractmethod
    def rate_limit(self) -> float:
        """Minimum seconds between requests."""
        pass
    
    @abstractmethod
    def _fetch_page(self, page: int, **kwargs) -> List[Dict]:
        """Fetch a single page of results."""
        pass
    
    @abstractmethod
    def _parse_job(self, raw_job: Dict) -> JobPosting:
        """Parse raw API response into standardized JobPosting."""
        pass
    
    def _rate_limit_wait(self):
        """Enforce rate limiting."""
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _make_request(self, url: str, params: Dict = None) -> Dict:
        """Make HTTP request with retry logic."""
        self._rate_limit_wait()
        
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        self.request_count += 1
        logger.info(f"[{self.source_name}] Request #{self.request_count}: {url}")
        
        return response.json()
    
    def extract(
        self, 
        search_terms: List[str],
        location: str = None,
        max_pages: int = 10
    ) -> List[JobPosting]:
        """
        Main extraction method.
        
        Args:
            search_terms: List of job titles/keywords to search
            location: Location filter
            max_pages: Maximum pages to fetch per search term
            
        Returns:
            List of standardized JobPosting objects
        """
        all_jobs = []
        
        for term in search_terms:
            logger.info(f"[{self.source_name}] Extracting jobs for: {term}")
            
            for page in range(1, max_pages + 1):
                try:
                    raw_jobs = self._fetch_page(
                        page=page,
                        search_term=term,
                        location=location
                    )
                    
                    if not raw_jobs:
                        logger.info(f"[{self.source_name}] No more results at page {page}")
                        break
                    
                    for raw_job in raw_jobs:
                        try:
                            job = self._parse_job(raw_job)
                            all_jobs.append(job)
                        except Exception as e:
                            logger.warning(f"Failed to parse job: {e}")
                            continue
                            
                except Exception as e:
                    logger.error(f"Failed to fetch page {page}: {e}")
                    break
        
        logger.info(f"[{self.source_name}] Extracted {len(all_jobs)} total jobs")
        return all_jobs
    
    def to_dataframe(self, jobs: List[JobPosting]):
        """Convert jobs to pandas DataFrame."""
        import pandas as pd
        return pd.DataFrame([vars(job) for job in jobs])

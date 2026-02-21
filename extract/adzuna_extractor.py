import os
from datetime import datetime
from typing import List, Dict, Optional
from extract.base_extractor import BaseExtractor, JobPosting


class AdzunaExtractor(BaseExtractor):
    """
    Adzuna API extractor.
    
    API Docs: https://developer.adzuna.com/
    Free tier: 100 requests/day
    """
    
    BASE_URL = "https://api.adzuna.com/v1/api/jobs"
    
    def __init__(self, app_id: str = None, app_key: str = None):
        super().__init__()
        self.app_id = app_id or os.getenv("ADZUNA_APP_ID")
        self.app_key = app_key or os.getenv("ADZUNA_APP_KEY")
        
        if not self.app_id or not self.app_key:
            raise ValueError("ADZUNA_APP_ID and ADZUNA_APP_KEY required")
    
    @property
    def source_name(self) -> str:
        return "adzuna"
    
    @property
    def rate_limit(self) -> float:
        return 1.0  # 1 second between requests
    
    def _fetch_page(
        self, 
        page: int,
        search_term: str = None,
        location: str = None,
        country: str = "us"
    ) -> List[Dict]:
        """Fetch page from Adzuna API."""
        
        url = f"{self.BASE_URL}/{country}/search/{page}"
        
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": 50,
            "content-type": "application/json"
        }
        
        if search_term:
            params["what"] = search_term
        if location:
            params["where"] = location
            
        response = self._make_request(url, params)
        return response.get("results", [])
    
    def _parse_job(self, raw_job: Dict) -> JobPosting:
        """Parse Adzuna job into standardized format."""
        
        # Parse salary
        salary_min = raw_job.get("salary_min")
        salary_max = raw_job.get("salary_max")
        
        # Parse date
        created = raw_job.get("created")
        posted_date = datetime.fromisoformat(created.replace("Z", "+00:00")) if created else datetime.now()
        
        return JobPosting(
            source="adzuna",
            source_id=str(raw_job.get("id")),
            title=raw_job.get("title", ""),
            company=raw_job.get("company", {}).get("display_name", "Unknown"),
            location=raw_job.get("location", {}).get("display_name", ""),
            salary_min=float(salary_min) if salary_min else None,
            salary_max=float(salary_max) if salary_max else None,
            salary_currency="USD",
            description=raw_job.get("description", ""),
            url=raw_job.get("redirect_url", ""),
            posted_date=posted_date,
            extracted_at=datetime.utcnow(),
            raw_data=raw_job
        )

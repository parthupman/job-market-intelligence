from datetime import datetime
from typing import List, Dict
from extract.base_extractor import BaseExtractor, JobPosting


class RemoteOKExtractor(BaseExtractor):
    """
    RemoteOK API extractor.
    
    API: https://remoteok.com/api
    Free: Unlimited (be respectful)
    No auth required!
    """
    
    BASE_URL = "https://remoteok.com/api"
    
    @property
    def source_name(self) -> str:
        return "remoteok"
    
    @property
    def rate_limit(self) -> float:
        return 2.0  # Be respectful - 2 seconds
    
    def _fetch_page(self, page: int, **kwargs) -> List[Dict]:
        """RemoteOK returns all jobs in single request."""
        if page > 1:
            return []  # Only one page available
            
        # Add user agent to avoid blocking
        self.session.headers.update({
            "User-Agent": "JobMarketIntelligence/1.0 (educational project)"
        })
        
        response = self._make_request(self.BASE_URL)
        
        # First element is metadata, rest are jobs
        return response[1:] if len(response) > 1 else []
    
    def _parse_job(self, raw_job: Dict) -> JobPosting:
        """Parse RemoteOK job into standardized format."""
        
        # Parse salary from tags or salary field
        salary_min = None
        salary_max = None
        
        if raw_job.get("salary_min"):
            salary_min = float(raw_job["salary_min"])
        if raw_job.get("salary_max"):
            salary_max = float(raw_job["salary_max"])
        
        # Parse date (epoch timestamp)
        epoch = raw_job.get("epoch")
        posted_date = datetime.fromtimestamp(int(epoch)) if epoch else datetime.now()
        
        return JobPosting(
            source="remoteok",
            source_id=str(raw_job.get("id")),
            title=raw_job.get("position", ""),
            company=raw_job.get("company", "Unknown"),
            location="Remote",
            salary_min=salary_min,
            salary_max=salary_max,
            salary_currency="USD",
            description=raw_job.get("description", ""),
            url=raw_job.get("url", f"https://remoteok.com/l/{raw_job.get('id')}"),
            posted_date=posted_date,
            extracted_at=datetime.utcnow(),
            raw_data=raw_job
        )

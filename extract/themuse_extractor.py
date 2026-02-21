from datetime import datetime
from typing import List, Dict
from base_extractor import BaseExtractor, JobPosting


class TheMuseExtractor(BaseExtractor):
    """
    The Muse API extractor.
    
    API: https://www.themuse.com/developers/api/v2
    Free: Unlimited
    No auth required!
    """
    
    BASE_URL = "https://www.themuse.com/api/public/jobs"
    
    @property
    def source_name(self) -> str:
        return "themuse"
    
    @property
    def rate_limit(self) -> float:
        return 1.0
    
    def _fetch_page(
        self, 
        page: int,
        search_term: str = None,
        location: str = None,
        **kwargs
    ) -> List[Dict]:
        """Fetch from The Muse API."""
        
        params = {
            "page": page,
            "descending": "true"
        }
        
        if search_term:
            params["category"] = search_term
        if location:
            params["location"] = location
            
        response = self._make_request(self.BASE_URL, params)
        return response.get("results", [])
    
    def _parse_job(self, raw_job: Dict) -> JobPosting:
        """Parse The Muse job into standardized format."""
        
        # Get location
        locations = raw_job.get("locations", [])
        location = locations[0].get("name", "Unknown") if locations else "Unknown"
        
        # Get company
        company = raw_job.get("company", {}).get("name", "Unknown")
        
        # Parse date
        pub_date = raw_job.get("publication_date")
        posted_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00")) if pub_date else datetime.now()
        
        return JobPosting(
            source="themuse",
            source_id=str(raw_job.get("id")),
            title=raw_job.get("name", ""),
            company=company,
            location=location,
            salary_min=None,  # The Muse doesn't provide salary
            salary_max=None,
            salary_currency=None,
            description=raw_job.get("contents", ""),
            url=raw_job.get("refs", {}).get("landing_page", ""),
            posted_date=posted_date,
            extracted_at=datetime.utcnow(),
            raw_data=raw_job
        )

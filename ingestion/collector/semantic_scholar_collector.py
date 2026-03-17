from typing import List
from datetime import datetime
from ingestion.collector.base_Collector import BaseCollector
from ingestion.utils.models import Paper

class SemanticScholarCollector(BaseCollector):
    def __init__(self, rate_limit: float = 1, api_key: str = None):
        super().__init__("SemanticScholar", rate_limit)
        self.base_url = "https://api.semanticscholar.org/graph/v1/paper/search"
        self.api_key = api_key
    
    def search(self, query: str, max_results: int = 100) -> List[Paper]:
        """Search Semantic Scholar"""
        params = {
            'query': query,
            'limit': min(max_results, 100),
            'sort': 'relevance',
            'fields': 'paperId,title,abstract,authors,year,citationCount,publicationDate,venue,url'
        }
        
        if self.api_key:
            params['x-api-key'] = self.api_key
        
        try:
            response = self._fetch(self.base_url, params)
            return self.parse_response(response)
        except Exception as e:
            self.logger.error(f"Error searching Semantic Scholar: {e}")
            return []
    
    def parse_response(self, response) -> List[Paper]:
        """Parse Semantic Scholar response"""
        papers = []
        
        for result in response.get('data', []):
            try:
                authors = [
                    author.get('name', 'Unknown') 
                    for author in result.get('authors', [])
                ]
                
                paper = Paper(
                    paper_id=result.get('paperId', ''),
                    title=result.get('title', 'Unknown'),
                    abstract=result.get('abstract'),
                    authors=authors,
                    year=result.get('year', 0),
                    source="semanticscholar",
                    url=result.get('url', ''),
                    citations_count=result.get('citationCount'),
                    venue=result.get('venue')
                )
                papers.append(paper)
            except Exception as e:
                self.logger.warning(f"Error parsing Semantic Scholar result: {e}")
                continue
        
        return papers
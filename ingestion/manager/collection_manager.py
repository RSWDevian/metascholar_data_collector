from typing import Dict
from ingestion.collector.arxiv_collector import ArxivCollector
# from ingestion.collector.openalex_collector import OpenAlexCollector
# from ingestion.collector.pubmed_collector import PubMedCollector
# from ingestion.collector.semantic_scholar_collector import SemanticScholarCollector
from ingestion.storage.storage_manager import StorageManager
from ingestion.utils.logger import setup_logger
import yaml

class CollectionManager:
    def __init__(self, config_file: str = "config/sources.yaml"):
        self.logger = setup_logger("manager")
        self.storage = StorageManager()
        
        # Load config
        with open(config_file) as f:
            config = yaml.safe_load(f)
        
        self.sources_config = config.get('sources', {})
        self.collectors = self._init_collectors()
    
    def _init_collectors(self) -> Dict:
        """Initialize collectors based on config"""
        collectors = {}
        
        if self.sources_config.get('arxiv', {}).get('enabled'):
            arxiv_cfg = self.sources_config['arxiv']
            collectors['arxiv'] = ArxivCollector(
                rate_limit=arxiv_cfg.get('rate_limit', 3),
                config=arxiv_cfg
            )
        
        # if self.sources_config.get('openalex', {}).get('enabled'):
        #     openalex_cfg = self.sources_config['openalex']
        #     collectors['openalex'] = OpenAlexCollector(
        #         rate_limit=openalex_cfg.get('rate_limit', 10),
        #         config = openalex_cfg,
        #     )
        
        # if self.sources_config.get('pubmed', {}).get('enabled'):
        #     pubmed_cfg = self.sources_config['pubmed']
        #     collectors['pubmed'] = PubMedCollector(
        #         rate_limit=pubmed_cfg.get('rate_limit', 3),
        #         config=pubmed_cfg
        #     )
        
        # if self.sources_config.get('semanticscholar', {}).get('enabled'):
        #     collectors['semanticscholar'] = SemanticScholarCollector(
        #         rate_limit=self.sources_config['semanticscholar'].get('rate_limit', 1)
        #     )
        
        return collectors
    
    def collect_papers(self, query: str, source: str = None) -> Dict:
        results = {}

        collectors_to_use = {source: self.collectors[source]} if source else self.collectors

        for name, collector in collectors_to_use.items():
            try:
                self.logger.info(f"Collecting from {name}...")

                papers = []
                if name == 'arxiv':
                    categories = self.sources_config['arxiv'].get('categories', [])
                    max_results = self.sources_config[name].get('max_results_per_query', 1000)
                    papers = collector.search(query, max_results=max_results, categories=categories)

                    # ArXiv now saves incrementally per request
                    if getattr(collector, "incremental_save_enabled", False):
                        downloaded_count = int(getattr(collector, "last_run_total_downloaded", 0))
                        results[name] = {'count': downloaded_count, 'status': 'success'}
                        self.logger.info(f"Collected {downloaded_count} papers from {name} (incremental mode)")
                        continue

                self.storage.save_papers(papers, name)
                results[name] = {'count': len(papers), 'status': 'success'}
                self.logger.info(f"Collected {len(papers)} papers from {name}")
            except Exception as e:
                self.logger.error(f"Error collecting from {name}: {e}")
                results[name] = {'count': 0, 'status': 'error', 'error': str(e)}

        return results
    
    def get_stats(self) -> dict:
        """Get collection statistics"""
        return self.storage.get_stats()
    
    def close(self):
        """Close all collectors"""
        for collector in self.collectors.values():
            collector.close()

from typing import Dict, List, Optional
from ingestion.collector.arxiv_collector import ArxivCollector
from ingestion.storage.storage_manager import StorageManager
from ingestion.utils.logger import setup_logger
import yaml


class CollectionManager:
    def __init__(self, config_file: str = "config/sources.yaml"):
        self.logger = setup_logger("manager")
        self.storage = StorageManager()

        with open(config_file) as f:
            config = yaml.safe_load(f)

        self.sources_config = config.get("sources", {})
        self.collectors = self._init_collectors()

    def _init_collectors(self) -> Dict:
        collectors = {}

        if self.sources_config.get("arxiv", {}).get("enabled"):
            arxiv_cfg = self.sources_config["arxiv"]
            collectors["arxiv"] = ArxivCollector(
                rate_limit=arxiv_cfg.get("rate_limit", 3),
                config=arxiv_cfg,
            )

        return collectors

    def collect_papers(
        self,
        query: str,
        source: str = None,
        categories: Optional[List[str]] = None,
    ) -> Dict:
        results = {}
        collectors_to_use = {source: self.collectors[source]} if source else self.collectors

        for name, collector in collectors_to_use.items():
            try:
                self.logger.info(f"Collecting from {name}...")

                papers = []
                if name == "arxiv":
                    selected_categories = categories or self.sources_config["arxiv"].get("categories", [])
                    max_results = self.sources_config[name].get("max_results_per_query", 1000)

                    papers = collector.search(
                        query,
                        max_results=max_results,
                        categories=selected_categories,
                    )

                    if getattr(collector, "incremental_save_enabled", False):
                        downloaded_count = int(getattr(collector, "last_run_total_downloaded", 0))
                        results[name] = {
                            "count": downloaded_count,
                            "status": "success",
                            "categories_used": selected_categories,
                        }
                        self.logger.info(
                            f"Collected {downloaded_count} papers from {name} (incremental mode)"
                        )
                        continue

                self.storage.save_papers(papers, name)
                results[name] = {"count": len(papers), "status": "success"}
                self.logger.info(f"Collected {len(papers)} papers from {name}")

            except Exception as e:
                self.logger.error(f"Error collecting from {name}: {e}")
                results[name] = {"count": 0, "status": "error", "error": str(e)}

        return results

    def get_stats(self) -> dict:
        return self.storage.get_stats()

    def close(self):
        for collector in self.collectors.values():
            collector.close()
"""
OpenAlex Collector

Two modes:
  1. Via collection_manager:
         collector.search(query, max_results)   ← keyword search
  2. Standalone bulk crawl:
         python -m ingestion.collector.openalex_collector
         (runs interactive CLI, reads concepts from sources.yaml)

Run standalone from project root:
    python -m ingestion.collector.openalex_collector
"""

from __future__ import annotations

import os
import json
import time
import logging
import requests
from datetime import datetime
from typing import List, Optional
from ingestion.collector.base_Collector import BaseCollector
from ingestion.utils.models import Paper

class OpenAlexCollector(BaseCollector):
    SELECT_FIELDS = ",".join([
        "id", "title", "abstract_inverted_index",
        "authorships", "publication_year","publication_date",
        "primary_location", "concepts", "topics",
        "doi", "cited_by_count", "type", "ids", "updated_date",
    ])
    
    def __init__(self, rate_limit: float = 10, config: dict = None):
        super().__init__("OpenAlex", rate_limit)
        
        cfg = config or {}
        self.base_url = cfg.get("base_url", "https://api.openalex.org") + "/works"
        self.email = cfg.get("email", "")
        self.year_from = cfg.get("year_from", 1900)
        self.year_to = cfg.get("year_to", 2026)
        self.request_delay = cfg.get("request_delay_seconds", 0.1)
        self.concepts = cfg.get("concepts", [])
        
        pagination = cfg.get("pagination", {})
        self.resume_checkpoint = pagination.get("resume_checkpoint", True)
        self.checkpoint_file = pagination.get("checkpoint_file", "data/raw/openalex/openalex_checkpoint.json")

        self.output_dir = os.path.dirname(self.checkpoint_file)


    @staticmethod
    def reconstruct_abstract(inverted_index: Optional[dict]) -> Optional[str]:
        if not inverted_index:
            return None
        position_word = {}
        for word, positions in inverted_index.items():
            for pos in positions:
                position_word[pos] = word
        return " ".join(position_word[i] for i in sorted(position_word)) if position_word else None

    def search(self, query: str, max_results: int = 200) -> List[Paper]:
        all_papers: List[Paper] = []
        cursor = "*"
        remaining = max_results
        
        while remaining > 0:
            params = {
                "search": query,
                "per-page": min(remaining, 200),
                "sort": "cited_by_count:desc",
                "cursor": cursor,
                "select": self.SELECT_FIELDS,
            }
            if self.email:
                params["mailto"] = self.email
                
            try:
                response = self._fetch(self.base_url, params)
            except Exception as e:
                self.logger.error(f"search failed: {e}")
                break
            page_papers = self.parse_response(response)
            if not page_papers:
                break
            
            all_papers.extend(page_papers)
            remaining -= len(page_papers)
            cursor = response.get("meta", {}).get("next_cursor")
            if not cursor:
                break
        return all_papers[:max_results]
    
    def parse_response(self, response: dict) -> List[Paper]:
        papers = []
        for result in response.get("results", []):
            try:
                paper = self._parse_single(result)
                if paper:
                    papers.append(paper)
        
            except Exception as e:
                self.logger.warning(f"Failed to parse item: {e}")
        return papers
    
    # bulk concept crawl
    def crawl_concept(self) -> dict:
        if not self.concepts:
            self.logger.error("No concepts defined in sources.yaml")
            return {}
        
        os.makedirs(self.output_dir, exist_ok=True)
        summary = {}
        
        for concept in self.concepts:
            count = self._crawl_concept(concept)
            summary[concept["name"]] = count
            
        return summary
    
    def _crawl_concept(self, concept: dict) -> int:
        concept_id = concept["id"]
        concept_name = concept["name"]
        
        output_path = os.path.join(self.output_dir, f"{concept_name}_{self.year_from}_{self.year_to}.json")
        checkpoint_path = self.checkpoint_file
        
        checkpoint = self._load_checkpoint(checkpoint_path, concept_id)
        total_saved = checkpoint["total_saved"]
        
        records: list = []
        if total_saved > 0 and os.path.exists(output_path):
            self.logger.info(f"Loading {total_saved:,} existing records from disk")
            with open(output_path, "r", encoding="utf-8") as f:
                records = json.load(f)
                
        self.logger.info(f"Output: {output_path}")
        
        for year in range(self.year_from, self.year_to + 1):
            last_year = checkpoint.get("last_year")
            if last_year and year < last_year:
                self.logger.info(f"skipping year {year}(already done)")
                continue
            
            year_filter = (
                f"publication_year:{year},"
                f"concepts.id:{concept_id},"
                f"type:article|preprint"
            )
            count = self._get_count(year_filter)
            self.logger.info(f"Year {year}: {count:,} papers")
            
            if count == 0:
                continue
            
            if count <= 9_000:
                start_cursor = (
                    checkpoint.get("cursor", "*")
                    if checkpoint.get("last_year") == year and not checkpoint.get("last_month")
                    else "*"
                )
                records, total_saved = self._fetch_all_pages(
                    filter_str=year_filter,
                    records=records,
                    total_saved=total_saved,
                    output_path=output_path,
                    checkpoint_path=checkpoint_path,
                    concept_id=concept_id,
                    start_cursor=start_cursor,
                    year=year,
                    month=None,
                )
            else:
                for month in range(1, 13):
                    last_month = checkpoint.get("last_month")
                    if (checkpoint.get("last_year") == year
                            and last_month and month < last_month):
                        self.logger.info(f"skipping {year}-{month:02d}(already done)")
                        continue
                    
                    month_str = f"{year}-{month:02d}"
                    month_filter = (
                        f"from_publication_date:{month_str}-01,"
                        f"to_publication_date:{month_str}-28,"
                        f"concepts.id:{concept_id},"
                        f"type:article|preprint"
                    )
                    month_count = self._get_count(month_filter)
                    self.logger.info(f"Month {month_str}: {month_count:,} papers")
                    if month_count == 0:
                        continue
                    start_cursor = (
                        checkpoint.get("cursor", "*")
                        if (checkpoint.get("last_year") == year and checkpoint.get("last_month") == month)
                        else "*"
                    )
                    
                    records, total_saved = self._fetch_all_pages(
                        filter_str=month_filter,
                        records=records,
                        total_saved=total_saved,
                        output_path=output_path,
                        checkpoint_path=checkpoint_path,
                        concept_id=concept_id,
                        start_cursor=start_cursor,
                        year=year,
                        month=month,
                    )
                    time.sleep(self.request_delay)
            self.logger.info(f"Year {year} complete. Total so far: {total_saved:,}")
        self.logger.info(f"Concept '{concept_name}' complete. Total: {total_saved:,} records.")
        self._save_checkpoint(checkpoint_path, concept_id, "*", total_saved)
        return total_saved
    
    def _fetch_all_pages(
        self,
        filter_str: str,
        records: list,
        total_saved: int,
        output_path: str,
        checkpoint_path: str,
        concept_id: str,
        start_cursor: str = "*",
        year: int = None,
        month: int = None,
    ) -> tuple[list, int]:
        cursor = start_cursor
        page_num = 0
        
        base_params = {
            "filter": filter_str,
            "select": self.SELECT_FIELDS,
            "per-page": 200,
        }
        if self.email:
            base_params["mailto"] = self.email
        
        while True:
            page_num += 1
            params = {**base_params, "cursor": cursor}
            
            try:
                data = self._fetch(self.base_url, params)
            except Exception as e:
                self.logger.error(f"Failed on page {page_num}: {e}. Saving progress and stopping.")
                break
            
            results = data.get("results", [])
            if not results:
                break
            
            for work in results:
                records.append({
                    "raw": work,
                    "normalized": self._normalize(work),
                })
            
            total_saved += len(results)
            meta = data.get("meta", {})
            next_cursor = meta.get("next_cursor")
            total_available = meta.get("count", "?")
            
            self.logger.info(f"Page {page_num} — +{len(results)} | total so far: {total_saved:,} / {total_available}")
            
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            
            self._save_checkpoint(checkpoint_path, concept_id, next_cursor or "*", total_saved, year, month)
            
            if not next_cursor:
                break
            cursor = next_cursor
            time.sleep(self.request_delay)
            
        return records, total_saved
    
    def _parse_single(self, result: dict) -> Optional[Paper]:
        authors = [
            a["author"]["display_name"]
            for a in result.get("authorships", [])
            if a.get("author", {}).get("display_name")
        ]
        pub_date = None
        raw_data = result.get("publication_date")
        if raw_data:
            try:
                pub_date = datetime.fromisoformat(raw_data)
            except ValueError:
                pass
            
        abstract = self.reconstruct_abstract(result.get("abstract_inverted_index"))
        categories = [
            t["display_name"] for t in result.get("topics", [])
            if t.get("display_name")
        ]
        if not categories:
            categories = [
                c["display_name"] for c in result.get("concepts", [])
                if c.get("display_name")
            ]
        doi = result.get("doi") or ""
        if doi.startswith("https://doi.org/"):
            doi = doi.replace("https://doi.org/", "")
        
        primary_location = result.get("primary_location") or {}
        venue = (primary_location.get("source") or {}).get("display_name")
        url = primary_location.get("landing_page_url") or result.get("id", "")
        
        ids = result.get("ids") or {}
        arxiv_url = ids.get("arxiv")
        arxiv_id = arxiv_url.split("/")[-1] if arxiv_url else None
        
        return Paper(
            paper_id = result["id"].split("/")[-1],
            title = result.get("title") or "Unknown",
            abstract = abstract,
            authors = authors,
            year = result.get("publication_year") or 0,
            source = "openalex",
            url = url,
            publication_date = pub_date,
            citations_count = result.get("cited_by_count"),
            venue = venue,
            doi = doi or None,
            categories = categories,
            arxiv_id = arxiv_id,
        )
        
    def _normalize(self, raw: dict) -> dict:
        authors = [
            a.get("author", {}).get("display_name")
            for a in raw.get("authorships", [])
            if a.get("author", {}).get("display_name")
        ]
        primary_location = raw.get("primary_location") or {}
        source_name = (primary_location.get("source") or {}).get("display_name")
        
        categories = [
            t["display_name"] for t in raw.get("topics", [])
            if t.get("display_name")
        ]
        if not categories:
            categories = [
                c["display_name"] for c in raw.get("concepts", [])
                if c.get("display_name")
            ]
        doi = raw.get("doi") or ""
        if doi.startswith("https://doi.org/"):
            doi = doi.replace("https://doi.org/", "")
            
        ids = raw.get("ids") or {}
        arxiv_url = ids.get("arxiv")
        arxiv_id = arxiv_url.split("/")[-1] if arxiv_url else None
        return {
            "source": "openalex",
            "paper_id": raw.get("id", "").replace("https://openalex.org/", ""),
            "title": raw.get("title"),
            "abstract": self.reconstruct_abstract(raw.get("abstract_inverted_index")),
            "authors": authors,
            "year": raw.get("publication_year"),
            "source_name": source_name,
            "categories": categories,
            "doi": doi or None,
            "citation_count": raw.get("cited_by_count"),
            "url": primary_location.get("landing_page_url"),
            "arxiv_id": arxiv_id,
            "type": raw.get("type"),
            "updated_date": raw.get("updated_date"),
        }
        
    def _get_count(self, filter_str: str) -> int:
        params = {
            "filter": filter_str,
            "per-page": 1,
            "select": "id",
        }
        if self.email:
            params["mailto"] = self.email
        try:
            data = self._fetch(self.base_url, params)
            return data.get("meta", {}).get("count", 0)
        except Exception:
            return 0
        
    
    
    def _load_checkpoint(self, path: str, concept_id: str) -> dict:
        if self.resume_checkpoint and os.path.exists(path):
            with open(path, "r") as f:
                data = json.load(f)
            if data.get("concept_id") == concept_id:
                self.logger.info(f"Resuming '{concept_id}' — {data.get('total_saved', 0):,} records saved so far.")
                return data
        return {
            "concept_id": concept_id,
            "cursor": "*",
            "total_saved": 0,
            "last_year": None,
            "last_month": None,
            "last_updated": None,
        }
    
    def _save_checkpoint(self, path: str, concept_id: str, cursor: str, total_saved: int, year: int = None, month: int = None):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump({
                "concept_id": concept_id,
                "cursor": cursor,
                "total_saved": total_saved,
                "last_year": year,
                "last_month": month,
                "last_updated": datetime.utcnow().isoformat(),
            }, f, indent=2)
            
if __name__ == "__main__":
    import yaml
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "sources.yaml")
    
    with open(config_path) as f:
        full_config = yaml.safe_load(f)
    openalex_cfg = full_config.get("sources", {}).get("openalex", {})
    
    collector = OpenAlexCollector(rate_limit=openalex_cfg.get("rate_limit", 10), config=openalex_cfg,)
    summary = collector.crawl_concept()
    for name, count in summary.items():
        print(f"{name}: {count:,} papers collected")
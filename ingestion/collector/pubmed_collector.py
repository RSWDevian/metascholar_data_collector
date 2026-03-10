from typing import List, Optional, Dict
import xml.etree.ElementTree as ET
import time
import random
import json
from pathlib import Path

from ingestion.collector.base_collector import BaseCollector
from ingestion.utils.models import Paper


class PubMedCollector(BaseCollector):

    def __init__(self, rate_limit: float = 3, config: Optional[Dict] = None):
        super().__init__("PubMed", rate_limit)

        self.esearch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        self.efetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

        self.config = config or {}

        pagination_cfg = self.config.get("pagination", {})

        self.pagination_enabled = pagination_cfg.get("enabled", True)
        self.default_start = int(pagination_cfg.get("start", 0))
        self.default_page_size = int(pagination_cfg.get("page_size", 200))
        self.max_requests = int(pagination_cfg.get("max_requests", 1000))

        self.resume_enabled = bool(
            pagination_cfg.get("resume_from_checkpoint", True)
        )
        self.checkpoint_every_requests = int(
            pagination_cfg.get("checkpoint_every_requests", 10)
        )

        self.request_delay_seconds = float(
            self.config.get("request_delay_seconds", 0)
        )
        self.request_delay_jitter_seconds = float(
            self.config.get("request_delay_jitter_seconds", 0)
        )

        checkpoint_path = pagination_cfg.get(
            "checkpoint_file",
            "data/raw/pubmed/pubmed_checkpoint.json"
        )

        self.checkpoint_file = Path(checkpoint_path)
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)

        self.checkpoint = self._load_checkpoint()
    def _load_checkpoint(self) -> Dict:

        if not self.checkpoint_file.exists():
            return {"queries": {}}

        try:
            with self.checkpoint_file.open("r", encoding="utf-8") as f:
                data = json.load(f)

            if "queries" not in data:
                data["queries"] = {}

            return data

        except Exception as e:
            self.logger.warning(f"Could not load checkpoint file: {e}")
            return {"queries": {}}

    def _save_checkpoint(self):

        tmp_file = self.checkpoint_file.with_suffix(".tmp")

        with tmp_file.open("w", encoding="utf-8") as f:
            json.dump(self.checkpoint, f, indent=2)

        tmp_file.replace(self.checkpoint_file)

    def _checkpoint_key(self, query: str) -> str:
        return query.strip()

    def _get_resume_start(self, query: str) -> int:

        if not self.resume_enabled:
            return self.default_start

        key = self._checkpoint_key(query)

        return int(
            self.checkpoint.get("queries", {}).get(key, self.default_start)
        )

    def _set_resume_start(self, query: str, start: int):

        key = self._checkpoint_key(query)

        self.checkpoint.setdefault("queries", {})[key] = int(start)

    def _mark_query_complete(self, query: str):

        key = self._checkpoint_key(query)

        self.checkpoint.setdefault("queries", {})[key] = -1
    def search(self, query: str, max_results: int = 1000) -> List[Paper]:

        papers: List[Paper] = []

        request_count = 0
        page_size = min(max_results, self.default_page_size)

        start = self._get_resume_start(query)

        if start == -1:
            self.logger.info(f"Skipping completed query: {query}")
            return papers

        while True:

            if request_count >= self.max_requests:

                self.logger.info("Reached max requests, saving checkpoint")
                self._save_checkpoint()
                return papers

            ids = self._search_ids(query, start, page_size)

            request_count += 1

            if not ids:

                self.logger.info("No more results")
                self._mark_query_complete(query)
                self._save_checkpoint()
                break

            batch = self._fetch_papers(ids)

            papers.extend(batch)

            next_start = start + len(ids)

            self._set_resume_start(query, next_start)

            if request_count % self.checkpoint_every_requests == 0:
                self._save_checkpoint()

            if len(ids) < page_size:

                self.logger.info("Reached last page")

                self._mark_query_complete(query)
                self._save_checkpoint()
                break

            if not self.pagination_enabled:
                break

            start += page_size

            self._sleep_between_requests()

        self._save_checkpoint()

        return papers
    def _sleep_between_requests(self):

        if self.request_delay_seconds <= 0:
            return

        jitter = (
            random.uniform(0, self.request_delay_jitter_seconds)
            if self.request_delay_jitter_seconds > 0
            else 0
        )

        time.sleep(self.request_delay_seconds + jitter)
    def _search_ids(self, query: str, start: int, max_results: int):
        params = {
            "db": "pubmed",
            "term": query,
            "retstart": start,
            "retmax": max_results,
            "retmode": "json"
        }
        try:
            data = self._fetch(self.esearch_url, params=params)
            return data["esearchresult"]["idlist"]
        except Exception as e:
            self.logger.error(f"Error searching PubMed: {e}")
            return []
    def _fetch_papers(self, id_list: List[str]) -> List[Paper]:
        params = {
            "db": "pubmed",
            "id": ",".join(id_list),
            "retmode": "xml"}
        try:
            self.rate_limiter.wait()
            response = self.session.get(
                self.efetch_url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            root = ET.fromstring(response.content)
            return self.parse_response(root)
        except Exception as e:
            self.logger.error(f"Error fetching PubMed papers: {e}")
            return []
    def parse_response(self, root) -> List[Paper]:
        papers = []
        for article in root.findall(".//PubmedArticle"):
            try:
                pmid = article.findtext(".//PMID")
                title = article.findtext(".//ArticleTitle")
                abstract = " ".join(
                    [a.text for a in article.findall(".//AbstractText") if a.text]
                )
                authors = []
                for author in article.findall(".//Author"):
                    lastname = author.findtext("LastName")
                    firstname = author.findtext("ForeName")
                    if lastname and firstname:
                        authors.append(f"{firstname} {lastname}")
                year = article.findtext(".//PubDate/Year")
                paper = Paper(
                    paper_id=pmid,
                    title=title,
                    abstract=abstract,
                    authors=authors,
                    year=int(year) if year else None,
                    source="pubmed",
                    url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                    publication_date=None,
                    venue="PubMed"
                )
                papers.append(paper)
            except Exception as e:
                self.logger.warning(f"Error parsing PubMed entry: {e}")
                continue
        return papers


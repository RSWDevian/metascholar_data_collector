from typing import List, Optional, Dict, Any
import feedparser
from datetime import datetime, timezone
from urllib.parse import urlencode
import time
import random
import json
from pathlib import Path

from ingestion.collector.base_Collector import BaseCollector
from ingestion.utils.models import Paper
from ingestion.storage.storage_manager import StorageManager


class ArxivCollector(BaseCollector):
    def __init__(self, rate_limit: float = 3, config: Optional[Dict] = None):
        super().__init__("ArXiv", rate_limit)
        self.config = config or {}
        self.base_url = self.config.get("base_url", "http://export.arxiv.org/api/query")

        pagination_cfg = self.config.get("pagination", {})
        self.pagination_enabled = pagination_cfg.get("enabled", True)
        self.default_start = int(pagination_cfg.get("start", 0))
        self.default_page_size = int(pagination_cfg.get("page_size", 1000))
        self.max_requests = int(pagination_cfg.get("max_requests", 1000))
        self.resume_enabled = bool(pagination_cfg.get("resume_from_checkpoint", True))
        self.checkpoint_every_requests = int(pagination_cfg.get("checkpoint_every_requests", 10))
        self.progress_every_requests = int(pagination_cfg.get("progress_every_requests", 1))

        self.request_delay_seconds = float(self.config.get("request_delay_seconds", 0))
        self.request_delay_jitter_seconds = float(self.config.get("request_delay_jitter_seconds", 0))

        # New behavior knobs
        self.incremental_save_enabled = bool(self.config.get("save_each_request", True))
        self.collect_in_memory = bool(self.config.get("collect_in_memory", False))

        checkpoint_path = pagination_cfg.get("checkpoint_file", "data/raw/arxiv/arxiv_checkpoint.json")
        self.checkpoint_file = Path(checkpoint_path)
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint = self._load_checkpoint()

        self.storage = StorageManager()
        self.last_run_total_downloaded = 0

    def _load_checkpoint(self) -> Dict[str, Any]:
        default_state = {
            "queries": {},
            "total_downloaded": 0,
            "last_paper_id": None,
            "updated_at": None,
        }

        if not self.checkpoint_file.exists():
            return default_state

        try:
            with self.checkpoint_file.open("r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, dict):
                return default_state

            data.setdefault("queries", {})
            data.setdefault("total_downloaded", 0)
            data.setdefault("last_paper_id", None)
            data.setdefault("updated_at", None)

            # Backward compatibility for old int-based query states
            normalized_queries = {}
            for q, state in data["queries"].items():
                if isinstance(state, int):
                    if state == -1:
                        normalized_queries[q] = {
                            "next_start": -1,
                            "status": "complete",
                            "downloaded": 0,
                            "last_paper_id": None,
                        }
                    else:
                        normalized_queries[q] = {
                            "next_start": int(state),
                            "status": "in_progress",
                            "downloaded": 0,
                            "last_paper_id": None,
                        }
                elif isinstance(state, dict):
                    normalized_queries[q] = {
                        "next_start": int(state.get("next_start", self.default_start)),
                        "status": state.get("status", "in_progress"),
                        "downloaded": int(state.get("downloaded", 0)),
                        "last_paper_id": state.get("last_paper_id"),
                    }
            data["queries"] = normalized_queries
            return data
        except Exception as e:
            self.logger.warning(f"Could not load checkpoint file: {e}")
            return default_state

    def _save_checkpoint(self):
        self.checkpoint["updated_at"] = datetime.now(timezone.utc).isoformat()
        tmp_file = self.checkpoint_file.with_suffix(".tmp")
        with tmp_file.open("w", encoding="utf-8") as f:
            json.dump(self.checkpoint, f, indent=2)
        tmp_file.replace(self.checkpoint_file)

    def _checkpoint_key(self, query: str) -> str:
        return query.strip()

    def _get_query_state(self, query: str) -> Dict[str, Any]:
        key = self._checkpoint_key(query)
        return self.checkpoint["queries"].get(
            key,
            {
                "next_start": self.default_start,
                "status": "in_progress",
                "downloaded": 0,
                "last_paper_id": None,
            },
        )

    def _set_query_state(
        self,
        query: str,
        *,
        next_start: int,
        status: str,
        downloaded: int,
        last_paper_id: Optional[str],
    ):
        key = self._checkpoint_key(query)
        self.checkpoint["queries"][key] = {
            "next_start": int(next_start),
            "status": status,
            "downloaded": int(downloaded),
            "last_paper_id": last_paper_id,
        }

    def _sleep_between_requests(self):
        if self.request_delay_seconds <= 0:
            return
        jitter = random.uniform(0, self.request_delay_jitter_seconds) if self.request_delay_jitter_seconds > 0 else 0
        time.sleep(self.request_delay_seconds + jitter)

    def _perform_search(self, query: str, start: int, max_results: int) -> List[Paper]:
        params = {
            "search_query": query,
            "start": start,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }

        try:
            self.rate_limiter.wait()
            url = f"{self.base_url}?{urlencode(params)}"
            feed = feedparser.parse(url)
            return self.parse_response(feed)
        except Exception as e:
            self.logger.error(f"Error searching ArXiv at start={start}: {e}")
            return []

    def search(self, query: str, max_results: int = 1000, categories: Optional[List[str]] = None) -> List[Paper]:
        papers: List[Paper] = []
        request_count = 0
        run_downloaded = 0
        page_size = min(max_results, self.default_page_size)

        queries = []
        if categories:
            for category in categories:
                cat_query = f"cat:{category}"
                if query:
                    cat_query = f"({cat_query}) AND ({query})"
                queries.append(cat_query)
        else:
            queries.append(query)

        try:
            for single_query in queries:
                state = self._get_query_state(single_query)
                if state["status"] == "complete" or state["next_start"] == -1:
                    self.logger.info(f"Skipping completed query: {single_query}")
                    continue

                start = int(state["next_start"])
                query_downloaded = int(state["downloaded"])
                self.logger.info(f"Starting ArXiv query: '{single_query}' from offset={start}")

                while True:
                    if request_count >= self.max_requests:
                        self._save_checkpoint()
                        self.last_run_total_downloaded = run_downloaded
                        self.logger.info(
                            "Reached max_requests=%s, stopping. run_downloaded=%s total_downloaded=%s",
                            self.max_requests,
                            run_downloaded,
                            int(self.checkpoint.get("total_downloaded", 0)),
                        )
                        return papers

                    batch = self._perform_search(single_query, start=start, max_results=page_size)
                    request_count += 1

                    if not batch:
                        self.logger.info(f"No more results for query: {single_query}")
                        self._set_query_state(
                            single_query,
                            next_start=-1,
                            status="complete",
                            downloaded=query_downloaded,
                            last_paper_id=state.get("last_paper_id"),
                        )
                        self._save_checkpoint()
                        break

                    # Save each request immediately
                    if self.incremental_save_enabled:
                        self.storage.save_papers(batch, "arxiv")

                    if self.collect_in_memory:
                        papers.extend(batch)

                    batch_count = len(batch)
                    run_downloaded += batch_count
                    query_downloaded += batch_count
                    last_paper_id = batch[-1].paper_id
                    next_start = start + batch_count

                    # Update checkpoint with robust state
                    self._set_query_state(
                        single_query,
                        next_start=next_start,
                        status="in_progress",
                        downloaded=query_downloaded,
                        last_paper_id=last_paper_id,
                    )
                    self.checkpoint["total_downloaded"] = int(self.checkpoint.get("total_downloaded", 0)) + batch_count
                    self.checkpoint["last_paper_id"] = last_paper_id

                    progress_every = max(self.progress_every_requests, 1)
                    if request_count % progress_every == 0:
                        self.logger.info(
                            "ArXiv progress | requests=%s | last_batch=%s | query_downloaded=%s | run_downloaded=%s | total_downloaded=%s | last_paper_id=%s | current_offset=%s",
                            request_count,
                            batch_count,
                            query_downloaded,
                            run_downloaded,
                            int(self.checkpoint.get("total_downloaded", 0)),
                            last_paper_id,
                            next_start,
                        )

                    # Persist checkpoint every request (and keeps your old knob too)
                    if request_count % max(self.checkpoint_every_requests, 1) == 0:
                        self._save_checkpoint()
                    self._save_checkpoint()

                    if len(batch) < page_size:
                        self.logger.info(f"Reached last page for query: {single_query}")
                        self._set_query_state(
                            single_query,
                            next_start=-1,
                            status="complete",
                            downloaded=query_downloaded,
                            last_paper_id=last_paper_id,
                        )
                        self._save_checkpoint()
                        break

                    if not self.pagination_enabled:
                        self._save_checkpoint()
                        break

                    start = next_start
                    self._sleep_between_requests()

            self._save_checkpoint()
            self.last_run_total_downloaded = run_downloaded
            self.logger.info(
                "ArXiv collection finished. run_downloaded=%s total_downloaded=%s",
                run_downloaded,
                int(self.checkpoint.get("total_downloaded", 0)),
            )
            return papers

        except KeyboardInterrupt:
            self.last_run_total_downloaded = run_downloaded
            self._save_checkpoint()
            self.logger.warning(
                "ArXiv interrupted. Progress saved. run_downloaded=%s total_downloaded=%s",
                run_downloaded,
                int(self.checkpoint.get("total_downloaded", 0)),
            )
            return papers

    def parse_response(self, feed) -> List[Paper]:
        papers = []

        for entry in getattr(feed, "entries", []):
            try:
                arxiv_id = entry.id.split("/abs/")[-1]
                version = f"v{arxiv_id.split('v')[-1]}" if "v" in arxiv_id else "v1"

                doi = getattr(entry, "arxiv_doi", None)
                if not doi:
                    for link in getattr(entry, "links", []):
                        href = getattr(link, "href", "") or ""
                        title = (getattr(link, "title", "") or "").lower()
                        if "doi.org/" in href:
                            doi = href.split("doi.org/")[-1].strip()
                            break
                        if "doi" in title and href:
                            doi = href.strip()
                            break

                paper = Paper(
                    paper_id=arxiv_id,
                    title=entry.title,
                    abstract=getattr(entry, "summary", None),
                    authors=[author.name for author in getattr(entry, "authors", [])],
                    year=int(entry.published[:4]),
                    source="arxiv",
                    url=entry.id,
                    publication_date=datetime.fromisoformat(entry.published.replace("Z", "+00:00")),
                    venue="ArXiv",
                    doi=doi,
                    categories=[tag.get("term") for tag in getattr(entry, "tags", []) if tag.get("term")],
                    version=version,
                    updated_date=datetime.fromisoformat(entry.updated.replace("Z", "+00:00")) if hasattr(entry, "updated") else None,
                    source_metadata={
                        "primary_category": getattr(entry, "arxiv_primary_category", {}).get("term"),
                        "pdf_url": entry.id.replace("/abs/", "/pdf/"),
                        "comment": getattr(entry, "arxiv_comment", None),
                        "arxiv_id": arxiv_id,
                        "doi": doi,
                    },
                )
                papers.append(paper)
            except Exception as e:
                self.logger.warning(f"Error parsing ArXiv entry: {e}")
                continue

        return papers
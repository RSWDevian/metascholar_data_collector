from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime

@dataclass
class Paper:
    # core
    paper_id: str    
    title: str
    abstract: Optional[str]
    authors: List[str]
    year: int
    source: str 
    url: str
    canonical_id: Optional[str] = None

    # Metadata
    publication_date: Optional[datetime] = None
    venue: Optional[str] = None
    doi: Optional[str] = None
    keywords: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    
    # Versioning
    version: Optional[str] = None
    updated_date: Optional[datetime] = None
    
    # Graph
    references: List[str] = field(default_factory=list)
    citation_count: Optional[int] = None
    
    # Ai pipeline
    embedding_ids: Dict[str, str] = field(default_factory=dict)
    
    # Source specific + extensible
    source_metadata: Dict[str, Any] = field(default_factory=dict)

    # Derived Metadata
    extra_metadata: Dict[str, Any] = field(default_factory=dict)

    # Pipeline tracking
    parsed: bool = False
    embedded: bool = False
    kg_extracted: bool = False

    def __post_init__(self):
        if self.canonical_id is None:
            self.canonical_id = self._generate_canonical_id()

    def _generate_canonical_id(self) -> str:
        import hashlib

        def _clean(s: str) -> str:
            return str(s).strip()

        def _normalize_doi(doi_value: str) -> str:
            d = _clean(doi_value).lower()
            for prefix in (
                "https://doi.org/",
                "http://doi.org/",
                "doi:",
                "https://dx.doi.org/",
                "http://dx.doi.org/",
            ):
                if d.startswith(prefix):
                    d = d[len(prefix):]
            return d

        def _normalize_arxiv_id(arxiv_value: str) -> str:
            aid = _clean(arxiv_value).lower()
            if aid.startswith("arxiv:"):
                aid = aid.split("arxiv:", 1)[1]
            if "v" in aid:
                left, right = aid.rsplit("v", 1)
                if right.isdigit():
                    aid = left
            return aid

        # 1) DOI (best universal key)
        doi = self.doi or self.source_metadata.get("doi")
        if doi:
            doi_norm = _normalize_doi(doi)

            # Special case: ArXiv DOI => convert to arxiv:<id> for cross-source alignment
            # 10.48550/arXiv.2302.01234 -> arxiv:2302.01234
            if doi_norm.startswith("10.48550/arxiv."):
                arxiv_part = doi_norm.split("10.48550/arxiv.", 1)[1]
                return f"arxiv:{_normalize_arxiv_id(arxiv_part)}"

            return f"doi:{doi_norm}"

        # 2) ArXiv ID
        arxiv_id = self.source_metadata.get("arxiv_id")
        if self.source == "arxiv":
            arxiv_id = self.paper_id
        if arxiv_id:
            return f"arxiv:{_normalize_arxiv_id(arxiv_id)}"

        # 3) PubMed ID
        pmid = self.source_metadata.get("pmid") or self.source_metadata.get("pubmed_id")
        if self.source == "pubmed":
            pmid = self.paper_id
        if pmid:
            return f"pmid:{_clean(pmid)}"

        # 4) Source-native IDs
        if self.source == "openalex":
            return f"openalex:{_clean(self.paper_id)}"
        if self.source == "semanticscholar":
            return f"s2:{_clean(self.paper_id)}"

        # 5) Deterministic fallback
        title = (self.title or "").strip().lower()
        year = str(self.year) if self.year is not None else ""
        first_author = (self.authors[0].strip().lower() if self.authors else "")
        raw = f"{title}|{year}|{first_author}"
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return f"hash:{digest}"

    @staticmethod
    def are_same_paper(canonical_id_a: str, canonical_id_b: str) -> bool:
        """
        Compare two canonical IDs with equivalence normalization
        (especially arXiv DOI <-> arxiv:<id>).
        """
        def normalize(cid: str) -> str:
            if not cid:
                return ""

            c = str(cid).strip().lower()

            # Normalize DOI URL/prefix forms
            if c.startswith("https://doi.org/"):
                c = "doi:" + c[len("https://doi.org/"):]
            elif c.startswith("http://doi.org/"):
                c = "doi:" + c[len("http://doi.org/"):]
            elif c.startswith("doi:"):
                c = c
            elif c.startswith("10."):
                c = "doi:" + c

            # Convert arXiv DOI to arxiv:<id>
            if c.startswith("doi:10.48550/arxiv."):
                arxiv_id = c.split("doi:10.48550/arxiv.", 1)[1]
                c = f"arxiv:{arxiv_id}"

            # Normalize arXiv version suffix
            if c.startswith("arxiv:"):
                aid = c.split("arxiv:", 1)[1]
                if "v" in aid:
                    left, right = aid.rsplit("v", 1)
                    if right.isdigit():
                        aid = left
                c = f"arxiv:{aid}"

            return c

        return normalize(canonical_id_a) == normalize(canonical_id_b)
    
    def to_dict(self):
        return {
            'paper_id': self.paper_id,
            'canonical_id': self.canonical_id,
            'title': self.title,
            'abstract': self.abstract,
            'authors': self.authors,
            'year': self.year,
            'source': self.source,
            'url': self.url,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'venue': self.venue,
            'doi': self.doi,
            'keywords': self.keywords,
            'categories': self.categories,
            'version': self.version,
            'updated_date': self.updated_date.isoformat() if self.updated_date else None,
            'references': self.references,
            'citation_count': self.citation_count,
            'embedding_ids': self.embedding_ids,
            'source_metadata': self.source_metadata,
            'extra_metadata': self.extra_metadata,
            'parsed': self.parsed,
            'embedded': self.embedded,
            'kg_extracted': self.kg_extracted,
        }
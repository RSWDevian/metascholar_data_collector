# Research Paper Data Collector

This repo is for collecting the required metadatas for MetaScholar.
A Python data ingestion pipeline to collect research paper metadata from:
- ArXiv 
- OpenAlex
- PubMed
- Semantic Scholar

Current primary focus is large-scale ArXiv collection with pagination + checkpoint/resume for long-running crawls.

## Project Structure

```
text
run_collectors.py
congif/
  ingestion_config.yaml
  sources.yaml
ingestion/
  collector/
  manager/
  normalization/
  queue/
  scheduler/
  storage/
  utils/
data/
  raw/
logs/
```

## Requirements

- Python 3.10+
- Internet access

Install dependencies:

```bash
pip install -r requirements.txt
```

If you don't have a `requirements.txt`, use:

```bash
pip install requests feedparser pyyaml
```

### Requirments.txt
```
PyYAML
requests
feedparser
```
The external dependencies required for this repo.

## Setting up the venv space
The whole project is set up in the venv space to localize the installations and resources of the project only to this repo.

Use the following commands to set up the virtual environment and to to install the external dependencies:

```
python -m venv venv
source venv/bin/activate
pip install -r requirments.txt
```

This shall prepare the repo for running the codedbase.

## Configuration of Source Collectors

Main source configuration: `congif/sources.yaml`

### ArXiv keys to be tuned

- `rate_limit`: requests per second limit via rate limiter
- `request_delay_seconds`: fixed sleep between paginated requests
- `request_delay_jitter_seconds`: random jitter added to delay
- `max_results_per_query`: page size per API call (typically up to 1000)
- `pagination.enabled`: turn pagination on/off
- `pagination.start`: initial offset
- `pagination.page_size`: pagination page size
- `pagination.max_requests`: hard cap per run
- `pagination.resume_from_checkpoint`: continue from saved offset
- `pagination.checkpoint_every_requests`: save progress every N calls
- `pagination.checkpoint_file`: checkpoint json path
- `categories`: list of arXiv archives/categories to crawl

### Semantic Scholar keys to be tuned

### OpenAlex keys to be tuned

### Pubmed keys to be tunned
- `rate_limit`: requests per second limit via rate limiter
- `request_delay_seconds`: fixed sleep between paginated requests
- `request_delay_jitter_seconds`: random jitter added to delay
- `pagination.enabled`: turn pagination on/off
- `pagination.start`: initial offset
- `pagination.page_size`: number of PubMed IDs fetched per search request (recommended: 200)
- `pagination.max_requests`: hard cap per run
- `pagination.resume_from_checkpoint`: continue from saved offset
- `pagination.checkpoint_every_requests`: save progress every N calls
- `pagination.checkpoint_file`: checkpoint json path

## Folders to setup
```
data/
  raw/
    arxiv/
    openalex/
    pubmed/
    semanticscholar/
```
This folder needs to be setup in the root space for the collection of the json data.

_(Note: Names can be changed but in that case we have to change the names at all the defined lcoations)_

## Run

Collect papers for a query from all enabled sources:

```bash
python run_collectors.py "machine learning"
```

Collect from only ArXiv:

```bash
python run_collectors.py "" --source arxiv
```

Show stored stats:

```bash
python run_collectors.py "" --stats
```

## Long-Run Crawling (ArXiv)

For near-complete collection:

1. Enable broad categories in `congif/sources.yaml` (e.g., `cs.*`, `math.*`, `stat.*`, `physics.*`, etc.)
2. Set a high `pagination.max_requests`
3. Keep conservative throttling (`rate_limit: 1`, delays enabled)
4. Keep checkpoint/resume enabled
5. Re-run the same command; crawl resumes from checkpoint until complete

Checkpoint file default:

```text
data/raw/arxiv/arxiv_checkpoint.json
```

## Output

Collected data is written under:

```text
data/raw/<source>/
```

Example:

- `data/raw/arxiv/`

## Notes

- ArXiv API/network reliability can vary; retries/checkpointing are important.
- "All papers" can take many hours to days depending on throttle and scope.
- Metadata collection is much faster than downloading full PDFs.

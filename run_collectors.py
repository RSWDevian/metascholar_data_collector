import argparse
from pathlib import Path

from ingestion.manager.collection_manager import CollectionManager
from ingestion.utils.logger import setup_logger


def _resolve_default_config() -> str:
    local_config = Path(__file__).resolve().parent / "config" / "sources.yaml"
    if local_config.exists():
        return str(local_config)
    return str(Path.cwd() / "config" / "sources.yaml")


def main():
    parser = argparse.ArgumentParser(description="Research Paper Collector")
    parser.add_argument(
        "query",
        type=str,
        help='Search query for papers (e.g., "machine learning" or "neural networks")',
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["arxiv", "openalex", "pubmed", "semanticscholar"],
        help="Specific source to collect from (default: all enabled sources)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to sources configuration file",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show collection statistics and exit",
    )
    parser.add_argument(
        "--category",
        action="append",
        default=None,
        help="ArXiv category filter. Use multiple times for multiple categories (e.g., --category cs.* --category stat.*)",
    )

    args = parser.parse_args()
    logger = setup_logger("main", "logs/collector.log")

    config_path = args.config or _resolve_default_config()
    if not Path(config_path).exists():
        raise FileNotFoundError(
            f"Config file not found at '{config_path}'. Pass explicit path with --config."
        )

    try:
        manager = CollectionManager(config_path)

        if args.stats:
            stats = manager.get_stats()
            logger.info("Collection Statistics:")
            for source, data in stats.items():
                logger.info(f"  {source}: {data['papers']} papers, {data['size_mb']:.2f} MB")
        else:
            logger.info(f"Starting collection for query: '{args.query}'")
            results = manager.collect_papers(
                args.query,
                source=args.source,
                categories=args.category,
            )

            logger.info("Collection Results:")
            for source, result in results.items():
                logger.info(f"  {source}: {result}")

            stats = manager.get_stats()
            logger.info("Updated Statistics:")
            for source, data in stats.items():
                logger.info(f"  {source}: {data['papers']} papers, {data['size_mb']:.2f} MB")

        manager.close()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
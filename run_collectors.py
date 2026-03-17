import argparse
import yaml
from ingestion.manager.collection_manager import CollectionManager
from ingestion.utils.logger import setup_logger

def main():
    parser = argparse.ArgumentParser(description="Research Paper Collector")
    parser.add_argument(
        'query',
        type=str,
        help='Search query for papers (e.g., "machine learning" or "neural networks")'
    )
    parser.add_argument(
        '--source',
        type=str,
        choices=['arxiv', 'openalex', 'pubmed', 'semanticscholar'],
        help='Specific source to collect from (default: all enabled sources)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/sources.yaml',
        help='Path to sources configuration file'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show collection statistics and exit'
    )
    
    args = parser.parse_args()
    logger = setup_logger("main", "logs/collector.log")
    
    try:
        manager = CollectionManager(args.config)
        
        if args.stats:
            stats = manager.get_stats()
            logger.info("Collection Statistics:")
            for source, data in stats.items():
                logger.info(f"  {source}: {data['papers']} papers, {data['size_mb']:.2f} MB")
        else:
            logger.info(f"Starting collection for query: '{args.query}'")
            results = manager.collect_papers(args.query, source=args.source)
            
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
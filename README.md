# Data Transformer

A simple PySpark pipeline that transforms insurance contracts and claims into transaction records.

## What it does

Takes CSV files with contracts and claims data, joins them together, and outputs transaction records with some business logic applied (like mapping claim types and generating hash IDs).

## Quick start

```bash
# Run everything with Docker
make docker-run

# Run tests  
make docker-test

# Development commands
make docker-build        # Build the image
make docker-shell        # Interactive shell for debugging
make docker-logs         # View container logs
make docker-clean        # Clean up containers and images
```

## Local development

```bash
# If you want to run without Docker
make run                 # Run pipeline locally
make test                # Run tests locally
```

## Requirements

- Docker
- Input files: `data/input/Contract.csv` and `data/input/Claim.csv`

## Configuration

Edit `.env` for basic settings like file paths and API URL.

## Output

Creates transaction records in `data/output/TRANSACTIONS/` folder. Invalid data gets saved separately so it doesn't break the pipeline.

## Design patterns used

### Template Method Pattern
The base `DataPipeline` class defines the standard flow: extract → validate → transform → load. Each concrete pipeline (like `ContractClaimPipeline`) implements the specific steps while following the same structure.

### Factory Pattern  
Creates the right components based on configuration:
- `DataExtractorFactory.create_extractor()` - handles different file formats (CSV, JSON, etc.)
- `LoaderFactory.create_loader()` - supports different output destinations
- `TransformerFactory` - provides business-specific transformers

This makes it easy to add new formats or change behavior without touching existing code.

## Notes

- Uses external API for generating hash IDs (with retry logic)
- Handles malformed data gracefully
- Configurable via environment variables
- Includes basic tests for reliability

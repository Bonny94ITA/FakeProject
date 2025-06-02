
from src.config import get_file_format_config
from src.pipeline.loading.base_loader import BaseLoader
from src.pipeline.loading.file_loader import FileLoader


class LoaderFactory:
    """Factory for creating data loaders with proper configuration."""

    @staticmethod
    def create_loader(output_dir: str) -> BaseLoader:
        """
        Creates a loader for the specified type and format.

        Args:
            output_dir: Output directory path

        Returns:
            BaseLoader: Concrete implementation based on type
        """
        # Get loader type from config
        file_config = get_file_format_config()
        loader_type = file_config["destination"].lower()

        # Factory logic - create loader based on type
        if loader_type == "file":
            return FileLoader(output_dir)
        elif loader_type == "s3":
            # Future: S3Loader(output_dir, config)
            raise NotImplementedError("S3 loader not yet implemented")
        elif loader_type == "database":
            # Future: DatabaseLoader(output_dir, config)
            raise NotImplementedError("Database loader not yet implemented")
        else:
            supported_types = ["file", "s3", "database"]
            raise ValueError(f"Unsupported loader type '{loader_type}'. Supported: {supported_types}")

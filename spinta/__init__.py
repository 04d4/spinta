import importlib.metadata

from .logging_config import setup_logging

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0.dev"


# Call the setup function to configure logging globally when the package is imported
setup_logging()

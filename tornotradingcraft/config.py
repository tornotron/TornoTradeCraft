from importlib import resources
from pathlib import Path

# Define the root of your package assets as a global, reusable Path object.
# Convert the Traversable to a Path for easier file system operations.
ASSETS_PATH: Path = Path(str(resources.files('tornotradingcraft') / 'assets'))
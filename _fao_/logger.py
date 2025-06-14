from loguru import logger
import sys
from pathlib import Path

# Remove default handler
logger.remove()

# Add console handler with custom format
logger.add(
    sys.stderr,
    format="<level>{level: <8}</level>[<cyan>{file.name}:{line} - {function}</cyan>]\n<level>{message}</level>\n",
    level="INFO",
    colorize=True,
)

# Add file handler with rotation
log_path = Path("logs")
log_path.mkdir(exist_ok=True)

logger.add(
    log_path / "app_{time:YYYY-MM-DD}.log",
    rotation="1 day",  # New file each day
    retention="1 month",  # Keep logs for 30 days
    level="DEBUG",  # File gets more detailed logs
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    backtrace=True,  # Include traceback in errors
    diagnose=True,  # Include variable values in traceback
    compression="zip",  # Compress old logs
)

# Add special handler for errors
logger.add(
    log_path / "errors.log", level="ERROR", rotation="1 week", retention="3 months", backtrace=True, diagnose=True
)


# Create child loggers for different modules
def get_logger(name: str):
    """Get a logger with a specific name/context"""
    return logger.bind(module=name)


# Export the base logger as default
__all__ = ["logger", "get_logger"]

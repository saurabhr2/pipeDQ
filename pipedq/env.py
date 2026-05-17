import sys
import logging

logger = logging.getLogger("pipedq.env")

def is_pyspark_active() -> bool:
    """
    Checks if PySpark is installed and a SparkSession is currently active.
    """
    if "pyspark" not in sys.modules:
        return False
        
    try:
        from pyspark.sql import SparkSession
        # getActiveSession() returns the active session or None if one doesn't exist
        return SparkSession.getActiveSession() is not None
    except ImportError:
        return False
    except Exception as e:
        logger.debug(f"Error checking for active SparkSession: {e}")
        return False

import importlib.util

def is_pandas_available() -> bool:
    return importlib.util.find_spec("pandas") is not None

def is_polars_available() -> bool:
    return importlib.util.find_spec("polars") is not None

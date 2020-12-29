try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    import importlib_metadata  # python <3.8

__version__ = importlib_metadata.version(__name__)

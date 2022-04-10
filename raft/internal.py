import logging

logger = logging.getLogger(__name__)

try:
    import trio
except ImportError:
    logger.info("Missing async features: install with `async` to enable")
    trio = None

import logging.config

try:
    from rich.logging import RichHandler

    RICH_HANDLING_ON = True
except ImportError:
    RICH_HANDLING_ON = False

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S:%M",
        },
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "raft": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "raft.io.transport": {
            "handlers": ["default"],
            "level": "WARN",
            "propagate": False,
        },
        "raft.models.server": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "__main__": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

if RICH_HANDLING_ON:
    LOGGING_CONFIG["handlers"]["default"] = {  # type: ignore
        "level": "INFO",
        "class": "rich.logging.RichHandler",
        "rich_tracebacks": True,
    }

logging.config.dictConfig(LOGGING_CONFIG)

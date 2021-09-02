import logging.config


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
        "raft": {  # root logger
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
            "level": "WARN",
            "propagate": False,
        },
        "__main__": {  # if __name__ == '__main__'
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
    },
}


logging.config.dictConfig(LOGGING_CONFIG)

LOG_MAP = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "[%(levelname)s] :%(asctime)s :Module: %(name)s: %(message)s",
            "datefmt": "%d/%m/%y %H:%M:%S",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        },
        "file_handler": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "simple",
            "maxBytes": 10485760,
            "backupCount": 20,
            "encoding": "utf8",
        },
    },
    "root": {"level": "DEBUG", "handlers": ["console", "file_handler"]},
}

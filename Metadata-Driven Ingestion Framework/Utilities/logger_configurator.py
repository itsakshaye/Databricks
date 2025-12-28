import logging


class LoggerConfigurator:

    def get_logger(self, logger_name: str, log_level: str = "INFO") -> logging.Logger:
        logger = logging.getLogger(logger_name)
        level_map = {
            "NOTSET": logging.NOTSET,
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }

        if logger.hasHandlers():
            logger.handlers.clear()
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s]: %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
        logger.setLevel(level_map.get(log_level.upper(), logging.INFO))
        return logger
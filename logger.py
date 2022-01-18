import logging
import sys


logging.basicConfig(
    format="'%(levelname)s - %(filename)s:%(lineno)d -- %(message)s'",
    stream=sys.stdout,
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)


class LoggerMixin:
    @property
    def _logger(self) -> logging.Logger:
        name = ".".join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from configlookup.main import Configuration

log = logging.getLogger(__name__)


class PipelineSinkException(Exception):
    def __init__(self, *args, **kwargs):
        super(Exception, self).__init__(*args, **kwargs)


class PipelineSinkConfigException(PipelineSinkException):
    def __init__(self, *args, **kwargs):
        super(PipelineSinkException, self).__init__(*args, **kwargs)


class PipelineSink(ABC):
    """
    abstract class to extend in order to implement various sinks
    """

    __DICT_TEXT_FRAGMENT_LENGTH = 36

    def __init__(self, config: Dict[str, Any]):
        log.info(f"[__init__|in] ({config})")
        self._validate_config(config)
        self.__config = config
        log.info("[__init__|out]")



    @staticmethod
    def _text_fragment(text: str, length: int = 10):
        log.info("[_text_fragment|in]")
        if text is None:
            raise ValueError("[_text_fragment] no text provided")
        substr_len = min(max(0, length - 3), len(text))
        result = text[0:substr_len] + "..."
        log.info(f"[_text_fragment|out] => {result}")
        return result

    def _get_config(self, entry):
        log.info(f"[_get_config|in] ({entry})")
        result = None
        try:
            config_entry = self.__config[entry]
            if "value" in config_entry.keys():
                result = config_entry["value"]
            elif "key" in config_entry.keys():
                result = Configuration.get(config_entry["key"])
            else:
                raise ValueError(f"[_get_config] no value or key defined in config entry: {entry}")
        except Exception as le:
            raise PipelineSinkException(f"[_get_config] config not found: {entry}") from le
        log.info(f"[_get_config|out] => {self._text_fragment(result)}")
        return result

    @abstractmethod
    def put(self, msg: str):
        """
        puts data into a sink

        Parameters
        ----------
        msg : str
            the msg to be sent, in a specific format,  json or other, the consumers will have to adapt
        """

    @abstractmethod
    def _validate_config(self, config: Dict[str, Any]):
        """
        puts data into a sink

        Raises
        ----------
        PipelineSinkConfigException
            exception with description of config failure
        """


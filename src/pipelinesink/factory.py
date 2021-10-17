import inspect
import logging
from importlib import import_module
from typing import Dict, Any

from pipelinesink.pipeline_sink import PipelineSink

logger = logging.getLogger(__name__)


class PipelineSinkFactory:

    @staticmethod
    def get(config: Dict[str, Any]) -> PipelineSink:
        logger.info(f"[PipelineSinkFactory.get|in] ({config})")

        classname = config["class"]
        module = config["module"]
        config = config["config"]

        callable_object = getattr(import_module(module), classname)
        if not inspect.isclass(callable_object):
            raise TypeError(f"[PipelineSinkFactory.get] not a class")
        else:
            if not issubclass(callable_object, PipelineSink):
                raise TypeError(f"[PipelineSinkFactory.get] Wrong class type, it is not a subclass of PipelineSink")

        logger.info(f"[PipelineSinkFactory.get] loading PipelineSink {module}.{classname}")
        instance = callable_object(config)
        logger.info(f"[PipelineSinkFactory.get|out] =>  {instance}")
        return instance

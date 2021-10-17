import logging
from abc import ABC
from typing import Dict, Any

from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy
)

from pipelinesink.pipeline_sink import PipelineSink, PipelineSinkConfigException

log = logging.getLogger(__name__)


class AzureStorageQueuePipelineSink(PipelineSink, ABC):

    def __init__(self, config: Dict[str, Any]) -> None:
        super(AzureStorageQueuePipelineSink, self).__init__(config=config)
        self.__client = None
        self.__queue_properties = None

    def _validate_config(self, config: Dict[str, Any]):
        config_keys = ["queue", "connection_string"]
        if not all(x in config.keys() for x in config_keys):
            raise PipelineSinkConfigException(f"expected keys: {config_keys} | actual keys: {config.keys()}")

    def _get_client(self):
        log.info("[AzureStorageQueuePipelineSink._get_client|in]")
        if not self.__client:
            queue_name = self._get_config("queue")
            connect_string = self._get_config("connection_string")
            self.__client = QueueClient.from_connection_string(conn_str=connect_string,
                                                               queue_name=queue_name,
                                                               message_encode_policy=BinaryBase64EncodePolicy(),
                                                               message_decode_policy=BinaryBase64DecodePolicy()
                                                               )
        log.info(f"[AzureStorageQueuePipelineSink._get_client|out] => {self.__client}")
        return self.__client

    def _assert_queue(self) -> bool:
        log.info("[AzureStorageQueuePipelineSink._assert_queue|in]")
        if not self.__queue_properties:
            result = False
            try:
                self.__queue_properties = self._get_client().get_queue_properties()
                result = True
            except Exception as ex:
                log.info(f"[AzureStorageQueuePipelineSink._assert_queue] queue not found: {ex}")
        else:
            result = True
        log.info(f"[AzureStorageQueuePipelineSink._assert_queue|out] => {result}")
        return result

    def _create_queue(self):
        log.info("[AzureStorageQueuePipelineSink._create_queue|in]")
        self._get_client().create_queue()
        log.info("[AzureStorageQueuePipelineSink._create_queue|out]")

    def put(self, msg: str):
        log.info(f"[AzureStorageQueuePipelineSink.put|in] ({self._text_fragment(msg) if msg else None})")
        if not self._assert_queue():
            self._create_queue()
        self._get_client().send_message(msg.encode(encoding='UTF-8', errors='replace'))
        log.info("[AzureStorageQueuePipelineSink.put|out]")









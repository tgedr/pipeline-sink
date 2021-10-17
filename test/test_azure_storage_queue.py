import json
import logging
import os

import pytest
from configlookup.main import Configuration

from pipelinesink.factory import PipelineSinkFactory

RESOURCES_DIR = f"{os.path.dirname(os.path.realpath(__file__))}/resources"


def test_azure_storage_queue_config_validation():
    with pytest.raises(Exception):
        PipelineSinkFactory.get(config={"gugu": "dada"})


# just for manual test purposes while we don't mock a storage account
@pytest.mark.skip
def test_assert_azure_storage_queue(monkeypatch):
    logger = logging.getLogger()
    logger.setLevel(level=logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    monkeypatch.setenv("CONFIGLOOKUP_DIR", RESOURCES_DIR)

    sink = PipelineSinkFactory.get(config=Configuration.get("pipelinesink.test_queue"))
    assert sink is not None, "no sink here"
    assert (not sink._assert_queue()), "it should not have the queue"


# just for manual test purposes while we don't mock a storage account
@pytest.mark.skip
def test_insert_in_azure_storage_queue(monkeypatch):
    logger = logging.getLogger()
    logger.setLevel(level=logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    monkeypatch.setenv("CONFIGLOOKUP_DIR", RESOURCES_DIR)
    sink = PipelineSinkFactory.get(config=Configuration.get("pipelinesink.test_queue"))

    message = json.dumps({"axe": "fdljasfaÊ"}, ensure_ascii=False)
    sink.put(msg=message)
    assert True, "oops, couldn't send message"


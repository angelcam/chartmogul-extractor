import asyncio
import json
import os
from logging import getLogger, basicConfig, INFO
import logging_gelf.handlers
import logging_gelf.formatters


from extractor.extractor import ChartMogulExtractor

logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(host=os.getenv('KBC_LOGGER_ADDR'),
                                                                  port=int(os.getenv('KBC_LOGGER_PORT')))
logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
basicConfig(level=INFO, handlers=[logging_gelf_handler], force=True)

logger = getLogger(__name__)

if __name__ == "__main__":
    logger.info("Loading configuration")
    #
    with open("/data/config.json", encoding="utf-8") as conf_fid:
        conf = json.load(conf_fid)
        conf = conf["parameters"]

    account_token = conf["account_token"]
    secret_key = conf["#secret_key"]
    logger.info("Configuration loaded")
    extractor = ChartMogulExtractor(account_token, secret_key, output_path='/data/out/tables')
    asyncio.run(extractor.extract())

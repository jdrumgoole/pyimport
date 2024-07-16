from pyimport.logger import Logger

if __name__ == "__main__":
    log = Logger(Logger.LOGGER_NAME).log()
    Logger.add_stream_handler()
    log.info("Started logtest")

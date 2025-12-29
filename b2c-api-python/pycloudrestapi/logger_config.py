import os
import logging
from logging.handlers import TimedRotatingFileHandler

def setup_logger():

    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = TimedRotatingFileHandler(filename=os.path.join(log_dir, 'pycloudrestapi.log'), when='midnight', backupCount=7)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

# Call the setup_logger function to configure the logger
logger = setup_logger()
import os
import logging


def init_directories():
    directory_name = "result"
    try:
        os.mkdir(directory_name)
        logging.info(f"Directory '{directory_name}' created successfully.")
    except FileExistsError:
        logging.info(f"Directory '{directory_name}' already exists.")

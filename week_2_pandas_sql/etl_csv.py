import pandas as pd
from etl_exception import EtlError
import logging
import os


def read_csv(csv_file_path: str):
    if not csv_file_path.startswith("/"):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        csv_file_path = f"{script_dir}/{csv_file_path}"
    try:
        logging.info("importing csv file:", csv_file_path)
        df = pd.read_csv(csv_file_path)
        return df
    except FileNotFoundError:
        logging.error("File doesn't exist! Check the file name/path", csv_file_path)
        raise EtlError
    except pd.errors.EmptyDataError:
        logging.error("File is empty!", csv_file_path)
        raise EtlError
    except UnicodeDecodeError:
        logging.error("File has unknown encoding...", csv_file_path)
        df = pd.read_csv('file_with_special_chars.csv', encoding='latin-1')
        raise EtlError
    except Exception as e:
        logging.error(f"Something went wrong: {e}")
        logging.error("Try checking if the file is corrupted or in a different format")
        raise EtlError
    

def write_to_csv(df: pd.DataFrame, filename: str):
    try:
        df.to_csv(filename, mode='a', index=False)
    except Exception as error:
        print("error on writing duplicates to csv file", error)
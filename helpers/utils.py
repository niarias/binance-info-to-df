import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import hashlib
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load the .env file
load_dotenv()


def connect_to_db():
    try:
        db = {
            "host": os.getenv('REDSHIFT_ENDPOINT'),
            "port": os.getenv('REDSHIFT_PORT'),
            "dbname": os.getenv('REDSHIFT_DATABASE'),
            "user": os.getenv('REDSHIFT_USER'),
            "pwd": os.getenv('REDSHIFT_PASSWD'),
            "schema": os.getenv('REDSHIFT_SCHEMA')

        }

        logging.info("Conectándose a la base de datos...")
        engine = create_engine(
            f"postgresql://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}", connect_args={"options": f"-c search_path={db['schema']}"}
        )

        logging.info(
            "Conexión a la base de datos establecida exitosamente")
        return engine

    except Exception as e:
        logging.error(f"Error al conectarse a la base de datos: {e}")
        return None


def load_to_sql(df, table_name, engine, if_exists="replace"):
    try:
        logging.info("Cargando datos en la base de datos...")
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method="multi"
        )
        logging.info("Datos cargados exitosamente en la base de datos")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")


def generate_hash(value):
    return hashlib.sha256(value.encode()).hexdigest()

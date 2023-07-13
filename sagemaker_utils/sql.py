import psycopg2
import boto3
#from sqlalchemy import create_engine
#import connectorx as cx
import numpy as np
from tqdm import tqdm


rs_connection = None
ssm = boto3.client('ssm')


def _load_params(
        username: str="mcontreras",
        user_parameter_name: str="user_ds_1",
        user_password_name: str="pass_ds_1"
        ):
    """
    El usuario y contrasena
    se obtienen de Parameter Store
    :return: Diccionario con parametros de conexion
    """
    cnx_params = {
        "username": ssm.get_parameter(Name=f"/sagemaker/redshift/{username}/{user_parameter_name}")['Parameter']['Value'],
        "password": ssm.get_parameter(Name=f"/sagemaker/redshift/{username}/{user_password_name}", WithDecryption=True)['Parameter']['Value']
                                      }

    if all(cnx_params.values()):
        return cnx_params
    else:
        raise Exception("Parametros de conexion a Redshift faltantes!")


def get_engine(engine: str="sqlalchemy"):
    "Returns sqlAlchemy engine for AWS prod database"
    # getting credentials
    username, password = _load_params().values()
    # values
    host = "redshift-privado-prod.c0iwf7ndlvaw.us-east-1.redshift.amazonaws.com"
    port = "5439"
    db = "prod"

    if engine == "sqlalchemy":
        eng = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    elif engine == "connectorx":
        eng = f'redshift://{username}:{password}@{host}:{port}/{db}'
    elif engine == "psycopg2":
        eng = psycopg2.connect(
            dbname=db, 
            host=host, 
            port=port, 
            user=username, 
            password=password
        )

    return eng

def execute_mogrify(conn, df, table_name):
    """
    Using cursor.mogrify() to build the bulk insert query
    then cursor.execute() to execute the query
    """

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x.tolist()) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    cursor = conn.cursor()
    n_cols = len(df.columns) # number of columns
    values_to_morgrify = f"({','.join(['%s' for _ in range(n_cols)])})" # "(%s, %s ... to n_cols)"
    values = [cursor.mogrify(values_to_morgrify, tup).decode('utf8') for tup in tuples]
    query  = "INSERT INTO %s(%s) VALUES " % (table_name, cols) + ",".join(values)
    
    try:
        cursor.execute(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


def insert_to_redshift(conn, df, table_name, batch_size :int=10_000):
    items = df.shape[0]
    if batch_size < items:
        BATCHES = np.ceil(items/batch_size)
    else:
        BATCHES = 1

    for i in tqdm(range(BATCHES)):
        start = i * batch_size
        end = (i + 1) * batch_size
        sub_df = df.iloc[start:end]
        # Executing    
        execute_mogrify(conn, sub_df, table_name)
    print("Insertion finished succesfully!")
    conn.close()


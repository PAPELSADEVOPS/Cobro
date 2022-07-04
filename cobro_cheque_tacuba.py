import pandas as pd
from datetime import date


from sqlalchemy import create_engine
from prefect import task, Flow


@task(log_stdout=True)
def extract():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    cobro_cheque = []
    df3 = pd.DataFrame(cobro_cheque)

    #Creando conexion con sqlalchemy
    engine = create_engine('mysql+pymysql://root:sys@10.10.1.101/siipapx?charset=utf8', encoding='latin1')

    #query = (f"SELECT * FROM cobro WHERE fecha='{today}' AND forma_de_pago='CHEQUE' AND tipo='CON'")
    query_cobro = (f"SELECT * FROM cobro WHERE fecha='2022-06-28' AND forma_de_pago='CHEQUE' AND tipo='CON'")
    df = pd.read_sql(query_cobro, engine)
    
    #query2 = (f"SELECT * FROM FICHA WHERE fecha='{today}' AND origen='CON' AND tipo_de_ficha='OTROS_BANCOS'")
    query_ficha = (f"SELECT * FROM FICHA WHERE fecha='2022-06-28' AND origen='CON' AND tipo_de_ficha='OTROS_BANCOS'")
    df2 = pd.read_sql(query_ficha, engine)

    for i in df['id']:
        query_cheque = (f'SELECT * FROM cobro_cheque WHERE cobro_id = "{i}"')
        row = pd.read_sql(query_cheque, engine)
        df3 = df3.append(row,ignore_index=True)
    

    return df, df2, df3


# Transformación
@task(log_stdout=True)
def transform(df):

    return df


# Carga
@task(log_stdout=True)
def load(df, df2, df3):
    engine_test = create_engine('mysql+pymysql://root:sys@10.10.1.229/siipapx_tacuba?charset=utf8', encoding='latin1')
    #engine_ofi = create_engine('mysql+pymysql://root:sys@10.10.1.229/siipapx?charset=utf8', encoding='latin1', echo=True)

    #df.to_sql(name='cobro', con=engine_ofi, index=False, if_exists='append')
    df.to_sql(name='cobro', con=engine_test, index=False, if_exists='append')

    #df2.to_sql(name='ficha', con=engine_ofi, index=False, if_exists='append')
    df2.to_sql(name='ficha', con=engine_test, index=False, if_exists='append')

    #df3.to_sql(name='cobro_cheque', con=engine_ofi, index=False, if_exists='append')
    df3.to_sql(name='cobro_cheque', con=engine_test, index=False, if_exists='append')

    


with Flow("Prueba") as flow:
    df = extract()[0]
    df2 = extract()[1]
    df3 = extract()[2]
    
    load(df, df2, df3)

flow.run()
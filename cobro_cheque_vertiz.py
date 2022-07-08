import mysql.connector


from datetime import date
from prefect import task, Flow


@task(log_stdout=True)
def extract():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    print(today)
    

    #Estableciendo conexón tacuba
    conn = mysql.connector.connect(
    user='root', password='sys', host='10.10.6.100', database='siipapx')

    #Creando cursor
    cursor = conn.cursor()

    # Sentencia SQL cobro
    sql_cobro = f"""SELECT * FROM cobro 
            WHERE fecha='{today}' 
            AND forma_de_pago='CHEQUE' 
            AND tipo='CON' 
            AND cliente_id='402880fc5e4ec411015e4ecc5dfc0554'
            """

    #Sentencia SQL ficha
    sql_cobro_cheque = f"""SELECT cq.* FROM cobro c join cobro_cheque cq on (c.id=cq.cobro_id) where c.fecha='{today}' 
                         AND c.forma_de_pago='CHEQUE' 
                         AND c.tipo='CON' 
                         AND c.cliente_id='402880fc5e4ec411015e4ecc5dfc0554'
                      """

    #Sentencia SQL ficha
    sql_ficha = f"""SELECT f.* FROM cobro c join cobro_cheque cq on (c.id=cq.cobro_id) join ficha f on (f.id=cq.ficha_id) 
                where c.fecha='{today}'
                AND c.forma_de_pago='CHEQUE' 
                AND c.tipo='CON' 
                AND c.cliente_id='402880fc5e4ec411015e4ecc5dfc0554'
                """

    try:
        # Executing the SQL command
        cursor.execute(sql_cobro)
        row_cobro = cursor.fetchall()
        #print(row)

        cursor.execute(sql_cobro_cheque)
        row_cobro_cheque = cursor.fetchall()

        cursor.execute(sql_ficha)
        row_ficha = cursor.fetchall()

    except:
        # Rollback en caso de Error
        conn.rollback()
    

    return row_cobro, row_cobro_cheque, row_ficha


# Carga
@task(log_stdout=True)
def load(row_cobro, row_cobro_cheque, row_ficha):
    #Estableciendo conexión con oficinas
    conn_ofi = mysql.connector.connect(
    user='root', password='sys', host='10.10.1.229', database='siipapx')

    #Cursor oficinas
    cursor_ofi = conn_ofi.cursor()

    #Estableciendo conexión con oficinas de prueba
    conn_test = mysql.connector.connect(
    user='root', password='sys', host='10.10.1.229', database='siipapx_tacuba')

    #Cursor oficinas
    cursor_test = conn_test.cursor()

    sql_insert_cobro = """INSERT IGNORE INTO cobro (id, version, diferencia_fecha, date_created, anticipo, forma_de_pago, last_updated, 
                     sucursal_id, referencia, moneda, TIPO_DE_CAMBIO, primera_aplicacion, update_user, diferencia, tipo, 
                     fecha, enviado, sw2, cliente_id, importe, create_user, comentario, cfdi_id, cancelacion_de_cfdi_id, 
                     cancelacion_motivo, recibo_anterior, anticipo_sat) VALUES
                     (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                  """

    sql_insert_ficha = """INSERT IGNORE INTO ficha (id,	version,	date_created,	total,	cuenta_de_banco_id,	last_updated,	tipo_de_ficha,
                  	sucursal_id,	fecha_corte,	origen,	folio,	fecha,	cancelada,	comentario,	ingreso_id,	sw2,	envio_valores,
                  	update_user,	create_user,	diferencia_tipo,	diferencia_usuario,	diferencia) VALUES
                     (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                  """

    sql_insert_cobro_cheque = """INSERT IGNORE INTO cobro_cheque (id,	version,	ficha_id,	date_created,	cobro_id,	cambio_por_efectivo,	
                     last_updated,	emisor,	numero_de_cuenta,	numero,	banco_origen_id,	vencimiento,	sw2,	nombre,	post_fechado) VALUES
                     (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                  """

    try:
        # Executing the SQL command

        #cursor_test.executemany(sql_insert_ficha, row_ficha)
        cursor_ofi.executemany(sql_insert_ficha, row_ficha)

        #cursor_test.executemany(sql_insert_cobro, row_cobro)
        cursor_ofi.executemany(sql_insert_cobro, row_cobro)

        #cursor_test.executemany(sql_insert_cobro_cheque, row_cobro_cheque)
        cursor_ofi.executemany(sql_insert_cobro_cheque, row_cobro_cheque)

        # Commit your changes in the database

        #conn_test.commit()
        conn_ofi.commit()

        #print(cursor_test.rowcount, "Filas insertadas en la tabla")
        print(cursor_ofi.rowcount, "Filas insertadas en la tabla")

    except mysql.connector.Error as error: 
        
        # Rolling back in case of error
        #conn_test.rollback()

        print("Error al insertar filas en la tabla {}".format(error))

    finally:
        #if conn_test.is_connected():
        #    cursor_test.close()
        #    conn_test.close()
        #    print("conexión con oficinas prueba Cerrada")

        if conn_ofi.is_connected():
            cursor_ofi.close()
            conn_ofi.close()
            print("conexión con oficinas Cerrada")


with Flow("cobro_cheque_vertiz") as flow:
    row_cobro = extract()[0]
    row_cobro_cheque = extract()[1]
    row_ficha = extract()[2]
    load(row_cobro, row_cobro_cheque, row_ficha)

flow.register(project_name="Cobro_cheque")
#flow.run()
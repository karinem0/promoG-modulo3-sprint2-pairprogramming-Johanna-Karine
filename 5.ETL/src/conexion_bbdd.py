#%%
# Importar librería para la conexión con MySQL
# -----------------------------------------------------------------------
import mysql.connector
from mysql.connector import errorcode


# Importar librerías para manipulación y análisis de datos
# -----------------------------------------------------------------------
import pandas as pd
import numpy as np


def creacion_bbdd (usuario, contrasenya):
    """Esta funcion crea la bbdd en mysql

    Args:
    - usuario: usuario para la conexion al servidor
    - contraseña: contraseña para la conexión al servidor

    Returns:
    No devuelve ningún valor
    """
    
    cnx = mysql.connector.connect(user=usuario, password=contrasenya,
                                host='127.0.0.1')


    mycursor = cnx.cursor()
    query = "CREATE SCHEMA RESTAURANT"

    try: 
        mycursor.execute(query) 
    
        print("BBDD creada")

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)


def creacion_tablas(usuario, contrasenya, bbdd):
    """Esta funcion crea las tablas empleado, empleado_empresa y registros en mysql

    Args:
    - usuario: usuario para la conexion al servidor
    - contraseña: contraseña para la conexión al servidor
    - bbdd: nombre de la bbdd donde queremos crear las tablas

    Returns:
    No devuelve ningún valor
    """
    
    cnx = mysql.connector.connect(user=usuario, password=contrasenya,
                                host='127.0.0.1', database=bbdd)


    # tabla clientes: campos relacionados con los clientes que no varian en los distintos registros
    mycursor = cnx.cursor()
    query = """
                CREATE TABLE `clientes` (
                    id_cliente INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    gender VARCHAR(50) DEFAULT NULL,
                    city VARCHAR(100) DEFAULT NULL,
                    address VARCHAR(255) DEFAULT NULL,
                    fecha_venta DATE NOT NULL,
                    cantidad INT DEFAULT NULL,
                    total FLOAT DEFAULT NULL
                )
                """
    try: 
        mycursor.execute(query)
    
        print("Tabla clientes creada")

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)
        
        
    mycursor = cnx.cursor()
    query = """
                CREATE TABLE `productos` (
                    id_producto INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    nombre_producto VARCHAR(100) NOT NULL,
                    clasificacion_plato VARCHAR(100) DEFAULT NULL,
                    categoria FLOAT DEFAULT NULL,
                    origen VARCHAR(100) DEFAULT NULL,
                    descripcion VARCHAR(255) DEFAULT NULL
                )
                """
    try: 
        mycursor.execute(query)
    
        print("Tabla productos creada")

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    # tabla pedidos: campos que relacionan al empleado con la empresa que no varian en los distintos registros
    mycursor = cnx.cursor()
    query = """
            CREATE TABLE pedidos (
                id_pedido INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                id_cliente INT NOT NULL,
                id_producto INT NOT NULL,
                fecha_pedido DATE NOT NULL,
                cantidad INT DEFAULT NULL,
                total FLOAT DEFAULT NULL,
                FOREIGN KEY (id_cliente) REFERENCES clientes (id_cliente),
                FOREIGN KEY (id_producto) REFERENCES productos (id_producto)
            )     """
            
    try: 
        mycursor.execute(query)
        
        print("Tabla pedidos creada")

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    cnx.close()


##  QUERYS DE INSERCION DE DATOS ##
query_insertar_clientes = """ INSERT INTO clientes (first_name, last_name, email, gender, city, address, fecha_venta, cantidad, total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
query_insert_productos = """ INSERT INTO productos (nombre_producto, clasificacion_plato, categoria, origen, descripcion) VALUES (%s, %s, %s, %s, %s)"""
query_insertar_pedidos = """INSERT INTO pedidos (id_cliente, id_producto, fecha_pedido, cantidad, total) VALUES (%s, %s, %s, %s, %s)"""

def insertar_datos(query, contraseña, nombre_bbdd, lista_tuplas):
    """
    Inserta datos en una base de datos utilizando una consulta y una lista de tuplas como valores.

    Args:
    - query (str): Consulta SQL con placeholders para la inserción de datos.
    - contraseña (str): Contraseña para la conexión a la base de datos.
    - nombre_bbdd (str): Nombre de la base de datos a la que se conectará.
    - lista_tuplas (list): Lista que contiene las tuplas con los datos a insertar.

    Returns:
    No devuelve ningún valor, pero inserta los datos en la base de datos.

    """
    cnx = mysql.connector.connect(
        user="root", 
        password=contraseña, 
        host="127.0.0.1", database=nombre_bbdd
    )

    mycursor = cnx.cursor()

    try:
        mycursor.executemany(query, lista_tuplas)
        cnx.commit()
        print(mycursor.rowcount, "registro/s insertado/s.")
        cnx.close()

    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)
        cnx.close()


# %%

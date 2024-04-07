#%%
# main.py 

import pandas as pd
import conexion_bbdd as conexion
from funciones_eda import cargar_dataframes, union_dataframes, exploracion_dataframe, configurar_visualizacion, imputar_valores_nulos, eliminar_columnas, transformar_nombres_columnas

# Rutas de los archivos CSV
ruta_clientes = "../data/clientes.csv"
ruta_productos = "../data/productos.csv"
ruta_ventas = "../data/ventas.csv"

# Cargar los DataFrames desde los archivos CSV
df_clientes, df_productos, df_ventas = cargar_dataframes(ruta_clientes, ruta_productos, ruta_ventas)

# Unir los DataFrames
df_final = union_dataframes(df_clientes, df_productos, df_ventas)

# Explorar el DataFrame resultante
exploracion_dataframe(df_final, columna_control="ID")

# Configurar la visualización de pandas
configurar_visualizacion()

# Imputar valores nulos en columnas específicas
columnas_desconocido = ["email", "gender", "City", "Country", "Address", "Descripción"]
df_final = imputar_valores_nulos(df_final, columnas_desconocido)

# Eliminar columnas no deseadas
columnas_eliminar = ["id", "index", "Precio", "Country"]
eliminar_columnas(df_final, columnas_eliminar)

# Transformar nombres de columnas
df_final = transformar_nombres_columnas(df_final)

# Mostrar DataFrame final
print(df_final.head())

# %%
# Creamos la BBDD 
conexion.creacion_bbdd("root", "AlumnaAdalab")

#%%
conexion.creacion_tablas("root", "AlumnaAdalab", "RESTAURANT")

# Convertir los DataFrames a listas de tuplas para la inserción en la base de datos
lista_clientes = df_final[['first_name', 'last_name', 'email', 'gender', 'city', 'address', 'fecha_venta', 'cantidad', 'total']].to_records(index=False).tolist()
lista_pedidos = df_final[['id_cliente', 'fecha_venta', 'cantidad', 'total']].to_records(index=False).tolist()
lista_productos = df_final[['id_producto','nombre_producto', 'clasificacion_plato', 'categoría', 'origen', 'descripción']].to_records(index=False).tolist()
# Insertar datos en la base de datos
conexion.insertar_datos(conexion.query_insertar_clientes, "AlumnaAdalab", "RESTAURANT", lista_clientes)
conexion.insertar_datos(conexion.query_insert_productos, "AlumnaAdalab", "RESTAURANT", lista_productos)
conexion.insertar_datos(conexion.query_insertar_pedidos, "AlumnaAdalab", "RESTAURANT", lista_pedidos)


# %%
print(df_final.columns)

# %%

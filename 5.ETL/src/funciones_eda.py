#%% 
# funciones.py
# -----------------------------------------------------------------------
import pandas as pd
import numpy as np

# Visualización
# ------------------------------------------------------------------------------
import matplotlib.pyplot as plt
import seaborn as sns

# Evaluar linealidad de las relaciones entre las variables
# y la distribución de las variables
# ------------------------------------------------------------------------------
import scipy.stats as stats

# Configuración
# -----------------------------------------------------------------------
pd.set_option('display.max_columns', None) # para poder visualizar todas las columnas de los DataFrames

# Gestión de los warnings
# -----------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore")



def cargar_dataframes(ruta_csv1, ruta_csv2, ruta_csv3):
    """Carga los dataframes desde archivos CSV.

    Args:
    - ruta_flight (str): La ruta al archivo CSV de Customer Flight Activity.
    - ruta_loyalty (str): La ruta al archivo CSV de Customer Loyalty History.

    Returns:
    - df_flight (DataFrame): El dataframe cargado desde el archivo CSV de Customer Flight Activity.
    - df_loyalty (DataFrame): El dataframe cargado desde el archivo CSV de Customer Loyalty History.
    """
    # Cargar los DataFrames desde archivos CSV y resetear los índices
    df_clientes = pd.read_csv(ruta_csv1, index_col=0).reset_index()
    df_productos = pd.read_csv(ruta_csv2, index_col=0, on_bad_lines="warn").reset_index()
    df_ventas = pd.read_csv(ruta_csv3, index_col=0).reset_index()
    
    return df_clientes, df_productos, df_ventas


def union_dataframes(df1, df2, df3):
    """
    Unimos los DataFrames utilizando la función merge de pandas.
    Args:
    - df1 (DataFrame): El primer DataFrame a unir.
    - df2 (DataFrame): El segundo DataFrame a unir.
    - df3 (DataFrame): El tercer DataFrame a unir.

    Returns:
    - df (DataFrame): El DataFrame resultante de la unión de los tres DataFrames.
    """
    # Unir los DataFrames df1 y df2 utilizando merge
    df_temp = pd.merge(df1, df2, left_index=True, right_index=True)

    # Unir el DataFrame temporal con df3
    df = pd.merge(df_temp, df3, left_index=True, right_index=True)

    return df


def exploracion_dataframe(dataframe, columna_control):
    """
    Realiza un análisis exploratorio básico de un DataFrame, mostrando información sobre duplicados,
    valores nulos, tipos de datos, valores únicos para columnas categóricas y estadísticas descriptivas
    para columnas categóricas y numéricas, agrupadas por la columna de control.

    Parámetros:
    - dataframe (DataFrame): El DataFrame que se va a explorar.
    - columna_control (str): El nombre de la columna que se utilizará como control para dividir el DataFrame.

    Returns: 
    No devuelve nada directamente, pero imprime en la consola la información exploratoria.
    """
    
    # Imprimir el número de duplicados en el DataFrame
    print(f"Los duplicados que tenemos en el conjunto de datos son: {dataframe.duplicated().sum()}")
    print("\n ..................... \n")
    
    # Generar un DataFrame para los valores nulos
    print("Los nulos que tenemos en el conjunto de datos son:")
    df_nulos = pd.DataFrame(dataframe.isnull().sum() / dataframe.shape[0] * 100, columns = ["%_nulos"])
    display(df_nulos[df_nulos["%_nulos"] > 0])
    
    print("\n ..................... \n")
    # Mostrar los tipos de datos de las columnas
    print(f"Los tipos de las columnas son:")
    display(pd.DataFrame(dataframe.dtypes, columns = ["tipo_dato"]))
    
    
    print("\n ..................... \n")
    print("Los valores que tenemos para las columnas categóricas son: ")
    # Seleccionar las columnas de tipo objeto (categóricas)
    dataframe_categoricas = dataframe.select_dtypes(include = "O")
    
    for col in dataframe_categoricas.columns:
        # Imprimir los valores únicos para cada columna categórica
        print(f"La columna {col.upper()} tiene las siguientes valore únicos:")
        display(pd.DataFrame(dataframe[col].value_counts()).head())    
    
    
    for categoria in dataframe[columna_control].unique():
        # Filtrar el DataFrame por categoría y mostrar estadísticas descriptivas
        dataframe_filtrado = dataframe[dataframe[columna_control] == categoria]
    
        print("\n ..................... \n")
        print(f"Los principales estadísticos de las columnas categóricas para el {categoria.upper()} son: ")
        display(dataframe_filtrado.describe(include = "O").T)
        
        print("\n ..................... \n")
        print(f"Los principales estadísticos de las columnas numéricas para el {categoria.upper()} son: ")
        display(dataframe_filtrado.describe().T)
        
        
def configurar_visualizacion():
    """Configura la visualización de pandas para mostrar todas las columnas y formato de los números."""
    # Configurar opciones de visualización de pandas
    pd.set_option('display.max_columns', None)
    pd.set_option('display.float_format', '{:.2f}'.format)
    


def imputar_valores_nulos(df, columnas_desconocido):
    """Imputa valores nulos en una lista de columnas del DataFrame utilizando 'fillna' con 'Unknown'.

    Args:
    - df (DataFrame): El DataFrame en el que se imputarán los valores nulos.
    - columnas_desconocido (list): Lista de nombres de las columnas en las que se imputarán los valores nulos.
    """
    # Iterar sobre la lista de columnas a las que se les cambiarán los nulos por "Unknown"
    for columna in columnas_desconocido:
        # Reemplazar los nulos por el valor "Unknown" para cada una de las columnas de la lista
        df[columna] = df[columna].fillna("Unknown")
        
    # Comprobar si quedan nulos en las columnas categóricas
    print("Después del reemplazo usando 'fillna', quedan los siguientes nulos:")
    print(df[columnas_desconocido].isnull().sum())
    
    return df


def eliminar_columnas(dataframe, columnas):
    """Elimina columnas del DataFrame.

    Args:
    - dataframe (DataFrame): El DataFrame del que se eliminarán las columnas.
    - columnas (list): La lista de nombres de las columnas que se eliminarán.
    """
    # Eliminar las columnas especificadas
    dataframe.drop(columns=columnas, inplace=True)



def transformar_nombres_columnas(dataframe):
    """Transforma los nombres de las columnas del DataFrame.

    Args:
    - dataframe (DataFrame): El DataFrame cuyos nombres de columnas se transformarán.
    """
    # Transformar los nombres de las columnas a minúsculas y reemplazar espacios en blanco por guiones bajos
    nuevas_columnas = [col.replace(" ", '_').lower() for col in dataframe.columns]
    dataframe.columns = nuevas_columnas
    # Renombrar columnas específicas
    dataframe.rename(columns={ 'nombre_producto': 'clasificacion_plato', 'id': 'nombre_producto',}, inplace=True)
    
    return dataframe

# %%

import pandas as pd
import requests
import json
import pyodbc

def extract_from_sql_server(server_name, database_name, table_name):
    print(f"--- Extrayendo datos de SQL Server: {server_name}/{database_name}.{table_name} ---")
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server_name};"
        f"DATABASE={database_name};"
        f"TRUSTED_CONNECTION=yes;"
    )
    try:
        cnxn = pyodbc.connect(conn_str)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, cnxn)
        cnxn.close()
        print("Datos extraídos de SQL Server (Muestra):")
        print(df.head())
        return df
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error al conectar o extraer de SQL Server: {sqlstate}")
        print("Asegúrate de tener el 'ODBC Driver 17 for SQL Server' instalado y que el servidor esté accesible.")
        return None

def extract_from_cepalstat():
    print("\n--- Extrayendo datos de CEPALSTAT API ---")
    indicator_id = 3351  # Emisiones de GEI por sector
    area_id = 221       # Bolivia
    sector_id = 63374   # Energía (incluye Transporte)

    url = f"https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/records?lang=es&in_id_area={area_id}"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Levanta una excepción para códigos de estado HTTP erróneos
        data = response.json()
        records = data["body"]["data"]

        # Filtrar por sector Energía (dim_63371 = 63374)
        filtered_records = [r for r in records if r.get("dim_63371") == sector_id]

        # Mapeo manual de años basado en nuestra investigación previa
        year_mapping = {
            29183: "2013", 29184: "2014", 29185: "2015", 29186: "2016",
            29187: "2017", 29188: "2018", 29189: "2019", 29190: "2020",
            29191: "2021", 29192: "2022"
        }

        df_cepal = pd.DataFrame(filtered_records)
        df_cepal["Anio"] = df_cepal["dim_29117"].map(year_mapping)
        df_cepal = df_cepal[["Anio", "value"]].dropna()
        df_cepal.columns = ["Anio", "Emisiones_MtCO2e"]
        df_cepal["Emisiones_MtCO2e"] = pd.to_numeric(df_cepal["Emisiones_MtCO2e"])

        print("Datos extraídos de CEPALSTAT (Bolivia - Sector Energía):")
        print(df_cepal.sort_values("Anio").tail())
        return df_cepal
    except requests.exceptions.RequestException as e:
        print(f"Error de red o HTTP al extraer de CEPALSTAT: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON de CEPALSTAT: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado en extracción CEPALSTAT: {e}")
        return None

def clean_and_transform_data(df_entregas):
    print("\n--- Limpiando y transformando datos de Entregas_Bronze ---")
    if df_entregas is None:
        print("DataFrame de entregas es None, no se puede limpiar.")
        return None

    print("[INFO] Inicio de limpieza")
    print(f"[INFO] Registros de entrada: {len(df_entregas)}")
    print(f"[INFO] Columnas disponibles: {list(df_entregas.columns)}")

    total_detectados = 0
    total_arreglados = 0
    total_pendientes = 0

    # 1. Tratamiento de Nulos en Tiempo_Idling_Minutos
    # Rellenar con 0 o la media, dependiendo de la lógica de negocio. Aquí usaremos 0.
    print("\n[ETAPA 1/4] Tratamiento de nulos en Tiempo_Idling_Minutos")
    nulos_idling_antes = df_entregas["Tiempo_Idling_Minutos"].isnull().sum()
    print(f"[ANTES] Nulos en Tiempo_Idling_Minutos: {nulos_idling_antes}")
    df_entregas["Tiempo_Idling_Minutos"] = df_entregas["Tiempo_Idling_Minutos"].fillna(0).astype(int)
    nulos_idling_despues = df_entregas["Tiempo_Idling_Minutos"].isnull().sum()
    arreglados_idling = nulos_idling_antes - nulos_idling_despues
    pendientes_idling = nulos_idling_despues
    total_detectados += nulos_idling_antes
    total_arreglados += arreglados_idling
    total_pendientes += pendientes_idling
    print(f"[DESPUES] Nulos en Tiempo_Idling_Minutos: {nulos_idling_despues}")
    print(f"[METRICA] Detectados: {nulos_idling_antes} | Arreglados: {arreglados_idling} | Pendientes: {pendientes_idling}")

    # 2. Normalización de Estado_Entrega
    # Eliminar espacios extra, convertir a minúsculas y luego a formato título.
    print("\n[ETAPA 2/4] Normalización de Estado_Entrega")
    nulos_estado_antes = df_entregas["Estado_Entrega"].isnull().sum()
    print(f"[ANTES] Nulos en Estado_Entrega: {nulos_estado_antes}")
    print(f"[ANTES] Valores únicos en Estado_Entrega: {df_entregas['Estado_Entrega'].dropna().unique()}")
    df_entregas["Estado_Entrega"] = df_entregas["Estado_Entrega"].str.strip().str.lower().str.title()
    
    # Estandarizar valores si hay variaciones (ej. 'Cancelado' vs 'Anulado')
    # Para este ejemplo, asumimos que 'Cancelado' y 'En Proceso' son los principales
    df_entregas["Estado_Entrega"] = df_entregas["Estado_Entrega"].replace({
        "Cancelado": "Cancelado",
        "En proceso": "En Proceso",
        "Entregado": "Entregado"
    })
    # Manejar posibles nulos introducidos por el replace si el valor original era None
    df_entregas["Estado_Entrega"] = df_entregas["Estado_Entrega"].fillna("Desconocido")
    nulos_estado_despues = df_entregas["Estado_Entrega"].isnull().sum()
    arreglados_estado = nulos_estado_antes - nulos_estado_despues
    pendientes_estado = nulos_estado_despues
    total_detectados += nulos_estado_antes
    total_arreglados += arreglados_estado
    total_pendientes += pendientes_estado
    print(f"[DESPUES] Nulos en Estado_Entrega: {nulos_estado_despues}")
    print(f"[METRICA] Detectados: {nulos_estado_antes} | Arreglados: {arreglados_estado} | Pendientes: {pendientes_estado}")
    print(f"Valores únicos en Estado_Entrega después de limpieza: {df_entregas['Estado_Entrega'].unique()}")

    # 3. Conversión de tipos de datos si es necesario (ej. Fecha_Hora a datetime)
    print("\n[ETAPA 3/4] Conversión de Fecha_Hora a datetime")
    nulos_fecha_antes = df_entregas["Fecha_Hora"].isnull().sum()
    total_detectados += nulos_fecha_antes
    print(f"[ANTES] Tipo de Fecha_Hora: {df_entregas['Fecha_Hora'].dtype}")
    print(f"[ANTES] Nulos en Fecha_Hora: {nulos_fecha_antes}")
    df_entregas["Fecha_Hora"] = pd.to_datetime(df_entregas["Fecha_Hora"])
    nulos_fecha_despues = df_entregas["Fecha_Hora"].isnull().sum()
    arreglados_fecha = nulos_fecha_antes - nulos_fecha_despues
    pendientes_fecha = nulos_fecha_despues
    total_arreglados += arreglados_fecha
    total_pendientes += pendientes_fecha
    print(f"[DESPUES] Tipo de Fecha_Hora: {df_entregas['Fecha_Hora'].dtype}")
    print(f"[DESPUES] Nulos en Fecha_Hora: {nulos_fecha_despues}")
    print(f"[METRICA] Detectados: {nulos_fecha_antes} | Arreglados: {arreglados_fecha} | Pendientes: {pendientes_fecha}")

    # 4. Validaciones de Calidad de Datos (Asserts)
    # Asegurar que no haya nulos en columnas críticas después de la limpieza
    print("\n[ETAPA 4/4] Validaciones de calidad")
    print(f"[CHECK] Nulos en ID_Entrega: {df_entregas['ID_Entrega'].isnull().sum()}")
    print(f"[CHECK] Nulos en Fecha_Hora: {df_entregas['Fecha_Hora'].isnull().sum()}")
    print(f"[CHECK] Nulos en Estado_Entrega: {df_entregas['Estado_Entrega'].isnull().sum()}")
    emisiones_negativas = (df_entregas["Emisiones_CO2_KG"] < 0).sum()
    total_detectados += emisiones_negativas
    total_pendientes += emisiones_negativas
    print(f"[CHECK] Emisiones_CO2_KG negativas: {emisiones_negativas}")
    assert df_entregas["ID_Entrega"].isnull().sum() == 0, "Error: ID_Entrega contiene valores nulos."
    assert df_entregas["Fecha_Hora"].isnull().sum() == 0, "Error: Fecha_Hora contiene valores nulos."
    assert df_entregas["Estado_Entrega"].isnull().sum() == 0, "Error: Estado_Entrega contiene valores nulos."
    assert (df_entregas["Emisiones_CO2_KG"] >= 0).all(), "Error: Emisiones_CO2_KG contiene valores negativos."

    print("\n[OK] Todas las validaciones pasaron correctamente")
    print("Limpieza y transformación completadas exitosamente.")
    print("Datos limpios de Entregas (Muestra):")
    print(df_entregas.head())
    print(f"[RESUMEN] Registros finales: {len(df_entregas)}")
    print(f"[RESUMEN CALIDAD] Problemas detectados: {total_detectados}")
    print(f"[RESUMEN CALIDAD] Problemas arreglados: {total_arreglados}")
    print(f"[RESUMEN CALIDAD] Problemas pendientes: {total_pendientes}")
    return df_entregas

if __name__ == "__main__":
    # Configuración de conexión a SQL Server
    SERVER_NAME = "LAPTOP-0FE1UK8O"
    DATABASE_NAME = "LogisticaUltimaMilla"
    TABLE_NAME = "Entregas_Bronze"

    # Extracción de SQL Server
    df_entregas_bronze = extract_from_sql_server(SERVER_NAME, DATABASE_NAME, TABLE_NAME)

    # Extracción de CEPALSTAT
    df_cepalstat_bronze = extract_from_cepalstat()

    # Limpieza y Transformación (Capa Silver)
    df_entregas_silver = clean_and_transform_data(df_entregas_bronze)

    # Guardar datos limpios para la Capa Silver
    if df_entregas_silver is not None:
        df_entregas_silver.to_csv("silver_entregas.csv", index=False)
        print("\nDatos de entregas limpios guardados en 'silver_entregas.csv'")
    
    if df_cepalstat_bronze is not None:
        df_cepalstat_bronze.to_csv("silver_cepalstat.csv", index=False)
        print("Datos de CEPALSTAT guardados en 'silver_cepalstat.csv'")

    print("\nCapa Silver completada: Archivos 'silver_entregas.csv' y 'silver_cepalstat.csv' generados.")

# =============================================================
#  CAPA SILVER - Logistica de Ultima Milla en Bolivia
#  Materia: Inteligencia de Negocios | UPDS - Grupo Familia Mallku
#  Actividad 8: Sprint de Refinamiento
#
#  Proceso:
#    1. Extrae datos "sucios" de la Capa Bronze (SQL Server)
#    2. Integra indicadores externos de CEPALSTAT (CSV)
#    3. Limpia, normaliza y valida (0 nulos en columnas criticas)
#    4. Carga el resultado limpio al Data Warehouse (SQL Server)
#
#  Ejecutar: python capa_silver.py
#  Requisitos: pip install pandas sqlalchemy pyodbc
# =============================================================

import warnings

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# CONFIGURACION - Ajustar segun laboratorio
# ─────────────────────────────────────────────────────────────
#

SERVIDOR = "LAPTOP-0FE1UK8O"  # Cambiar al nombre de tu instancia
DB_BRONZE = "LogisticaUltimaMilla"
DB_GOLD = "LogisticaUltimaMilla"  # Mismo servidor, tablas Silver cargadas ahi
CEPAL_CSV = "silver_cepalstat.csv"  # Archivo con datos CEPALSTAT de Bolivia


# ─────────────────────────────────────────────────────────────
# PASO 0: Conexion a la Capa Bronze
# ─────────────────────────────────────────────────────────────
def get_engine(database: str):
    # Construye engine SQLAlchemy para SQL Server con autenticacion Windows.
    conn_str = (
        f"mssql+pyodbc://{SERVIDOR}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
        "&TrustServerCertificate=yes"
    )
    return create_engine(conn_str, fast_executemany=True)




# Uso
engine_bronze = get_engine(DB_BRONZE)
engine_gold = get_engine(DB_GOLD)


print("=" * 60)
print("  CAPA SILVER - Logistica Ultima Milla Bolivia")
print("=" * 60)

engine_bronze = get_engine(DB_BRONZE)
print(f"[OK] Conexion establecida a Bronze: {DB_BRONZE}")

# ─────────────────────────────────────────────────────────────
# PASO 1: EXTRACCION desde Capa Bronze
# ─────────────────────────────────────────────────────────────
print("\n[1/5] Extrayendo datos de Capa Bronze...")

SQL_BRONZE = """
SELECT
    e.EntregaID,
    e.FechaEntrega,
    e.HoraSalida,
    e.HoraLlegada,
    e.DistanciaKM,
    e.ConsumoCombLitros,
    e.PesoCargaKG,
    e.TiempoIdlingMin,
    e.EstadoEntrega,
    e.ObservacionRaw,
    -- Vehiculo
    v.Placa,
    v.Tipo              AS TipoVehiculo,
    v.Modelo            AS ModeloVehiculo,
    v.CapacidadMaxKG,
    v.FactorEmisionKgCO2PorLitro,
    -- Conductor
    c.Nombre            AS NombreConductor,
    c.LicenciaCateg,
    -- Cliente
    cl.NombreEmpresa    AS NombreCliente,
    cl.Segmento         AS SegmentoCliente,
    -- Origen
    uo.Ciudad           AS CiudadOrigen,
    do.Nombre           AS DepartamentoOrigen,
    do.Region           AS RegionOrigen,
    -- Destino
    ud.Ciudad           AS CiudadDestino,
    dd.Nombre           AS DepartamentoDestino,
    dd.Region           AS RegionDestino
FROM Entrega e
INNER JOIN Vehiculo  v  ON v.VehiculoID  = e.VehiculoID
INNER JOIN Conductor c  ON c.ConductorID = e.ConductorID
INNER JOIN Cliente   cl ON cl.ClienteID  = e.ClienteID
INNER JOIN Ubicacion uo ON uo.UbicacionID = e.UbicacionOrigenID
INNER JOIN Ubicacion ud ON ud.UbicacionID = e.UbicacionDestinoID
INNER JOIN Departamento do ON do.DepartamentoID = uo.DepartamentoID
INNER JOIN Departamento dd ON dd.DepartamentoID = ud.DepartamentoID;
"""

df_raw = pd.read_sql(SQL_BRONZE, engine_bronze)
print(f"    Filas extraidas  : {len(df_raw):,}")
print(f"    Columnas         : {len(df_raw.columns)}")

# ─────────────────────────────────────────────────────────────
# PASO 2: DIAGNOSTICO DE SUCIEDAD (Antes de limpiar)
# ─────────────────────────────────────────────────────────────
print("\n[2/5] Diagnostico de suciedad (estado ANTES):")

nulos_antes = df_raw.isnull().sum()
nulos_antes = nulos_antes[nulos_antes > 0]
print(f"\n    Columnas con nulos:\n{nulos_antes.to_string()}")

# Detectar nombres con espacios extras
nombres_sucios = (
    df_raw["NombreConductor"].str.contains(r"\s{2,}|^\s|\s$", regex=True).sum()
)
print(f"\n    Conductores con espacios extras en nombre : {nombres_sucios}")
clientes_sucios = (
    df_raw["NombreCliente"].str.contains(r"\s{2,}|^\s|\s$", regex=True).sum()
)
print(f"    Clientes con espacios extras en nombre   : {clientes_sucios}")

print(f"\n    Info del DataFrame raw:")
df_raw.info(verbose=False, memory_usage="deep")

# ─────────────────────────────────────────────────────────────
# PASO 3: LIMPIEZA Y NORMALIZACION (Scrubbing)
# ─────────────────────────────────────────────────────────────
print("\n[3/5] Ejecutando limpieza y normalizacion...")

df = df_raw.copy()

# -- 3a. Tipos de datos basicos --
df["FechaEntrega"] = pd.to_datetime(df["FechaEntrega"])
df["Anio"] = df["FechaEntrega"].dt.year
df["Mes"] = df["FechaEntrega"].dt.month
df["NombreMes"] = df["FechaEntrega"].dt.strftime("%B")
df["DiaSemana"] = df["FechaEntrega"].dt.day_name()

# -- 3b. Normalizar texto: STRIP + eliminar espacios dobles + Title Case --
cols_texto = [
    "NombreConductor",
    "NombreCliente",
    "CiudadOrigen",
    "CiudadDestino",
    "DepartamentoOrigen",
    "DepartamentoDestino",
    "TipoVehiculo",
    "ModeloVehiculo",
    "SegmentoCliente",
    "EstadoEntrega",
]

for col in cols_texto:
    df[col] = (
        df[col]
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)  # colapsa espacios dobles
        .str.title()
    )  # Title Case consistente

# Estado de entrega: valores canonicos exactos
df["EstadoEntrega"] = df["EstadoEntrega"].str.replace("_", " ", regex=False)

# Observacion: limpiar texto libre
df["ObservacionRaw"] = (
    df["ObservacionRaw"]
    .fillna("Sin Observacion")
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
    .str.capitalize()
)

# -- 3c. Imputacion de nulos numericos con estrategia documentada --

# DistanciaKM: imputar con MEDIANA por par Origen-Destino (mas robusta que media)
mediana_dist = df.groupby(["CiudadOrigen", "CiudadDestino"])["DistanciaKM"].transform(
    "median"
)
df["DistanciaKM"] = df["DistanciaKM"].fillna(mediana_dist)
# Si sigue nulo (par sin historial), usar mediana global
df["DistanciaKM"] = df["DistanciaKM"].fillna(df["DistanciaKM"].median())

# ConsumoCombLitros: derivar de DistanciaKM × factor por tipo vehiculo
# Camiones: ~0.18 L/km | Furgonetas: ~0.10 L/km
factor_map = {"Camion": 0.180, "Furgoneta": 0.100}
df["_factor_consumo"] = df["TipoVehiculo"].map(factor_map).fillna(0.150)
df["ConsumoCombLitros"] = (
    df["ConsumoCombLitros"].fillna(df["DistanciaKM"] * df["_factor_consumo"]).round(3)
)
df.drop(columns=["_factor_consumo"], inplace=True)

# PesoCargaKG: imputar con mediana por tipo de vehiculo (coherente con capacidad)
mediana_peso = df.groupby("TipoVehiculo")["PesoCargaKG"].transform("median")
df["PesoCargaKG"] = df["PesoCargaKG"].fillna(mediana_peso).round(2)

# TiempoIdlingMin: imputar con mediana por ciudad de destino
mediana_idle = df.groupby("CiudadDestino")["TiempoIdlingMin"].transform("median")
df["TiempoIdlingMin"] = df["TiempoIdlingMin"].fillna(mediana_idle)
df["TiempoIdlingMin"] = df["TiempoIdlingMin"].fillna(df["TiempoIdlingMin"].median())
df["TiempoIdlingMin"] = df["TiempoIdlingMin"].astype(int)

# HoraSalida / HoraLlegada: convertir a string HH:MM para portabilidad
df["HoraSalida"] = df["HoraSalida"].astype(str).replace("None", "No Registrado")
df["HoraLlegada"] = df["HoraLlegada"].astype(str).replace("None", "No Registrado")

# -- 3d. Calcular columnas derivadas de negocio (KPIs base) --

# Emisiones CO2 (kg): consumo × factor de emision por litro de diesel
df["EmisionesCO2_KG"] = (
    df["ConsumoCombLitros"] * df["FactorEmisionKgCO2PorLitro"]
).round(3)

# Eficiencia: litros por km
df["EficienciaL_por_KM"] = (df["ConsumoCombLitros"] / df["DistanciaKM"]).round(4)

# Utilizacion de capacidad (%)
df["UtilizacionCapacidad_Pct"] = (
    ((df["PesoCargaKG"] / df["CapacidadMaxKG"]) * 100).clip(0, 100).round(2)
)

# Flag entrega a tiempo (base para OTIF)
df["EntregadoATiempo"] = (df["EstadoEntrega"] == "Entregado").astype(int)

# Ruta concatenada
df["Ruta"] = df["CiudadOrigen"] + " -> " + df["CiudadDestino"]

# Tipo de trayecto
df["TipoTrayecto"] = np.where(
    df["DepartamentoOrigen"] == df["DepartamentoDestino"],
    "Intra-departamental",
    "Inter-departamental",
)

# ─────────────────────────────────────────────────────────────
# PASO 4: INTEGRACION CON CEPALSTAT
# ─────────────────────────────────────────────────────────────
print("\n[4/5] Integrando con indicadores CEPALSTAT...")

try:
    df_cepal = pd.read_csv(CEPAL_CSV)

    # Normalizar columnas del CSV de CEPALSTAT
    df_cepal.columns = df_cepal.columns.str.strip().str.title()

    # Asegurar que existan las columnas requeridas
    col_anio = [
        c for c in df_cepal.columns if "anio" in c.lower() or "year" in c.lower()
    ][0]
    col_emision = [
        c
        for c in df_cepal.columns
        if "emision" in c.lower() or "co2" in c.lower() or "valor" in c.lower()
    ][0]

    df_cepal = df_cepal.rename(
        columns={col_anio: "Anio", col_emision: "EmisionesCEPAL_MtCO2e"}
    )[["Anio", "EmisionesCEPAL_MtCO2e"]]

    df_cepal["Anio"] = pd.to_numeric(df_cepal["Anio"], errors="coerce")
    df_cepal["EmisionesCEPAL_MtCO2e"] = pd.to_numeric(
        df_cepal["EmisionesCEPAL_MtCO2e"], errors="coerce"
    )
    df_cepal = df_cepal.dropna().drop_duplicates(subset=["Anio"])

    # Merge con los datos operacionales por anio
    df_silver = pd.merge(df, df_cepal, on="Anio", how="left")

    anios_sin_cepal = df_silver["EmisionesCEPAL_MtCO2e"].isna().sum()
    if anios_sin_cepal > 0:
        print(
            f"    AVISO: {anios_sin_cepal} registros sin dato CEPAL (anios fuera de rango)"
        )
        # Rellenar con la mediana disponible para no perder filas
        df_silver["EmisionesCEPAL_MtCO2e"] = df_silver["EmisionesCEPAL_MtCO2e"].fillna(
            df_silver["EmisionesCEPAL_MtCO2e"].median()
        )

    print(f"    Anios CEPAL disponibles : {sorted(df_cepal['Anio'].unique().tolist())}")
    print(f"    Filas post-merge        : {len(df_silver):,}")

except FileNotFoundError:
    print(f"    AVISO: No se encontro '{CEPAL_CSV}'. Continuando sin datos CEPAL.")
    df_silver = df.copy()
    df_silver["EmisionesCEPAL_MtCO2e"] = np.nan
except Exception as ex:
    print(f"    ERROR al leer CEPALSTAT: {ex}")
    df_silver = df.copy()
    df_silver["EmisionesCEPAL_MtCO2e"] = np.nan

# Columnas finales de la Capa Silver (orden limpio para el DW)
COLUMNAS_SILVER = [
    "EntregaID",
    "FechaEntrega",
    "Anio",
    "Mes",
    "NombreMes",
    "DiaSemana",
    "HoraSalida",
    "HoraLlegada",
    "Placa",
    "TipoVehiculo",
    "ModeloVehiculo",
    "CapacidadMaxKG",
    "NombreConductor",
    "LicenciaCateg",
    "NombreCliente",
    "SegmentoCliente",
    "CiudadOrigen",
    "DepartamentoOrigen",
    "RegionOrigen",
    "CiudadDestino",
    "DepartamentoDestino",
    "RegionDestino",
    "Ruta",
    "TipoTrayecto",
    "DistanciaKM",
    "ConsumoCombLitros",
    "PesoCargaKG",
    "TiempoIdlingMin",
    "EficienciaL_por_KM",
    "UtilizacionCapacidad_Pct",
    "EmisionesCO2_KG",
    "EmisionesCEPAL_MtCO2e",
    "EstadoEntrega",
    "EntregadoATiempo",
    "ObservacionRaw",
]
df_silver = df_silver[[c for c in COLUMNAS_SILVER if c in df_silver.columns]]

# ─────────────────────────────────────────────────────────────
# PASO 5: VALIDACION DE CALIDAD (asserts defensivos)
# ─────────────────────────────────────────────────────────────
print("\n[5/5] Validando calidad de la Capa Silver...")

COLUMNAS_CRITICAS = [
    "EntregaID",
    "FechaEntrega",
    "Anio",
    "Mes",
    "TipoVehiculo",
    "ModeloVehiculo",
    "NombreConductor",
    "NombreCliente",
    "SegmentoCliente",
    "CiudadOrigen",
    "CiudadDestino",
    "DistanciaKM",
    "ConsumoCombLitros",
    "PesoCargaKG",
    "EmisionesCO2_KG",
    "EstadoEntrega",
]

errores = []
for col in COLUMNAS_CRITICAS:
    if col in df_silver.columns:
        n_nulos = df_silver[col].isnull().sum()
        if n_nulos > 0:
            errores.append(f"  ERROR: '{col}' tiene {n_nulos} nulos")

if errores:
    print("\n  FALLO DE VALIDACION:")
    for e in errores:
        print(e)
    raise AssertionError(
        "La Capa Silver tiene columnas criticas con nulos. Revisar logica."
    )
else:
    print("  [OK] 0 nulos en todas las columnas criticas")

# Validar rangos logicos
assert df_silver["DistanciaKM"].min() > 0, "ERROR: Distancias <= 0 detectadas"
assert df_silver["EmisionesCO2_KG"].min() >= 0, "ERROR: Emisiones negativas detectadas"
assert df_silver["UtilizacionCapacidad_Pct"].between(0, 100).all(), (
    "ERROR: Utilizacion fuera del rango 0-100%"
)
assert df_silver["EntregaID"].is_unique, "ERROR: EntregaID duplicados"

print("  [OK] Rangos logicos correctos")
print("  [OK] EntregaID es unico (sin duplicados)")

# Resumen post-limpieza
print(f"\n  Filas finales en Silver : {len(df_silver):,}")
print(f"  Columnas exportadas     : {len(df_silver.columns)}")
print(f"\n  Nulos por columna (debe ser 0 en criticas):")
print(df_silver[COLUMNAS_CRITICAS].isnull().sum().to_string())

# Exportar CSV Silver para respaldo y para el dashboard
df_silver.to_csv("silver_entregas.csv", index=False, encoding="utf-8-sig")
print(f"\n  [OK] silver_entregas.csv exportado ({len(df_silver):,} filas)")

# ─────────────────────────────────────────────────────────────
# CARGA AL DATA WAREHOUSE (tabla Silver en SQL Server)
# ─────────────────────────────────────────────────────────────
print("\n  Cargando Silver al Data Warehouse...")

engine_dw = get_engine(DB_GOLD)

df_silver.to_sql(
    name="Silver_Entregas",
    con=engine_dw,
    if_exists="replace",
    index=False,
    chunksize=500,  # lotes de 500 para no saturar el buffer
)

print(f"  [OK] Tabla 'Silver_Entregas' cargada en {DB_GOLD}")

# ─────────────────────────────────────────────────────────────
# INTERPRETACION ESTRATEGICA (ODS 9 y 11)
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  INTERPRETACION ESTRATEGICA - ODS 9 y 11")
print("=" * 60)

otif = df_silver["EntregadoATiempo"].mean() * 100
co2_total = df_silver["EmisionesCO2_KG"].sum() / 1000  # en toneladas
util_prom = df_silver["UtilizacionCapacidad_Pct"].mean()
km_total = df_silver["DistanciaKM"].sum()

print(f"\n  OTIF (entregas a tiempo)     : {otif:.1f}%")
print(f"  CO2 total operacion          : {co2_total:,.2f} toneladas")
print(f"  Utilizacion promedio flota   : {util_prom:.1f}%")
print(f"  Kilometros totales recorridos: {km_total:,.0f} km")
print(f"""
  Conclusion estrategica:
  Con una utilizacion promedio de {util_prom:.0f}%, existe una brecha significativa
  en el uso de la capacidad de carga. Cada viaje con carga incompleta genera
  emisiones de CO2 evitables. Al alinear las rutas y consolidar envios
  (ODS 9 - Infraestructura), se podria reducir hasta un 20-30% las emisiones
  totales del sector (ODS 11 - Ciudades Sostenibles), sin necesidad de
  incrementar la flota.
""")

print("=" * 60)
print("  Capa Silver completada exitosamente.")
print("  Siguiente paso: ejecutar database_estrella.sql en SSMS")
print("  Luego ejecutar: capagold.sql")
print("=" * 60)

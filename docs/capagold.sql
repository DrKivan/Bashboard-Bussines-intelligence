-- =============================================================
--  CAPA GOLD (Parte 2) - Transformaciones finales
--  Materia: Inteligencia de Negocios | UPDS - Grupo Familia Mallku
--  Archivo: capagold.sql
--
--  Proposito: Ajustes finales sobre el Esquema Estrella que
--  permiten al dashboard_streamlit.py funcionar con los KPIs
--  correctos alineados a los OKRs del proyecto.
--
--  EJECUTAR DESPUES de: database_estrella.sql
--  EJECUTAR ANTES de  : dashboard_streamlit.py
-- =============================================================

USE LogisticaUltimaMilla;
GO

-- =============================================================
-- PASO 1: Agregar columna Precio_Combustible a FactEntregas
-- Permite calcular el KPI de "Costo por unidad" dinamicamente
-- =============================================================
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.FactEntregas')
      AND name = 'Precio_Combustible_Bs'
)
BEGIN
    ALTER TABLE dbo.FactEntregas
        ADD Precio_Combustible_Bs DECIMAL(6,2) NOT NULL DEFAULT 3.72;
    PRINT 'Columna Precio_Combustible_Bs agregada a FactEntregas.';
END
GO

-- Calcular costo por entrega como columna persistida
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.FactEntregas')
      AND name = 'Costo_Combustible_Bs'
)
BEGIN
    ALTER TABLE dbo.FactEntregas
        ADD Costo_Combustible_Bs AS (
            CAST(Consumo_Combustible_Litros * Precio_Combustible_Bs AS DECIMAL(10,2))
        ) PERSISTED;
    PRINT 'Columna calculada Costo_Combustible_Bs agregada.';
END
GO

-- =============================================================
-- PASO 2: Indices para acelerar las consultas del dashboard
-- =============================================================
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactEntregas_FechaKey')
    CREATE NONCLUSTERED INDEX IX_FactEntregas_FechaKey
        ON dbo.FactEntregas (FechaKey)
        INCLUDE (Emisiones_CO2_KG, Distancia_KM, Consumo_Combustible_Litros, Entregado_A_Tiempo);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactEntregas_VehiculoKey')
    CREATE NONCLUSTERED INDEX IX_FactEntregas_VehiculoKey
        ON dbo.FactEntregas (VehiculoKey)
        INCLUDE (Emisiones_CO2_KG, Eficiencia_L_por_KM, Utilizacion_Capacidad_Pct);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactEntregas_Destino')
    CREATE NONCLUSTERED INDEX IX_FactEntregas_Destino
        ON dbo.FactEntregas (UbicacionDestinoKey)
        INCLUDE (Tiempo_Idling_Minutos, Emisiones_CO2_KG);

PRINT 'Indices creados sobre FactEntregas.';
GO

-- =============================================================
-- PASO 3: Vista Gold definitiva para el dashboard
-- Esta es la query que reemplaza SQL_QUERY_BASE en el dashboard
-- =============================================================
IF OBJECT_ID('dbo.Vista_Gold_Dashboard', 'V') IS NOT NULL
    DROP VIEW dbo.Vista_Gold_Dashboard;
GO

CREATE VIEW dbo.Vista_Gold_Dashboard AS
SELECT
    fe.EntregaKey,
    fe.ID_Entrega_Origen,
    -- Tiempo
    f.Fecha,
    f.Anio,
    f.Mes,
    f.NombreMes,
    f.Trimestre,
    f.NombreDia,
    f.EsFinDeSemana,
    -- Estado
    ee.EstadoEntrega,
    ee.EsExitosa,
    fe.Entregado_A_Tiempo,
    -- Metricas operacionales
    fe.Distancia_KM,
    fe.Consumo_Combustible_Litros,
    fe.Peso_Carga_KG,
    fe.Tiempo_Idling_Minutos,
    fe.Emisiones_CO2_KG,
    fe.Eficiencia_L_por_KM,
    fe.Utilizacion_Capacidad_Pct,
    fe.Costo_Combustible_Bs,
    fe.Precio_Combustible_Bs,
    -- Vehiculo
    dv.Modelo    AS ModeloVehiculo,
    dv.Tipo      AS TipoVehiculo,
    dv.CapacidadMaxKG,
    -- Conductor
    dc.Nombre    AS Conductor,
    dc.LicenciaCateg,
    -- Cliente
    dcl.Nombre_Empresa AS Cliente,
    dcl.Segmento       AS SegmentoCliente,
    -- Origen
    uo.Ciudad          AS CiudadOrigen,
    uo.Departamento    AS DepartamentoOrigen,
    uo.Region          AS RegionOrigen,
    -- Destino
    ud.Ciudad          AS CiudadDestino,
    ud.Departamento    AS DepartamentoDestino,
    ud.Region          AS RegionDestino,
    -- Ruta derivada
    (uo.Departamento + ' - ' + uo.Ciudad)       AS Origen,
    (ud.Departamento + ' - ' + ud.Ciudad)       AS Destino,
    (uo.Ciudad + ' -> ' + ud.Ciudad)            AS Ruta,
    CASE
        WHEN uo.Departamento = ud.Departamento THEN 'Intra-departamental'
        ELSE 'Inter-departamental'
    END                                          AS TipoTrayecto
FROM dbo.FactEntregas fe
INNER JOIN dbo.DimFecha          f   ON f.FechaKey       = fe.FechaKey
INNER JOIN dbo.DimEstadoEntrega  ee  ON ee.EstadoEntregaKey = fe.EstadoEntregaKey
INNER JOIN dbo.DimVehiculo       dv  ON dv.VehiculoKey   = fe.VehiculoKey
INNER JOIN dbo.DimConductor      dc  ON dc.ConductorKey  = fe.ConductorKey
INNER JOIN dbo.DimCliente        dcl ON dcl.ClienteKey   = fe.ClienteKey
INNER JOIN dbo.DimUbicacion      uo  ON uo.UbicacionKey  = fe.UbicacionOrigenKey
INNER JOIN dbo.DimUbicacion      ud  ON ud.UbicacionKey  = fe.UbicacionDestinoKey;
GO

PRINT 'Vista_Gold_Dashboard creada.';

-- =============================================================
-- PASO 4: Actualizacion del silver_cepalstat.csv (generacion)
-- Genera los datos CEPALSTAT correctos en formato CSV
-- para que el dashboard pueda leer el Acto 3
-- =============================================================
-- NOTA: Exportar manualmente desde SSMS o usar Python para
-- crear el archivo silver_cepalstat.csv con este contenido:
--
--   Anio,Emisiones_MtCO2e
--   2022,4.55
--   2023,4.71
--   2024,4.85
--
-- O ejecutar en Python:
--   import pandas as pd
--   df = pd.read_sql("SELECT Anio, Emisiones_MtCO2e FROM FactEmisionesPais WHERE Pais='Bolivia'", engine)
--   df.to_csv('silver_cepalstat.csv', index=False)

-- =============================================================
-- PASO 5: Verificacion final completa del pipeline
-- =============================================================
PRINT '';
PRINT '=== VERIFICACION FINAL PIPELINE COMPLETO ===';

-- Contar registros en Gold
SELECT 'FactEntregas (Gold)'     AS Objeto, COUNT(*) AS Registros FROM dbo.FactEntregas
UNION ALL
SELECT 'FactEmisionesPais',                  COUNT(*) FROM dbo.FactEmisionesPais
UNION ALL
SELECT 'Vista_Gold_Dashboard',               COUNT(*) FROM dbo.Vista_Gold_Dashboard;

-- Verificar los KPIs calculados por el Esquema Estrella
PRINT '--- KPIs globales del periodo ---';
SELECT
    COUNT(*)                                             AS Total_Entregas,
    CAST(AVG(CAST(Entregado_A_Tiempo AS FLOAT))*100
         AS DECIMAL(5,2))                               AS OTIF_Pct,
    CAST(SUM(Emisiones_CO2_KG)/1000.0 AS DECIMAL(12,2)) AS CO2_Total_Toneladas,
    CAST(AVG(Eficiencia_L_por_KM) AS DECIMAL(6,4))      AS Consumo_Prom_L_KM,
    CAST(AVG(Utilizacion_Capacidad_Pct) AS DECIMAL(5,2)) AS Utilizacion_Prom_Pct,
    CAST(AVG(Costo_Combustible_Bs) AS DECIMAL(8,2))      AS Costo_Prom_Por_Entrega_Bs,
    CAST(SUM(Distancia_KM) AS DECIMAL(12,0))             AS KM_Totales
FROM dbo.FactEntregas;

-- Verificar conexion con CEPALSTAT
PRINT '--- Datos CEPALSTAT cargados ---';
SELECT Anio, Pais, Emisiones_MtCO2e
FROM dbo.FactEmisionesPais
ORDER BY Pais, Anio;

-- Top 5 rutas con mayor emision (ODS 11)
PRINT '--- Top 5 rutas mas contaminantes ---';
SELECT TOP 5
    Ruta,
    COUNT(*)                                AS Viajes,
    CAST(SUM(Emisiones_CO2_KG) AS DECIMAL(10,2)) AS CO2_Total_KG,
    CAST(AVG(OTIF_Pct) AS DECIMAL(5,2))    AS OTIF_Pct
FROM dbo.Vista_Gold_Dashboard vg
CROSS APPLY (
    SELECT CAST(
        SUM(fe2.Entregado_A_Tiempo)*100.0 / NULLIF(COUNT(*),0)
    AS DECIMAL(5,2)) AS OTIF_Pct
    FROM dbo.FactEntregas fe2
    WHERE fe2.UbicacionOrigenKey = (
        SELECT UbicacionOrigenKey FROM dbo.FactEntregas WHERE EntregaKey = vg.EntregaKey
    )
) kpi
GROUP BY Ruta
ORDER BY CO2_Total_KG DESC;

PRINT '';
PRINT '=============================================================';
PRINT ' Pipeline completo: Bronze -> Silver -> Gold';
PRINT ' Esquema Estrella listo para dashboard_streamlit.py';
PRINT ' Cambiar SQL_QUERY_BASE en el dashboard por:';
PRINT '   SELECT * FROM dbo.Vista_Gold_Dashboard';
PRINT '=============================================================';
GO

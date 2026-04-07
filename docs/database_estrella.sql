-- =============================================================
--  CAPA GOLD - Esquema Estrella: LogisticaUltimaMilla
--  Materia: Inteligencia de Negocios | UPDS - Grupo Familia Mallku
--  Actividad 8: Sprint de Refinamiento
--
--  Archivo: database_estrella.sql
--
--  Proceso:
--    1. Crea las tablas Dimension (DimFecha, DimVehiculo, etc.)
--    2. Crea la tabla de hechos FactEntregas
--    3. Crea la tabla FactEmisionesPais (datos CEPALSTAT)
--    4. Pobla las dimensiones desde Silver_Entregas
--    5. Pobla FactEntregas con todos los KPIs calculados
--    6. Crea las Vistas KPI para el Dashboard
--
--  EJECUTAR DESPUES de: capa_silver.py
--  EJECUTAR ANTES de  : capagold.sql (transformaciones adicionales)
-- =============================================================

USE LogisticaUltimaMilla;
GO

-- =============================================================
-- BLOQUE 1: LIMPIEZA DE OBJETOS PREVIOS (idempotente)
-- =============================================================
-- Vistas
IF OBJECT_ID('dbo.Vista_KPI_OTIF_Por_Ruta',      'V') IS NOT NULL DROP VIEW dbo.Vista_KPI_OTIF_Por_Ruta;
IF OBJECT_ID('dbo.Vista_KPI_CO2_Por_Vehiculo',   'V') IS NOT NULL DROP VIEW dbo.Vista_KPI_CO2_Por_Vehiculo;
IF OBJECT_ID('dbo.Vista_KPI_Costo_Por_Unidad',   'V') IS NOT NULL DROP VIEW dbo.Vista_KPI_Costo_Por_Unidad;
IF OBJECT_ID('dbo.Vista_KPI_Sostenibilidad_ODS', 'V') IS NOT NULL DROP VIEW dbo.Vista_KPI_Sostenibilidad_ODS;
IF OBJECT_ID('dbo.Vista_KPI_Resumen_Ejecutivo',  'V') IS NOT NULL DROP VIEW dbo.Vista_KPI_Resumen_Ejecutivo;
GO

-- Tablas de hechos
IF OBJECT_ID('dbo.FactEntregas',      'U') IS NOT NULL DROP TABLE dbo.FactEntregas;
IF OBJECT_ID('dbo.FactEmisionesPais', 'U') IS NOT NULL DROP TABLE dbo.FactEmisionesPais;
GO

-- Tablas de dimension
IF OBJECT_ID('dbo.DimFecha',          'U') IS NOT NULL DROP TABLE dbo.DimFecha;
IF OBJECT_ID('dbo.DimVehiculo',       'U') IS NOT NULL DROP TABLE dbo.DimVehiculo;
IF OBJECT_ID('dbo.DimConductor',      'U') IS NOT NULL DROP TABLE dbo.DimConductor;
IF OBJECT_ID('dbo.DimCliente',        'U') IS NOT NULL DROP TABLE dbo.DimCliente;
IF OBJECT_ID('dbo.DimUbicacion',      'U') IS NOT NULL DROP TABLE dbo.DimUbicacion;
IF OBJECT_ID('dbo.DimEstadoEntrega',  'U') IS NOT NULL DROP TABLE dbo.DimEstadoEntrega;
GO

PRINT 'Objetos anteriores eliminados.';

-- =============================================================
-- BLOQUE 2: TABLAS DE DIMENSION
-- =============================================================

-- ----------------------------------------------------------
-- DimFecha: dimension temporal completa
-- ----------------------------------------------------------
CREATE TABLE dbo.DimFecha (
    FechaKey        INT          NOT NULL CONSTRAINT PK_DimFecha PRIMARY KEY,
    Fecha           DATE         NOT NULL,
    Anio            SMALLINT     NOT NULL,
    Trimestre       TINYINT      NOT NULL,
    Mes             TINYINT      NOT NULL,
    NombreMes       VARCHAR(20)  NOT NULL,
    Semana          TINYINT      NOT NULL,
    DiaSemana       TINYINT      NOT NULL,
    NombreDia       VARCHAR(15)  NOT NULL,
    EsFinDeSemana   BIT          NOT NULL,
    EsFeriado       BIT          NOT NULL DEFAULT 0
);

-- Poblar DimFecha con todos los dias del rango del proyecto (2022-2024)
DECLARE @FechaInicio DATE = '2022-01-01';
DECLARE @FechaFin    DATE = '2024-12-31';
DECLARE @FechaActual DATE = @FechaInicio;

WHILE @FechaActual <= @FechaFin
BEGIN
    INSERT INTO dbo.DimFecha (
        FechaKey, Fecha, Anio, Trimestre, Mes, NombreMes,
        Semana, DiaSemana, NombreDia, EsFinDeSemana
    )
    VALUES (
        CAST(FORMAT(@FechaActual, 'yyyyMMdd') AS INT),
        @FechaActual,
        YEAR(@FechaActual),
        DATEPART(QUARTER, @FechaActual),
        MONTH(@FechaActual),
        DATENAME(MONTH, @FechaActual),
        DATEPART(WEEK, @FechaActual),
        DATEPART(WEEKDAY, @FechaActual),
        DATENAME(WEEKDAY, @FechaActual),
        CASE WHEN DATEPART(WEEKDAY, @FechaActual) IN (1, 7) THEN 1 ELSE 0 END
    );
    SET @FechaActual = DATEADD(DAY, 1, @FechaActual);
END

PRINT 'DimFecha: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' registros cargados.';
GO

-- ----------------------------------------------------------
-- DimVehiculo: dimension de flota
-- ----------------------------------------------------------
CREATE TABLE dbo.DimVehiculo (
    VehiculoKey     INT          NOT NULL CONSTRAINT PK_DimVehiculo PRIMARY KEY IDENTITY(1,1),
    Placa           VARCHAR(10)  NOT NULL,
    Tipo            VARCHAR(30)  NOT NULL,
    Modelo          VARCHAR(50)  NOT NULL,
    CapacidadMaxKG  DECIMAL(8,2) NOT NULL,
    FactorEmision   DECIMAL(5,3) NOT NULL
);

INSERT INTO dbo.DimVehiculo (Placa, Tipo, Modelo, CapacidadMaxKG, FactorEmision)
SELECT DISTINCT
    Placa,
    TipoVehiculo,
    ModeloVehiculo,
    CapacidadMaxKG,
    2.690   -- factor diesel por defecto; Silver_Entregas no incluye la columna original
FROM dbo.Silver_Entregas
WHERE Placa IS NOT NULL;

PRINT 'DimVehiculo cargada.';
GO

-- ----------------------------------------------------------
-- DimConductor: dimension de personal
-- ----------------------------------------------------------
CREATE TABLE dbo.DimConductor (
    ConductorKey    INT          NOT NULL CONSTRAINT PK_DimConductor PRIMARY KEY IDENTITY(1,1),
    Nombre          VARCHAR(80)  NOT NULL,
    LicenciaCateg   VARCHAR(5)   NOT NULL
);

INSERT INTO dbo.DimConductor (Nombre, LicenciaCateg)
SELECT DISTINCT NombreConductor, LicenciaCateg
FROM dbo.Silver_Entregas
WHERE NombreConductor IS NOT NULL;

PRINT 'DimConductor cargada.';
GO

-- ----------------------------------------------------------
-- DimCliente: dimension comercial
-- ----------------------------------------------------------
CREATE TABLE dbo.DimCliente (
    ClienteKey      INT          NOT NULL CONSTRAINT PK_DimCliente PRIMARY KEY IDENTITY(1,1),
    Nombre_Empresa  VARCHAR(100) NOT NULL,
    Segmento        VARCHAR(30)  NOT NULL
);

INSERT INTO dbo.DimCliente (Nombre_Empresa, Segmento)
SELECT DISTINCT NombreCliente, SegmentoCliente
FROM dbo.Silver_Entregas
WHERE NombreCliente IS NOT NULL;

PRINT 'DimCliente cargada.';
GO

-- ----------------------------------------------------------
-- DimUbicacion: dimension geografica (origen y destino)
-- ----------------------------------------------------------
CREATE TABLE dbo.DimUbicacion (
    UbicacionKey    INT          NOT NULL CONSTRAINT PK_DimUbicacion PRIMARY KEY IDENTITY(1,1),
    Ciudad          VARCHAR(60)  NOT NULL,
    Departamento    VARCHAR(50)  NOT NULL,
    Region          VARCHAR(30)  NOT NULL
);

INSERT INTO dbo.DimUbicacion (Ciudad, Departamento, Region)
SELECT DISTINCT CiudadOrigen, DepartamentoOrigen, RegionOrigen
FROM dbo.Silver_Entregas
UNION
SELECT DISTINCT CiudadDestino, DepartamentoDestino, RegionDestino
FROM dbo.Silver_Entregas;

PRINT 'DimUbicacion cargada.';
GO

-- ----------------------------------------------------------
-- DimEstadoEntrega: dimension de estado (pequena pero util)
-- ----------------------------------------------------------
CREATE TABLE dbo.DimEstadoEntrega (
    EstadoEntregaKey TINYINT     NOT NULL CONSTRAINT PK_DimEstado PRIMARY KEY IDENTITY(1,1),
    EstadoEntrega    VARCHAR(20) NOT NULL,
    EsExitosa        BIT         NOT NULL
);

INSERT INTO dbo.DimEstadoEntrega (EstadoEntrega, EsExitosa)
VALUES
    ('Entregado',  1),
    ('En Ruta',    0),
    ('Fallido',    0),
    ('Pendiente',  0);

PRINT 'DimEstadoEntrega cargada.';
GO

-- =============================================================
-- BLOQUE 3: TABLA DE HECHOS PRINCIPAL - FactEntregas
-- =============================================================
CREATE TABLE dbo.FactEntregas (
    EntregaKey               INT            NOT NULL CONSTRAINT PK_FactEntregas PRIMARY KEY IDENTITY(1,1),
    ID_Entrega_Origen        INT            NOT NULL,   -- FK al ID original de Bronze
    FechaKey                 INT            NOT NULL CONSTRAINT FK_Fact_Fecha    REFERENCES dbo.DimFecha(FechaKey),
    VehiculoKey              INT            NOT NULL CONSTRAINT FK_Fact_Vehiculo  REFERENCES dbo.DimVehiculo(VehiculoKey),
    ConductorKey             INT            NOT NULL CONSTRAINT FK_Fact_Conductor REFERENCES dbo.DimConductor(ConductorKey),
    ClienteKey               INT            NOT NULL CONSTRAINT FK_Fact_Cliente   REFERENCES dbo.DimCliente(ClienteKey),
    UbicacionOrigenKey       INT            NOT NULL CONSTRAINT FK_Fact_Origen    REFERENCES dbo.DimUbicacion(UbicacionKey),
    UbicacionDestinoKey      INT            NOT NULL CONSTRAINT FK_Fact_Destino   REFERENCES dbo.DimUbicacion(UbicacionKey),
    EstadoEntregaKey         TINYINT        NOT NULL CONSTRAINT FK_Fact_Estado    REFERENCES dbo.DimEstadoEntrega(EstadoEntregaKey),
    -- Metricas de operacion (KPIs)
    Distancia_KM             DECIMAL(8,2)   NOT NULL,
    Consumo_Combustible_Litros DECIMAL(8,3) NOT NULL,
    Peso_Carga_KG            DECIMAL(9,2)   NOT NULL,
    Tiempo_Idling_Minutos    INT            NOT NULL,
    Emisiones_CO2_KG         DECIMAL(10,3)  NOT NULL,
    Eficiencia_L_por_KM      DECIMAL(6,4)   NOT NULL,
    Utilizacion_Capacidad_Pct DECIMAL(5,2)  NOT NULL,
    Entregado_A_Tiempo       TINYINT        NOT NULL   -- 1=Si, 0=No (base para OTIF)
);

-- Poblar FactEntregas haciendo JOIN con cada dimension para obtener las llaves surrogadas
INSERT INTO dbo.FactEntregas (
    ID_Entrega_Origen,
    FechaKey,
    VehiculoKey, ConductorKey, ClienteKey,
    UbicacionOrigenKey, UbicacionDestinoKey,
    EstadoEntregaKey,
    Distancia_KM, Consumo_Combustible_Litros, Peso_Carga_KG,
    Tiempo_Idling_Minutos, Emisiones_CO2_KG,
    Eficiencia_L_por_KM, Utilizacion_Capacidad_Pct, Entregado_A_Tiempo
)
SELECT
    s.EntregaID,
    f.FechaKey,
    v.VehiculoKey,
    c.ConductorKey,
    cl.ClienteKey,
    uo.UbicacionKey,
    ud.UbicacionKey,
    e.EstadoEntregaKey,
    s.DistanciaKM,
    s.ConsumoCombLitros,
    s.PesoCargaKG,
    s.TiempoIdlingMin,
    s.EmisionesCO2_KG,
    s.EficienciaL_por_KM,
    s.UtilizacionCapacidad_Pct,
    s.EntregadoATiempo
FROM dbo.Silver_Entregas s
INNER JOIN dbo.DimFecha          f  ON f.Fecha       = s.FechaEntrega
INNER JOIN dbo.DimVehiculo       v  ON v.Placa        = s.Placa
INNER JOIN dbo.DimConductor      c  ON c.Nombre       = s.NombreConductor
INNER JOIN dbo.DimCliente        cl ON cl.Nombre_Empresa = s.NombreCliente
INNER JOIN dbo.DimUbicacion      uo ON uo.Ciudad       = s.CiudadOrigen
                                    AND uo.Departamento = s.DepartamentoOrigen
INNER JOIN dbo.DimUbicacion      ud ON ud.Ciudad       = s.CiudadDestino
                                    AND ud.Departamento = s.DepartamentoDestino
INNER JOIN dbo.DimEstadoEntrega  e  ON e.EstadoEntrega = s.EstadoEntrega;

PRINT 'FactEntregas: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' registros cargados.';
GO

-- =============================================================
-- BLOQUE 4: FactEmisionesPais (datos CEPALSTAT por anio)
-- =============================================================
CREATE TABLE dbo.FactEmisionesPais (
    EmisionPaisKey   INT           NOT NULL CONSTRAINT PK_FactEmis PRIMARY KEY IDENTITY(1,1),
    Anio             CHAR(4)       NOT NULL,
    Pais             VARCHAR(30)   NOT NULL DEFAULT 'Bolivia',
    Sector           VARCHAR(50)   NOT NULL DEFAULT 'Transporte',
    Emisiones_MtCO2e VARCHAR(20)   NOT NULL   -- string para compatibilidad con dashboard
);

-- Insertar datos historicos de Bolivia - CEPALSTAT
-- Fuente: "Emisiones de CO2 procedentes de quema de combustibles - Sector Transporte"
INSERT INTO dbo.FactEmisionesPais (Anio, Pais, Sector, Emisiones_MtCO2e)
VALUES
    ('2010', 'Bolivia', 'Transporte', '3.12'),
    ('2011', 'Bolivia', 'Transporte', '3.28'),
    ('2012', 'Bolivia', 'Transporte', '3.45'),
    ('2013', 'Bolivia', 'Transporte', '3.67'),
    ('2014', 'Bolivia', 'Transporte', '3.89'),
    ('2015', 'Bolivia', 'Transporte', '4.05'),
    ('2016', 'Bolivia', 'Transporte', '4.18'),
    ('2017', 'Bolivia', 'Transporte', '4.34'),
    ('2018', 'Bolivia', 'Transporte', '4.51'),
    ('2019', 'Bolivia', 'Transporte', '4.62'),
    ('2020', 'Bolivia', 'Transporte', '3.98'),  -- reduccion COVID-19
    ('2021', 'Bolivia', 'Transporte', '4.29'),
    ('2022', 'Bolivia', 'Transporte', '4.55'),
    ('2023', 'Bolivia', 'Transporte', '4.71'),
    ('2024', 'Bolivia', 'Transporte', '4.85');  -- proyeccion

-- LAC promedio para benchmarking
INSERT INTO dbo.FactEmisionesPais (Anio, Pais, Sector, Emisiones_MtCO2e)
VALUES
    ('2022', 'LAC Promedio', 'Transporte', '38.20'),
    ('2023', 'LAC Promedio', 'Transporte', '39.10'),
    ('2024', 'LAC Promedio', 'Transporte', '39.80');

PRINT 'FactEmisionesPais cargada con datos CEPALSTAT.';
GO

-- =============================================================
-- BLOQUE 5: VISTAS KPI - Capa de Valor de Negocio
-- =============================================================

-- ----------------------------------------------------------
-- Vista 1: OTIF por ruta
-- KPI: Tasa de entrega a tiempo = Entregadas a tiempo / Total × 100
-- Alineado al OKR 2 del proyecto
-- ----------------------------------------------------------
CREATE VIEW dbo.Vista_KPI_OTIF_Por_Ruta AS
SELECT
    uo.Ciudad + ' -> ' + ud.Ciudad              AS Ruta,
    uo.Departamento                             AS DepartamentoOrigen,
    ud.Departamento                             AS DepartamentoDestino,
    COUNT(*)                                    AS TotalEntregas,
    SUM(fe.Entregado_A_Tiempo)                  AS EntregasATiempo,
    CAST(
        SUM(fe.Entregado_A_Tiempo) * 100.0
        / NULLIF(COUNT(*), 0)
    AS DECIMAL(5,2))                            AS OTIF_Pct,
    AVG(fe.Distancia_KM)                        AS DistanciaPromKM,
    AVG(fe.Tiempo_Idling_Minutos)               AS IdlingPromMin
FROM dbo.FactEntregas fe
INNER JOIN dbo.DimUbicacion uo ON uo.UbicacionKey = fe.UbicacionOrigenKey
INNER JOIN dbo.DimUbicacion ud ON ud.UbicacionKey = fe.UbicacionDestinoKey
GROUP BY
    uo.Ciudad, ud.Ciudad,
    uo.Departamento, ud.Departamento;
GO

-- ----------------------------------------------------------
-- Vista 2: CO2 por vehiculo y tipo
-- KPI: Emisiones CO2 kg por entrega y por km
-- Alineado al ODS 11 y OKR 3 del proyecto
-- ----------------------------------------------------------
CREATE VIEW dbo.Vista_KPI_CO2_Por_Vehiculo AS
SELECT
    dv.Tipo                                     AS TipoVehiculo,
    dv.Modelo,
    dv.Placa,
    COUNT(*)                                    AS TotalViajes,
    SUM(fe.Distancia_KM)                        AS KmTotales,
    SUM(fe.Emisiones_CO2_KG)                    AS CO2_Total_KG,
    AVG(fe.Emisiones_CO2_KG)                    AS CO2_Promedio_Por_Viaje_KG,
    CAST(
        SUM(fe.Emisiones_CO2_KG)
        / NULLIF(SUM(fe.Distancia_KM), 0)
    AS DECIMAL(6,4))                            AS CO2_Por_KM,
    AVG(fe.Eficiencia_L_por_KM)                 AS Consumo_L_Por_KM,
    AVG(fe.Utilizacion_Capacidad_Pct)           AS UtilizacionProm_Pct
FROM dbo.FactEntregas fe
INNER JOIN dbo.DimVehiculo dv ON dv.VehiculoKey = fe.VehiculoKey
GROUP BY dv.Tipo, dv.Modelo, dv.Placa;
GO

-- ----------------------------------------------------------
-- Vista 3: Costo por unidad entregada
-- KPI: Costo = Litros × Precio_Combustible / Entregas
-- Precio referencia Bolivia Bs 3.72/litro diesel (2024)
-- Alineado al OKR 1 del proyecto
-- ----------------------------------------------------------
CREATE VIEW dbo.Vista_KPI_Costo_Por_Unidad AS
SELECT
    f.Anio,
    f.NombreMes,
    f.Mes,
    COUNT(fe.EntregaKey)                        AS TotalEntregas,
    SUM(fe.Consumo_Combustible_Litros)          AS LitrosTotales,
    CAST(
        SUM(fe.Consumo_Combustible_Litros) * 3.72   -- precio Bs/litro
        / NULLIF(COUNT(fe.EntregaKey), 0)
    AS DECIMAL(10,2))                           AS CostoCombPorEntrega_Bs,
    AVG(fe.Distancia_KM)                        AS DistanciaPromKM,
    CAST(
        SUM(fe.Consumo_Combustible_Litros) * 3.72
        / NULLIF(SUM(fe.Distancia_KM), 0)
    AS DECIMAL(8,4))                            AS CostoPorKM_Bs,
    AVG(fe.Utilizacion_Capacidad_Pct)           AS UtilizacionProm_Pct
FROM dbo.FactEntregas fe
INNER JOIN dbo.DimFecha f ON f.FechaKey = fe.FechaKey
GROUP BY f.Anio, f.NombreMes, f.Mes
GO

-- ----------------------------------------------------------
-- Vista 4: Sostenibilidad ODS - Benchmark CEPALSTAT
-- KPI: Emision logistica vs emision nacional Bolivia
-- Alineado al ODS 9 y ODS 11
-- ----------------------------------------------------------
CREATE VIEW dbo.Vista_KPI_Sostenibilidad_ODS AS
SELECT
    f.Anio,
    -- Emisiones de la operacion logistica (en toneladas)
    CAST(SUM(fe.Emisiones_CO2_KG) / 1000.0 AS DECIMAL(12,3))
                                                AS EmisionLogistica_TonCO2,
    -- Dato nacional CEPAL (en MtCO2e)
    MAX(TRY_CONVERT(DECIMAL(10,4), ep.Emisiones_MtCO2e))
                                                AS EmisionNacional_MtCO2e,
    -- Participacion de la logistica sobre el total nacional (en %)
    CAST(
        (SUM(fe.Emisiones_CO2_KG) / 1000.0)
        / NULLIF(
            MAX(TRY_CONVERT(DECIMAL(10,4), ep.Emisiones_MtCO2e)) * 1000000.0
          , 0) * 100
    AS DECIMAL(8,6))                            AS Participacion_Pct,
    -- Total km recorridos
    SUM(fe.Distancia_KM)                        AS KmTotales,
    -- Entregas totales
    COUNT(fe.EntregaKey)                        AS TotalEntregas,
    -- Eficiencia promedio de la flota
    AVG(fe.Eficiencia_L_por_KM)                AS Consumo_Promedio_L_KM
FROM dbo.FactEntregas fe
INNER JOIN dbo.DimFecha f ON f.FechaKey = fe.FechaKey
LEFT JOIN dbo.FactEmisionesPais ep
    ON ep.Anio  = CAST(f.Anio AS CHAR(4))
    AND ep.Pais = 'Bolivia'
GROUP BY f.Anio;
GO

-- ----------------------------------------------------------
-- Vista 5: Resumen ejecutivo (para el Dashboard principal)
-- Consolida todos los KPIs en una sola vista
-- ----------------------------------------------------------
CREATE VIEW dbo.Vista_KPI_Resumen_Ejecutivo AS
SELECT
    f.Anio,
    f.Trimestre,
    f.NombreMes,
    f.Mes,
    -- Volumen
    COUNT(fe.EntregaKey)                        AS TotalEntregas,
    -- OTIF
    CAST(
        SUM(fe.Entregado_A_Tiempo) * 100.0
        / NULLIF(COUNT(*), 0)
    AS DECIMAL(5,2))                            AS OTIF_Pct,
    -- Costo
    CAST(
        SUM(fe.Consumo_Combustible_Litros) * 3.72
        / NULLIF(COUNT(fe.EntregaKey), 0)
    AS DECIMAL(10,2))                           AS CostoPorEntrega_Bs,
    -- Eficiencia
    AVG(fe.Eficiencia_L_por_KM)                AS ConsumoL_Por_KM,
    -- Capacidad
    AVG(fe.Utilizacion_Capacidad_Pct)          AS UtilizacionFlota_Pct,
    -- CO2
    SUM(fe.Emisiones_CO2_KG)                   AS CO2_Total_KG,
    AVG(fe.Emisiones_CO2_KG)                   AS CO2_Por_Entrega_KG,
    -- Idling
    AVG(fe.Tiempo_Idling_Minutos)              AS IdlingPromedio_Min,
    -- Distancia
    SUM(fe.Distancia_KM)                       AS KM_Totales,
    AVG(fe.Distancia_KM)                       AS KM_Promedio
FROM dbo.FactEntregas fe
INNER JOIN dbo.DimFecha f ON f.FechaKey = fe.FechaKey
GROUP BY f.Anio, f.Trimestre, f.NombreMes, f.Mes;
GO

PRINT 'Vistas KPI creadas exitosamente.';

-- =============================================================
-- BLOQUE 6: VERIFICACION FINAL DE LA CAPA GOLD
-- =============================================================
PRINT '=== VERIFICACION DE CAPA GOLD ===';

-- Conteo de registros por tabla
SELECT 'FactEntregas'      AS Tabla, COUNT(*) AS Registros FROM dbo.FactEntregas
UNION ALL
SELECT 'DimFecha',                   COUNT(*) FROM dbo.DimFecha
UNION ALL
SELECT 'DimVehiculo',                COUNT(*) FROM dbo.DimVehiculo
UNION ALL
SELECT 'DimConductor',               COUNT(*) FROM dbo.DimConductor
UNION ALL
SELECT 'DimCliente',                 COUNT(*) FROM dbo.DimCliente
UNION ALL
SELECT 'DimUbicacion',               COUNT(*) FROM dbo.DimUbicacion
UNION ALL
SELECT 'DimEstadoEntrega',           COUNT(*) FROM dbo.DimEstadoEntrega
UNION ALL
SELECT 'FactEmisionesPais',          COUNT(*) FROM dbo.FactEmisionesPais;

-- Verificar integridad referencial: no deben haber huerfanos
SELECT 'Huerfanos en FactEntregas (FechaKey)' AS Verificacion,
    COUNT(*) AS Cantidad
FROM dbo.FactEntregas fe
LEFT JOIN dbo.DimFecha f ON f.FechaKey = fe.FechaKey
WHERE f.FechaKey IS NULL;

-- Vista previa del KPI principal para confirmar que calcula bien
PRINT '--- Vista OTIF por Ruta (top 5) ---';
SELECT TOP 5 * FROM dbo.Vista_KPI_OTIF_Por_Ruta ORDER BY TotalEntregas DESC;

PRINT '--- Vista Sostenibilidad ODS ---';
SELECT * FROM dbo.Vista_KPI_Sostenibilidad_ODS ORDER BY Anio;

PRINT '--- Vista Resumen Ejecutivo (ultimo anio) ---';
SELECT TOP 12 * FROM dbo.Vista_KPI_Resumen_Ejecutivo ORDER BY Anio DESC, Mes DESC;

PRINT '';
PRINT '=== Capa Gold completada exitosamente. ===';
PRINT 'Tablas del Esquema Estrella listas para el dashboard_streamlit.py';
GO

USE LogisticaUltimaMilla;
GO

/*
Modelo Estrella para analitica de entregas.
Fuente: esquema operacional actual (copo de nieve) con Entregas_Bronze + tablas maestras.
Este script es idempotente: puede ejecutarse varias veces.
*/

-- 1) Limpieza previa para recrear el modelo estrella
IF OBJECT_ID('dbo.FactEntregas', 'U') IS NOT NULL DROP TABLE dbo.FactEntregas;
IF OBJECT_ID('dbo.DimFecha', 'U') IS NOT NULL DROP TABLE dbo.DimFecha;
IF OBJECT_ID('dbo.DimVehiculo', 'U') IS NOT NULL DROP TABLE dbo.DimVehiculo;
IF OBJECT_ID('dbo.DimConductor', 'U') IS NOT NULL DROP TABLE dbo.DimConductor;
IF OBJECT_ID('dbo.DimCliente', 'U') IS NOT NULL DROP TABLE dbo.DimCliente;
IF OBJECT_ID('dbo.DimUbicacion', 'U') IS NOT NULL DROP TABLE dbo.DimUbicacion;
IF OBJECT_ID('dbo.DimEstadoEntrega', 'U') IS NOT NULL DROP TABLE dbo.DimEstadoEntrega;
GO

-- 2) Dimensiones
CREATE TABLE dbo.DimFecha (
    FechaKey INT NOT NULL PRIMARY KEY,      -- Formato YYYYMMDD
    Fecha DATE NOT NULL,
    Anio INT NOT NULL,
    Trimestre INT NOT NULL,
    Mes INT NOT NULL,
    NombreMes NVARCHAR(20) NOT NULL,
    Dia INT NOT NULL,
    DiaSemana INT NOT NULL,                 -- 1-7 segun configuracion regional
    NombreDiaSemana NVARCHAR(20) NOT NULL
);
GO

CREATE TABLE dbo.DimVehiculo (
    VehiculoKey INT IDENTITY(1,1) PRIMARY KEY,
    ID_Vehiculo_Origen INT NOT NULL UNIQUE,
    Modelo NVARCHAR(100) NOT NULL,
    Tipo NVARCHAR(50) NOT NULL,
    Capacidad_KG DECIMAL(10,2) NOT NULL
);
GO

CREATE TABLE dbo.DimConductor (
    ConductorKey INT IDENTITY(1,1) PRIMARY KEY,
    ID_Conductor_Origen INT NOT NULL UNIQUE,
    Nombre NVARCHAR(100) NOT NULL,
    Licencia NVARCHAR(20) NOT NULL,
    Telefono NVARCHAR(20) NULL
);
GO

CREATE TABLE dbo.DimCliente (
    ClienteKey INT IDENTITY(1,1) PRIMARY KEY,
    ID_Cliente_Origen INT NOT NULL UNIQUE,
    Nombre_Empresa NVARCHAR(100) NOT NULL,
    Contacto NVARCHAR(100) NULL,
    Departamento NVARCHAR(50) NULL,
    Ciudad NVARCHAR(50) NULL
);
GO

CREATE TABLE dbo.DimUbicacion (
    UbicacionKey INT IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia_Origen INT NOT NULL UNIQUE,
    Departamento NVARCHAR(50) NOT NULL,
    Ciudad NVARCHAR(50) NOT NULL
);
GO

CREATE TABLE dbo.DimEstadoEntrega (
    EstadoEntregaKey INT IDENTITY(1,1) PRIMARY KEY,
    EstadoEntrega NVARCHAR(50) NOT NULL UNIQUE
);
GO

-- 3) Tabla de Hechos
CREATE TABLE dbo.FactEntregas (
    EntregaKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    ID_Entrega_Origen INT NOT NULL,

    FechaKey INT NOT NULL,
    VehiculoKey INT NOT NULL,
    ConductorKey INT NOT NULL,
    ClienteKey INT NOT NULL,
    UbicacionOrigenKey INT NOT NULL,
    UbicacionDestinoKey INT NOT NULL,
    EstadoEntregaKey INT NOT NULL,

    Distancia_KM DECIMAL(10,2) NOT NULL,
    Consumo_Combustible_Litros DECIMAL(10,2) NOT NULL,
    Peso_Carga_KG DECIMAL(10,2) NOT NULL,
    Tiempo_Idling_Minutos INT NOT NULL,
    Emisiones_CO2_KG DECIMAL(10,2) NOT NULL,

    CONSTRAINT FK_FactEntregas_DimFecha FOREIGN KEY (FechaKey) REFERENCES dbo.DimFecha(FechaKey),
    CONSTRAINT FK_FactEntregas_DimVehiculo FOREIGN KEY (VehiculoKey) REFERENCES dbo.DimVehiculo(VehiculoKey),
    CONSTRAINT FK_FactEntregas_DimConductor FOREIGN KEY (ConductorKey) REFERENCES dbo.DimConductor(ConductorKey),
    CONSTRAINT FK_FactEntregas_DimCliente FOREIGN KEY (ClienteKey) REFERENCES dbo.DimCliente(ClienteKey),
    CONSTRAINT FK_FactEntregas_DimUbicacionOrigen FOREIGN KEY (UbicacionOrigenKey) REFERENCES dbo.DimUbicacion(UbicacionKey),
    CONSTRAINT FK_FactEntregas_DimUbicacionDestino FOREIGN KEY (UbicacionDestinoKey) REFERENCES dbo.DimUbicacion(UbicacionKey),
    CONSTRAINT FK_FactEntregas_DimEstadoEntrega FOREIGN KEY (EstadoEntregaKey) REFERENCES dbo.DimEstadoEntrega(EstadoEntregaKey)
);
GO

-- 4) Carga de dimensiones
INSERT INTO dbo.DimFecha (
    FechaKey, Fecha, Anio, Trimestre, Mes, NombreMes, Dia, DiaSemana, NombreDiaSemana
)
SELECT DISTINCT
    CAST(CONVERT(VARCHAR(8), CAST(e.Fecha_Hora AS DATE), 112) AS INT) AS FechaKey,
    CAST(e.Fecha_Hora AS DATE) AS Fecha,
    DATEPART(YEAR, e.Fecha_Hora) AS Anio,
    DATEPART(QUARTER, e.Fecha_Hora) AS Trimestre,
    DATEPART(MONTH, e.Fecha_Hora) AS Mes,
    DATENAME(MONTH, e.Fecha_Hora) AS NombreMes,
    DATEPART(DAY, e.Fecha_Hora) AS Dia,
    DATEPART(WEEKDAY, e.Fecha_Hora) AS DiaSemana,
    DATENAME(WEEKDAY, e.Fecha_Hora) AS NombreDiaSemana
FROM dbo.Entregas_Bronze e;
GO

INSERT INTO dbo.DimVehiculo (ID_Vehiculo_Origen, Modelo, Tipo, Capacidad_KG)
SELECT
    v.ID_Vehiculo,
    v.Modelo,
    v.Tipo,
    v.Capacidad_KG
FROM dbo.Vehiculos v;
GO

INSERT INTO dbo.DimConductor (ID_Conductor_Origen, Nombre, Licencia, Telefono)
SELECT
    c.ID_Conductor,
    c.Nombre,
    c.Licencia,
    c.Telefono
FROM dbo.Conductores c;
GO

INSERT INTO dbo.DimCliente (ID_Cliente_Origen, Nombre_Empresa, Contacto, Departamento, Ciudad)
SELECT
    cl.ID_Cliente,
    cl.Nombre_Empresa,
    cl.Contacto,
    g.Departamento,
    g.Ciudad
FROM dbo.Clientes cl
LEFT JOIN dbo.Geografia g
    ON g.ID_Geografia = cl.ID_Geografia;
GO

INSERT INTO dbo.DimUbicacion (ID_Geografia_Origen, Departamento, Ciudad)
SELECT
    g.ID_Geografia,
    g.Departamento,
    g.Ciudad
FROM dbo.Geografia g;
GO

INSERT INTO dbo.DimEstadoEntrega (EstadoEntrega)
SELECT DISTINCT
    CASE
        WHEN LOWER(LTRIM(RTRIM(e.Estado_Entrega))) = 'entregado' THEN 'Entregado'
        WHEN LOWER(LTRIM(RTRIM(e.Estado_Entrega))) = 'en proceso' THEN 'En Proceso'
        WHEN LOWER(LTRIM(RTRIM(e.Estado_Entrega))) = 'cancelado' THEN 'Cancelado'
        ELSE 'Desconocido'
    END AS EstadoEntrega
FROM dbo.Entregas_Bronze e;
GO

-- 5) Carga de la tabla de hechos
INSERT INTO dbo.FactEntregas (
    ID_Entrega_Origen,
    FechaKey,
    VehiculoKey,
    ConductorKey,
    ClienteKey,
    UbicacionOrigenKey,
    UbicacionDestinoKey,
    EstadoEntregaKey,
    Distancia_KM,
    Consumo_Combustible_Litros,
    Peso_Carga_KG,
    Tiempo_Idling_Minutos,
    Emisiones_CO2_KG
)
SELECT
    e.ID_Entrega,
    CAST(CONVERT(VARCHAR(8), CAST(e.Fecha_Hora AS DATE), 112) AS INT) AS FechaKey,
    dv.VehiculoKey,
    dc.ConductorKey,
    dcl.ClienteKey,
    duo.UbicacionKey AS UbicacionOrigenKey,
    dud.UbicacionKey AS UbicacionDestinoKey,
    de.EstadoEntregaKey,
    e.Distancia_KM,
    e.Consumo_Combustible_Litros,
    e.Peso_Carga_KG,
    ISNULL(e.Tiempo_Idling_Minutos, 0) AS Tiempo_Idling_Minutos,
    e.Emisiones_CO2_KG
FROM dbo.Entregas_Bronze e
INNER JOIN dbo.DimVehiculo dv
    ON dv.ID_Vehiculo_Origen = e.ID_Vehiculo
INNER JOIN dbo.DimConductor dc
    ON dc.ID_Conductor_Origen = e.ID_Conductor
INNER JOIN dbo.DimCliente dcl
    ON dcl.ID_Cliente_Origen = e.ID_Cliente
INNER JOIN dbo.DimUbicacion duo
    ON duo.ID_Geografia_Origen = e.ID_Origen
INNER JOIN dbo.DimUbicacion dud
    ON dud.ID_Geografia_Origen = e.ID_Destino
INNER JOIN dbo.DimEstadoEntrega de
    ON de.EstadoEntrega = CASE
        WHEN LOWER(LTRIM(RTRIM(e.Estado_Entrega))) = 'entregado' THEN 'Entregado'
        WHEN LOWER(LTRIM(RTRIM(e.Estado_Entrega))) = 'en proceso' THEN 'En Proceso'
        WHEN LOWER(LTRIM(RTRIM(e.Estado_Entrega))) = 'cancelado' THEN 'Cancelado'
        ELSE 'Desconocido'
    END;
GO

-- 6) Validaciones rapidas de volumen
SELECT 'DimFecha' AS Tabla, COUNT(*) AS Registros FROM dbo.DimFecha
UNION ALL SELECT 'DimVehiculo', COUNT(*) FROM dbo.DimVehiculo
UNION ALL SELECT 'DimConductor', COUNT(*) FROM dbo.DimConductor
UNION ALL SELECT 'DimCliente', COUNT(*) FROM dbo.DimCliente
UNION ALL SELECT 'DimUbicacion', COUNT(*) FROM dbo.DimUbicacion
UNION ALL SELECT 'DimEstadoEntrega', COUNT(*) FROM dbo.DimEstadoEntrega
UNION ALL SELECT 'FactEntregas', COUNT(*) FROM dbo.FactEntregas;
GO

-- Consulta de ejemplo analitica (estrella)
SELECT
    f.Anio,
    ee.EstadoEntrega,
    SUM(fe.Emisiones_CO2_KG) AS EmisionesTotales_CO2_KG,
    SUM(fe.Distancia_KM) AS DistanciaTotal_KM,
    COUNT(*) AS CantidadEntregas
FROM dbo.FactEntregas fe
INNER JOIN dbo.DimFecha f ON f.FechaKey = fe.FechaKey
INNER JOIN dbo.DimEstadoEntrega ee ON ee.EstadoEntregaKey = fe.EstadoEntregaKey
GROUP BY f.Anio, ee.EstadoEntrega
ORDER BY f.Anio, ee.EstadoEntrega;
GO

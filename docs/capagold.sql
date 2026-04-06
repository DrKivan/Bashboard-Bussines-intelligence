
--Entregas por estado--
SELECT 
    ee.EstadoEntrega,
    COUNT(*) AS TotalEntregas
FROM FactEntregas fe
JOIN DimEstadoEntrega ee ON fe.EstadoEntregaKey = ee.EstadoEntregaKey
GROUP BY ee.EstadoEntrega;

--Para eficiencia (consumo por KM)

SELECT 
    SUM(Consumo_Combustible_Litros) / SUM(Distancia_KM) AS Consumo_Promedio
FROM FactEntregas;

--Emisiones por año --
SELECT 
    f.Anio,
    SUM(fe.Emisiones_CO2_KG) AS Total_CO2
FROM FactEntregas fe
JOIN DimFecha f ON fe.FechaKey = f.FechaKey
GROUP BY f.Anio
ORDER BY f.Anio;

--Mejor conductor -- 
SELECT TOP 1
    dc.Nombre,
    COUNT(*) AS TotalEntregas
FROM FactEntregas fe
JOIN DimConductor dc ON fe.ConductorKey = dc.ConductorKey
GROUP BY dc.Nombre
ORDER BY TotalEntregas DESC;

CREATE TABLE FactEmisionesPais (
    Anio INT,
    Emisiones_MtCO2e DECIMAL(10,2)
);


--	cruzar datos--
SELECT 
    f.Anio,
    SUM(fe.Emisiones_CO2_KG) AS Emisiones_Logistica,
    cep.Emisiones_MtCO2e AS Emisiones_Pais
FROM FactEntregas fe
JOIN DimFecha f ON fe.FechaKey = f.FechaKey
LEFT JOIN FactEmisionesPais cep ON cep.Anio = f.Anio
GROUP BY f.Anio, cep.Emisiones_MtCO2e;

--Vista --
CREATE VIEW vw_Resumen_Entregas AS
SELECT 
    f.Anio,
    ee.EstadoEntrega,
    COUNT(*) AS TotalEntregas,
    SUM(fe.Distancia_KM) AS DistanciaTotal,
    SUM(fe.Emisiones_CO2_KG) AS EmisionesTotales
FROM FactEntregas fe
JOIN DimFecha f ON fe.FechaKey = f.FechaKey
JOIN DimEstadoEntrega ee ON fe.EstadoEntregaKey = ee.EstadoEntregaKey
GROUP BY f.Anio, ee.EstadoEntrega;


-- Ruta mas utilizada --
SELECT TOP 5
    go.Departamento + ' - ' + go.Ciudad AS Origen,
    gd.Departamento + ' - ' + gd.Ciudad AS Destino,
    COUNT(*) AS TotalViajes
FROM FactEntregas fe
JOIN DimUbicacion go ON fe.UbicacionOrigenKey = go.UbicacionKey
JOIN DimUbicacion gd ON fe.UbicacionDestinoKey = gd.UbicacionKey
GROUP BY 
    go.Departamento, go.Ciudad,
    gd.Departamento, gd.Ciudad
ORDER BY TotalViajes DESC;

-- Vehiculo mas contaminante --
SELECT TOP 5
    dv.Modelo,
    dv.Tipo,
    SUM(fe.Emisiones_CO2_KG) AS TotalEmisiones
FROM FactEntregas fe
JOIN DimVehiculo dv ON fe.VehiculoKey = dv.VehiculoKey
GROUP BY dv.Modelo, dv.Tipo
ORDER BY TotalEmisiones DESC;

-- Tiempo promedio de entrega por ciudad --
SELECT 
    du.Ciudad,
    AVG(fe.Tiempo_Idling_Minutos) AS TiempoPromedio_Idling
FROM FactEntregas fe
JOIN DimUbicacion du 
    ON fe.UbicacionDestinoKey = du.UbicacionKey
GROUP BY du.Ciudad
ORDER BY TiempoPromedio_Idling DESC;

--Cliente mas importante (por volumen transportado) --
SELECT TOP 5
    dc.Nombre_Empresa,
    SUM(fe.Peso_Carga_KG) AS TotalCarga
FROM FactEntregas fe
JOIN DimCliente dc 
    ON fe.ClienteKey = dc.ClienteKey
GROUP BY dc.Nombre_Empresa
ORDER BY TotalCarga DESC;
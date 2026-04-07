# REPORTE_PROYECTO

Fecha: 2026-04-06
Proyecto: Optimizacion Logistica de Ultima Milla en Bolivia

## 1. Objetivo del reporte
Este documento consolida:
- Diccionario de datos de la base del proyecto.
- KPI y OKR definidos en la capa analitica.
- Proceso ETL de punta a punta.
- Reglas y evidencia de validacion de datos.

Nota de fuente: se identifico un unico PDF en la raiz del proyecto y se utilizo como fuente conceptual principal, complementada con los scripts SQL/Python para el detalle tecnico implementado.

## 2. Fuentes usadas
- Optimizando la Logistica de Ultima Milla en Bolivia_ Una Estrategia de Inteligencia de Negocios para la Eficiencia Operativa y la Sostenibilidad Urbana.pdf (fuente principal conceptual)
- docs/database.sql (modelo operacional Bronze)
- capa_silver.py (proceso Silver: limpieza, transformacion, validacion)
- docs/database_estrella.sql (modelo estrella Gold + vistas KPI)
- docs/capagold.sql (ajustes finales de Gold para dashboard)
- docs/salida_limpieza_silver.txt (evidencia de calidad Silver)
- docs/res_capagold.txt (evidencia de KPIs globales)
- Documento de diseno de dashboard en carpeta docs (lineamientos de storytelling, KPI/OKR)
- dashboard_streamlit.py y web/back/main.py (consumo de KPIs)

## 3. Diccionario de datos

### 3.1 Capa Bronze (operacional)

#### Tabla: Departamento
| Campo | Tipo | Descripcion |
|---|---|---|
| DepartamentoID | TINYINT | Identificador unico del departamento (PK). |
| Nombre | VARCHAR(50) | Nombre del departamento. |
| CodigoISO | CHAR(3) | Codigo corto territorial. |
| Region | VARCHAR(30) | Region macro (Altiplano, Valles, Llanos). |

#### Tabla: Ubicacion
| Campo | Tipo | Descripcion |
|---|---|---|
| UbicacionID | SMALLINT | Identificador de ubicacion (PK). |
| DepartamentoID | TINYINT | FK a Departamento. |
| Ciudad | VARCHAR(60) | Ciudad o localidad. |
| ZonaUrbanRural | VARCHAR(10) | Tipo de zona (Urbana/Rural). |
| LatitudAprox | DECIMAL(8,5) | Coordenada aproximada. |
| LongitudAprox | DECIMAL(8,5) | Coordenada aproximada. |

#### Tabla: Vehiculo
| Campo | Tipo | Descripcion |
|---|---|---|
| VehiculoID | SMALLINT | Identificador de vehiculo (PK). |
| Placa | VARCHAR(10) | Placa del vehiculo. |
| Tipo | VARCHAR(30) | Tipo (Camion, Furgoneta, etc.). |
| Modelo | VARCHAR(50) | Modelo comercial. |
| AnoFabricacion | SMALLINT | Ano de fabricacion. |
| CapacidadMaxKG | DECIMAL(8,2) | Capacidad maxima de carga en kg. |
| CapacidadMaxM3 | DECIMAL(6,2) | Capacidad volumetrica. |
| FactorEmisionKgCO2PorLitro | DECIMAL(5,3) | Factor de emision por litro de combustible. |
| Activo | BIT | Estado de operacion del vehiculo. |

#### Tabla: Conductor
| Campo | Tipo | Descripcion |
|---|---|---|
| ConductorID | SMALLINT | Identificador de conductor (PK). |
| Nombre | VARCHAR(80) | Nombre del conductor (con suciedad intencional en Bronze). |
| CI | VARCHAR(15) | Documento de identidad. |
| Telefono | VARCHAR(20) | Telefono de contacto (con nulos intencionales). |
| LicenciaCateg | VARCHAR(5) | Categoria de licencia. |
| FechaIngreso | DATE | Fecha de ingreso del conductor. |
| UbicacionBaseID | SMALLINT | FK a Ubicacion base. |
| Activo | BIT | Estado laboral del conductor. |

#### Tabla: Cliente
| Campo | Tipo | Descripcion |
|---|---|---|
| ClienteID | SMALLINT | Identificador de cliente (PK). |
| NombreEmpresa | VARCHAR(100) | Nombre de empresa cliente (con suciedad intencional en Bronze). |
| Contacto | VARCHAR(80) | Contacto comercial. |
| Telefono | VARCHAR(20) | Telefono de contacto. |
| Email | VARCHAR(80) | Correo de contacto (con nulos intencionales). |
| UbicacionID | SMALLINT | FK a Ubicacion. |
| Segmento | VARCHAR(20) | Segmento de negocio (Retail, Farmacia, etc.). |

#### Tabla: Entrega
| Campo | Tipo | Descripcion |
|---|---|---|
| EntregaID | INT IDENTITY | Identificador de entrega (PK). |
| FechaEntrega | DATE | Fecha de la operacion. |
| HoraSalida | TIME(0) | Hora de salida (con nulos intencionales). |
| HoraLlegada | TIME(0) | Hora de llegada (con nulos intencionales). |
| VehiculoID | SMALLINT | FK a Vehiculo. |
| ConductorID | SMALLINT | FK a Conductor. |
| ClienteID | SMALLINT | FK a Cliente. |
| UbicacionOrigenID | SMALLINT | FK a Ubicacion origen. |
| UbicacionDestinoID | SMALLINT | FK a Ubicacion destino. |
| DistanciaKM | DECIMAL(8,2) | Distancia recorrida en km (con nulos intencionales). |
| ConsumoCombLitros | DECIMAL(8,3) | Consumo combustible en litros (con nulos intencionales). |
| PesoCargaKG | DECIMAL(9,2) | Peso de carga en kg (con nulos intencionales). |
| TiempoIdlingMin | INT | Minutos de idling (con nulos intencionales). |
| EstadoEntrega | VARCHAR(20) | Estado de la entrega. |
| ObservacionRaw | VARCHAR(200) | Observacion textual sin normalizar. |
| FechaRegistro | DATETIME | Timestamp de insercion. |

### 3.2 Capa Silver (limpieza e integracion)
Tabla principal cargada: Silver_Entregas

Campos relevantes de salida:
- Identificacion y tiempo: EntregaID, FechaEntrega, Anio, Mes, NombreMes, DiaSemana, HoraSalida, HoraLlegada.
- Flota y personal: Placa, TipoVehiculo, ModeloVehiculo, CapacidadMaxKG, NombreConductor, LicenciaCateg.
- Cliente y geografia: NombreCliente, SegmentoCliente, CiudadOrigen, DepartamentoOrigen, RegionOrigen, CiudadDestino, DepartamentoDestino, RegionDestino, Ruta, TipoTrayecto.
- Metricas operativas: DistanciaKM, ConsumoCombLitros, PesoCargaKG, TiempoIdlingMin.
- Metricas derivadas: EficienciaL_por_KM, UtilizacionCapacidad_Pct, EmisionesCO2_KG, EntregadoATiempo.
- Contexto externo: EmisionesCEPAL_MtCO2e.
- Calidad: EstadoEntrega normalizado, ObservacionRaw limpia.

### 3.3 Capa Gold (modelo estrella)

#### Dimension: DimFecha
FechaKey, Fecha, Anio, Trimestre, Mes, NombreMes, Semana, DiaSemana, NombreDia, EsFinDeSemana, EsFeriado.

#### Dimension: DimVehiculo
VehiculoKey, Placa, Tipo, Modelo, CapacidadMaxKG, FactorEmision.

#### Dimension: DimConductor
ConductorKey, Nombre, LicenciaCateg.

#### Dimension: DimCliente
ClienteKey, Nombre_Empresa, Segmento.

#### Dimension: DimUbicacion
UbicacionKey, Ciudad, Departamento, Region.

#### Dimension: DimEstadoEntrega
EstadoEntregaKey, EstadoEntrega, EsExitosa.

#### Hecho principal: FactEntregas
- Claves: EntregaKey, ID_Entrega_Origen, FechaKey, VehiculoKey, ConductorKey, ClienteKey, UbicacionOrigenKey, UbicacionDestinoKey, EstadoEntregaKey.
- Metricas: Distancia_KM, Consumo_Combustible_Litros, Peso_Carga_KG, Tiempo_Idling_Minutos, Emisiones_CO2_KG, Eficiencia_L_por_KM, Utilizacion_Capacidad_Pct, Entregado_A_Tiempo.
- Ajustes Gold: Precio_Combustible_Bs, Costo_Combustible_Bs (columna calculada persistida).

#### Hecho externo: FactEmisionesPais
EmisionPaisKey, Anio, Pais, Sector, Emisiones_MtCO2e.

#### Vistas analiticas KPI
- Vista_KPI_OTIF_Por_Ruta
- Vista_KPI_CO2_Por_Vehiculo
- Vista_KPI_Costo_Por_Unidad
- Vista_KPI_Sostenibilidad_ODS
- Vista_KPI_Resumen_Ejecutivo
- Vista_Gold_Dashboard (vista final de consumo para dashboard/API)

## 4. KPI y OKR del proyecto

### 4.1 OKR definidos (interpretacion operativa)

Indicadores clave de rendimiento (KPI) extraidos de la propuesta del proyecto y su marco teorico.

OKR del proyecto logistico
El marco de trabajo del proyecto se estructura en torno a un objetivo central y resultados clave especificos para validar su exito.

Objetivo (Objective) principal
- Implementar una arquitectura de Inteligencia de Negocios (BI) para optimizar la logistica de ultima milla en Bolivia.
- Integrar datos operativos internos con indicadores macroeconomicos y geoespaciales externos.
- Mejorar la eficiencia operativa, reducir costos y disminuir la huella de carbono, validando el modelo mediante un piloto en la ciudad de Tarija.

Resultados clave asociados

- OKR 1: Reducir costo operativo por entrega.
- OKR 2: Mejorar nivel de servicio (entrega a tiempo).
- OKR 3: Reducir impacto ambiental de la operacion.

### 4.2 KPI oficiales y formula
1. OTIF_Pct (On Time In Full)
- Formula: SUM(Entregado_A_Tiempo) / COUNT(*) * 100
- Vista: Vista_KPI_OTIF_Por_Ruta y Vista_KPI_Resumen_Ejecutivo
- Alineacion: OKR 2

2. CostoCombPorEntrega_Bs / CostoPorEntrega_Bs
- Formula: SUM(Consumo_Combustible_Litros * 3.72) / COUNT(Entregas)
- Vista: Vista_KPI_Costo_Por_Unidad y Vista_KPI_Resumen_Ejecutivo
- Alineacion: OKR 1

3. CO2_Total_KG y CO2_Por_KM
- Formula CO2 total: SUM(Emisiones_CO2_KG)
- Formula CO2 por km: SUM(Emisiones_CO2_KG) / SUM(Distancia_KM)
- Vista: Vista_KPI_CO2_Por_Vehiculo, Vista_KPI_Sostenibilidad_ODS
- Alineacion: OKR 3

4. Consumo_L_por_KM
- Formula: AVG(Eficiencia_L_por_KM)
- Uso: eficiencia energetica y soporte a costo y sostenibilidad

5. Utilizacion_Capacidad_Pct
- Formula: AVG(Utilizacion_Capacidad_Pct)
- Uso: productividad de carga, apoyo a reduccion de viajes y emisiones

6. IdlingPromedio_Min
- Formula: AVG(Tiempo_Idling_Minutos)
- Uso: identificar ineficiencia urbana y mejoras tacticas de ruta/paradas

### 4.3 Resultado actual observado (evidencia de ejecucion)
Datos leidos desde salida de verificacion Gold:
- Total_Entregas: 4995
- OTIF_Pct: 71.45
- CO2_Total_Toneladas: 505.11
- Consumo_Prom_L_KM: 0.1430
- Utilizacion_Prom_Pct: 77.95
- Costo_Prom_Por_Entrega_Bs: 145.77
- KM_Totales: 1,380,418

Lectura ejecutiva:
- Servicio: OTIF de 71.45% con espacio claro de mejora para estandar operativo.
- Costo: el costo promedio por entrega permite monitoreo mensual de eficiencia.
- Sostenibilidad: huella CO2 significativa y medible, apta para planes de reduccion por ruta y tipo de vehiculo.

## 5. Proceso ETL del proyecto

### 5.1 Flujo general
1. Extract (Bronze)
- Fuente: BD SQL Server LogisticaUltimaMilla.
- Extraccion por JOIN transaccional y dimensional (Entrega + Vehiculo + Conductor + Cliente + Ubicacion + Departamento).

2. Transform (Silver)
- Estandarizacion de tipos de fecha y columnas temporales.
- Normalizacion de texto (strip, colapso de espacios, title case).
- Canonizacion de estado de entrega (reemplazo de guion bajo y normalizacion).
- Limpieza de observaciones libres.

3. Imputacion de nulos (Silver)
- DistanciaKM: mediana por ruta origen-destino y fallback global.
- ConsumoCombLitros: derivado por DistanciaKM y factor por tipo de vehiculo.
- PesoCargaKG: mediana por tipo de vehiculo.
- TiempoIdlingMin: mediana por ciudad destino y fallback global.
- HoraSalida/HoraLlegada: conversion robusta para portabilidad.

4. Enriquecimiento
- KPIs base calculados: EmisionesCO2_KG, EficienciaL_por_KM, UtilizacionCapacidad_Pct, EntregadoATiempo, Ruta, TipoTrayecto.
- Integracion CEPALSTAT (silver_cepalstat.csv) por Anio.

5. Load
- Export a silver_entregas.csv.
- Carga a SQL Server como tabla Silver_Entregas.

6. Modelado analitico (Gold)
- Creacion de dimensiones y hechos en esquema estrella.
- Carga de FactEntregas con surrogate keys.
- Carga de FactEmisionesPais para benchmark.
- Creacion de vistas KPI y vista final Vista_Gold_Dashboard.

7. Consumo
- Streamlit y API FastAPI consultan KPIs y detalle desde Gold.

### 5.2 Secuencia operativa recomendada
1. Ejecutar docs/database.sql (crea y puebla Bronze).
2. Ejecutar capa_silver.py (genera Silver y carga Silver_Entregas).
3. Ejecutar docs/database_estrella.sql (crea estrella + vistas KPI).
4. Ejecutar docs/capagold.sql (ajustes finales e indices).
5. Ejecutar dashboard_streamlit.py o backend web/back/main.py.

## 6. Validacion de datos

### 6.1 Reglas de validacion implementadas
Reglas fuertes en capa_silver.py:
- 0 nulos en columnas criticas (set de columnas de negocio minimo).
- DistanciaKM > 0.
- EmisionesCO2_KG >= 0.
- UtilizacionCapacidad_Pct entre 0 y 100.
- EntregaID unico (sin duplicados).

Si alguna regla falla, el proceso detiene ejecucion con AssertionError.

### 6.2 Evidencia de calidad
- En salida_limpieza_silver.txt se reporta:
  - Problemas detectados: 237
  - Problemas arreglados: 237
  - Problemas pendientes: 0
  - Mensaje final: todas las validaciones pasaron correctamente.

- En res_capagold.txt se reporta:
  - FactEntregas (Gold): 4995 registros
  - FactEmisionesPais: 18 registros
  - Vista_Gold_Dashboard: 4995 registros

### 6.3 Validacion de integridad Gold
En database_estrella.sql se incluye verificacion de:
- Conteo por tabla dimensional y de hechos.
- Huerfanos referenciales en FactEntregas vs DimFecha.
- Consulta de muestra de vistas KPI para confirmar calculo.

## 7. Riesgos y recomendaciones

Riesgos observados:
- Credenciales de BD en texto plano en scripts y backend.
- Dependencia de archivo CEPAL CSV local si no se automatiza su actualizacion.
- OTIF actual por debajo de un nivel objetivo tipico (>90%) para ultima milla.

Recomendaciones:
1. Migrar credenciales a variables de entorno seguras y rotar password.
2. Automatizar carga de CEPAL por job programado con control de version del dato.
3. Definir meta OTIF trimestral y alertas por ruta con umbral.
4. Implementar tablero de calidad de datos (nulos, duplicados, outliers) por corrida ETL.
5. Revisar rutas criticas de mayor CO2 y mayor idling para quick wins operativos.

## 8. Conclusiones
El proyecto tiene una arquitectura BI completa y funcional:
- Base operacional con datos realistas y control de suciedad (Bronze).
- Capa de limpieza robusta con validaciones defendibles (Silver).
- Modelo estrella con KPIs alineados a objetivos de costo, servicio y sostenibilidad (Gold).

El reporte deja trazabilidad de como se calculan los indicadores y como se valida la calidad de datos antes de su consumo en dashboard y API.

-- ============================================================
--  CAPA BRONCE: LogisticaUltimaMilla
--  Proyecto: Optimizacion Logistica de Ultima Milla en Bolivia
--  Universidad Privada Domingo Savio - Inteligencia de Negocios
--  Grupo: Familia Mallku
-- ============================================================
--  NOTA PEDAGOGICA: Esta capa Bronze contiene intencionalmente:
--    - ~5% de NULLs en columnas no criticas
--    - Nombres con espacios extras y mezcla de mayusculas/minusculas
--    - Fechas en formatos mixtos (resuelto en Capa Silver)
--    - Valores atipicos de consumo/peso para ejercicio de limpieza
-- ============================================================

USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'LogisticaUltimaMilla')
    BEGIN
        ALTER DATABASE LogisticaUltimaMilla SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE LogisticaUltimaMilla;
    END
GO

CREATE DATABASE LogisticaUltimaMilla
    COLLATE Modern_Spanish_CI_AS;
GO

USE LogisticaUltimaMilla;
GO

-- ============================================================
-- TABLA 1: Departamentos (dimension geografica nivel 1)
-- ============================================================
CREATE TABLE Departamento (
                              DepartamentoID   TINYINT      NOT NULL CONSTRAINT PK_Departamento PRIMARY KEY,
                              Nombre           VARCHAR(50)  NOT NULL,
                              CodigoISO        CHAR(3)      NOT NULL,
                              Region           VARCHAR(30)  NOT NULL   -- Altiplano, Valles, Llanos
);

INSERT INTO Departamento VALUES
                             (1,  'La Paz',        'LPZ', 'Altiplano'),
                             (2,  'Cochabamba',    'CBB', 'Valles'),
                             (3,  'Santa Cruz',    'SCZ', 'Llanos'),
                             (4,  'Oruro',         'ORU', 'Altiplano'),
                             (5,  'Potosi',        'POT', 'Altiplano'),
                             (6,  'Tarija',        'TJA', 'Valles'),
                             (7,  'Beni',          'BEN', 'Llanos'),
                             (8,  'Pando',         'PAN', 'Llanos'),
                             (9,  'Chuquisaca',    'CHU', 'Valles');

-- ============================================================
-- TABLA 2: Ciudades / Ubicaciones (dimension geografica nivel 2)
-- ============================================================
CREATE TABLE Ubicacion (
                           UbicacionID      SMALLINT     NOT NULL CONSTRAINT PK_Ubicacion PRIMARY KEY,
                           DepartamentoID   TINYINT      NOT NULL CONSTRAINT FK_Ubic_Depto FOREIGN KEY REFERENCES Departamento(DepartamentoID),
                           Ciudad           VARCHAR(60)  NOT NULL,
                           ZonaUrbanRural   VARCHAR(10)  NOT NULL DEFAULT 'Urbana',  -- Urbana / Rural
                           LatitudAprox     DECIMAL(8,5) NULL,
                           LongitudAprox    DECIMAL(8,5) NULL
);

INSERT INTO Ubicacion VALUES
                          (1,  1, 'La Paz',           'Urbana',  -16.50000, -68.15000),
                          (2,  1, 'El Alto',          'Urbana',  -16.50200, -68.19300),
                          (3,  1, 'Viacha',           'Rural',   -16.65000, -68.29800),
                          (4,  2, 'Cochabamba',       'Urbana',  -17.39380, -66.15680),
                          (5,  2, 'Quillacollo',      'Urbana',  -17.39600, -66.28000),
                          (6,  2, 'Sacaba',           'Urbana',  -17.37600, -66.03900),
                          (7,  3, 'Santa Cruz',       'Urbana',  -17.78629, -63.18117),
                          (8,  3, 'Montero',          'Urbana',  -17.34100, -63.25200),
                          (9,  3, 'Warnes',           'Rural',   -17.50000, -63.16700),
                          (10, 4, 'Oruro',            'Urbana',  -17.96800, -67.10700),
                          (11, 5, 'Potosi',           'Urbana',  -19.58500, -65.75300),
                          (12, 6, 'Tarija',           'Urbana',  -21.53550, -64.72950),
                          (13, 6, 'Yacuiba',          'Urbana',  -22.02500, -63.67900),
                          (14, 7, 'Trinidad',         'Urbana',  -14.83300, -64.90000),
                          (15, 9, 'Sucre',            'Urbana',  -19.04770, -65.25940);

-- ============================================================
-- TABLA 3: Vehiculos (dimension de flota)
-- ============================================================
CREATE TABLE Vehiculo (
                          VehiculoID        SMALLINT     NOT NULL CONSTRAINT PK_Vehiculo PRIMARY KEY,
                          Placa             VARCHAR(10)  NOT NULL,
                          Tipo              VARCHAR(30)  NOT NULL,  -- Camion, Furgoneta, Moto, Bicicleta
                          Modelo            VARCHAR(50)  NOT NULL,
                          AnoFabricacion    SMALLINT     NOT NULL,
                          CapacidadMaxKG    DECIMAL(8,2) NOT NULL,
                          CapacidadMaxM3    DECIMAL(6,2) NULL,
                          FactorEmisionKgCO2PorLitro DECIMAL(5,3) NOT NULL DEFAULT 2.690,
                          Activo            BIT          NOT NULL DEFAULT 1
);

INSERT INTO Vehiculo VALUES
                         (1,  '2341-LPZ', 'Camion',    'Hino 300',          2018, 3500.00, 18.00, 2.690, 1),
                         (2,  '8812-SCZ', 'Camion',    'Hino 300',          2020, 3500.00, 18.00, 2.690, 1),
                         (3,  '1193-CBB', 'Camion',    'Hyundai HD65',      2019, 5000.00, 22.00, 2.690, 1),
                         (4,  '4421-LPZ', 'Camion',    'Hyundai HD65',      2017, 5000.00, 22.00, 2.690, 1),
                         (5,  '7731-SCZ', 'Camion',    'Mercedes Sprinter', 2021, 1500.00, 12.00, 2.690, 1),
                         (6,  '2209-CBB', 'Furgoneta', 'Toyota Hiace',      2020,  800.00,  6.50, 2.310, 1),
                         (7,  '5543-TJA', 'Furgoneta', 'Toyota Hiace',      2019,  800.00,  6.50, 2.310, 1),
                         (8,  '9901-ORU', 'Furgoneta', 'Nissan NV350',      2021,  900.00,  7.00, 2.310, 1),
                         (9,  '3378-LPZ', 'Furgoneta', 'Renault Trafic',    2018,  850.00,  6.80, 2.310, 1),
                         (10, '6612-SCZ', 'Camion',    'Volvo FL',          2016, 8000.00, 35.00, 2.690, 1),
                         (11, '1140-CBB', 'Camion',    'Volvo FL',          2022, 8000.00, 35.00, 2.690, 1),
                         (12, '8823-TJA', 'Furgoneta', 'Mercedes Vito',     2020,  700.00,  5.80, 2.310, 1),
                         (13, '4455-SCZ', 'Camion',    'Hino 500',          2021, 6000.00, 28.00, 2.690, 1),
                         (14, '7788-LPZ', 'Furgoneta', 'Ford Transit',      2022,  950.00,  7.20, 2.310, 1),
                         (15, '2211-CBB', 'Camion',    'Mercedes Accelo',   2019, 4500.00, 20.00, 2.690, 1),
                         (16, '5566-ORU', 'Furgoneta', 'VW Crafter',        2020,  850.00,  6.60, 2.310, 1),
                         (17, '9934-SCZ', 'Camion',    'Scania P280',       2020, 10000.0, 42.00, 2.690, 1),
                         (18, '3312-TJA', 'Camion',    'Hino 300',          2023, 3500.00, 18.00, 2.690, 1),
                         (19, '6690-LPZ', 'Furgoneta', 'Peugeot Boxer',     2021,  900.00,  7.10, 2.310, 1),
                         (20, '1178-CBB', 'Camion',    'Hyundai HD78',      2022, 4000.00, 19.00, 2.690, 1);

-- ============================================================
-- TABLA 4: Conductores (dimension de personal)
-- ============================================================
CREATE TABLE Conductor (
                           ConductorID      SMALLINT     NOT NULL CONSTRAINT PK_Conductor PRIMARY KEY,
                           Nombre           VARCHAR(80)  NOT NULL,  -- CONTIENE SUCIEDAD: espacios/mayusc mixtas
                           CI               VARCHAR(15)  NOT NULL,
                           Telefono         VARCHAR(20)  NULL,      -- ~5% NULLs intencionales
                           LicenciaCateg    VARCHAR(5)   NOT NULL,
                           FechaIngreso     DATE         NOT NULL,
                           UbicacionBaseID  SMALLINT     NOT NULL CONSTRAINT FK_Cond_Ubic FOREIGN KEY REFERENCES Ubicacion(UbicacionID),
                           Activo           BIT          NOT NULL DEFAULT 1
);

-- Nombres con impurezas intencionales (espacios, mayusculas mixtas)
INSERT INTO Conductor VALUES
                          (1,  'Juan Carlos  Mamani Quispe',  '7234561',  '72345610', 'B',   '2020-03-15', 1,  1),
                          (2,  'PEDRO alvarado flores',       '8123456',  '81234560', 'C',   '2019-07-22', 7,  1),
                          (3,  'Maria Elena Condori ',        '6543210',  NULL,       'B',   '2021-01-10', 4,  1),  -- NULL telefono
                          (4,  'Carlos  Mendoza Torrez',      '9876543',  '98765430', 'C',   '2020-11-05', 7,  1),
                          (5,  'ana lucia  Vargas Rojas',     '5432109',  '54321090', 'B',   '2022-02-28', 4,  1),
                          (6,  'Roberto Quispe  Mamani',      '3210987',  '32109870', 'CE',  '2019-04-18', 1,  1),
                          (7,  'LUISA Fernanda Gutierrez',    '6789012',  NULL,       'B',   '2021-09-03', 12, 1),  -- NULL telefono
                          (8,  'Sergio  Antonio Ramos',       '2345678',  '23456780', 'C',   '2020-06-14', 7,  1),
                          (9,  'Patricia Solis Aguilar',      '8901234',  '89012340', 'B',   '2022-04-20', 4,  1),
                          (10, 'DANIEL  Eduardo Perez',       '4567890',  '45678900', 'CE',  '2019-12-01', 1,  1),
                          (11, 'Verónica  Choque Laime',      '7890123',  '78901230', 'B',   '2021-07-07', 10, 1),
                          (12, 'Alejandro  Torrez  Mamani',   '1234567',  '12345670', 'C',   '2020-08-25', 7,  1),
                          (13, 'Carmen  Rosa Ibañez',         '9012345',  NULL,       'B',   '2022-01-15', 12, 1),  -- NULL telefono
                          (14, 'Felix Gutierrez  Ticona',     '3456789',  '34567890', 'CE',  '2019-10-30', 1,  1),
                          (15, 'MIRIAM condori  Vargas',      '5678901',  '56789010', 'B',   '2021-03-22', 4,  1),
                          (16, 'Hugo  Alejandro Salinas',     '2109876',  '21098760', 'C',   '2020-05-11', 7,  1),
                          (17, 'Sonia  Beatriz Flores',       '7654321',  '76543210', 'B',   '2022-06-08', 12, 1),
                          (18, 'Marcos Antonio  Medina',      '3098765',  NULL,       'CE',  '2019-02-14', 1,  1),  -- NULL telefono
                          (19, 'ANGELA  Sofia Mamani',        '8765432',  '87654320', 'B',   '2021-11-19', 10, 1),
                          (20, 'Ruben  Dario Sandoval',       '4321098',  '43210980', 'C',   '2020-09-03', 7,  1);

-- ============================================================
-- TABLA 5: Clientes (dimension comercial)
-- ============================================================
CREATE TABLE Cliente (
                         ClienteID        SMALLINT     NOT NULL CONSTRAINT PK_Cliente PRIMARY KEY,
                         NombreEmpresa    VARCHAR(100) NOT NULL,  -- CONTIENE SUCIEDAD: espacios/formato
                         Contacto         VARCHAR(80)  NULL,
                         Telefono         VARCHAR(20)  NULL,
                         Email            VARCHAR(80)  NULL,      -- ~5% NULLs intencionales
                         UbicacionID      SMALLINT     NOT NULL CONSTRAINT FK_Cli_Ubic FOREIGN KEY REFERENCES Ubicacion(UbicacionID),
                         Segmento         VARCHAR(20)  NOT NULL   -- Retail, Farmacia, Alimentos, Industrial
);

INSERT INTO Cliente VALUES
                        (1,  'Supermercados KETAL  S.A.',    'Jorge Lima',     '22334455', 'jlima@ketal.bo',      7,  'Retail'),
                        (2,  'Farmacia CHAVI  Bolivia',      'Ana Blanco',     '33445566', NULL,                   1,  'Farmacia'),  -- NULL email
                        (3,  'distribuidora El CONDOR',      'Pedro Soria',    '44556677', 'psoria@condor.bo',    4,  'Alimentos'),
                        (4,  'INDUSTRIAS Bolivianas S.R.L',  'Rosa Mamani',    '55667788', 'rmamani@ibsrl.bo',   7,  'Industrial'),
                        (5,  'Tiendas  BOLIVAR S.A.',        'Luis Torres',    '66778899', 'ltorres@bolivar.bo',  1,  'Retail'),
                        (6,  'FarmaBolivia  Ltda.',          'Clara Quispe',   '77889900', NULL,                   4,  'Farmacia'),  -- NULL email
                        (7,  'Alimentos  DEL NORTE',         'Mario Arce',     '88990011', 'marce@delnorte.bo',  7,  'Alimentos'),
                        (8,  'Construcciones  ANDINAS',      'Sofia Reyes',    '99001122', 'sreyes@candinas.bo',  10, 'Industrial'),
                        (9,  'SUPERMERCADOS HIPERMAXI',      'Carlos Vega',    '11223344', 'cvega@hipermaxi.bo',  7,  'Retail'),
                        (10, 'Farmacorp  S.A.',              'Elena Rojas',    '22334455', 'erojas@farmacorp.bo', 7,  'Farmacia'),
                        (11, 'distribuidora ANDINA  ltda.',  'Ivan Morales',   '33445560', NULL,                   4,  'Alimentos'),  -- NULL email
                        (12, 'Plasticos  DEL SUR S.A.',      'Claudia Paz',    '44556670', 'cpaz@psur.bo',        12, 'Industrial'),
                        (13, 'TIENDA  express boliviana',    'Roberto Cruz',   '55667780', 'rcruz@teb.bo',        1,  'Retail'),
                        (14, 'Botica  Popular  Tarija',      'Lucia Ordoñez',  '66778890', 'lordoniez@bp.bo',    12, 'Farmacia'),
                        (15, 'Importadora  EL AGUILA',       'Franco Aguila',  '77889900', 'faguila@eaguila.bo', 7,  'Alimentos'),
                        (16, 'Metalurgica  CHUQUISACA',      'Diana Salinas',  '88990010', NULL,                   15, 'Industrial'),  -- NULL email
                        (17, 'JUMBO  Supermercados',         'Pablo Fuentes',  '99001120', 'pfuentes@jumbo.bo',  7,  'Retail'),
                        (18, 'FarmaRed  Bolivia',            'Natalia Perez',  '11223340', 'nperez@farmared.bo', 4,  'Farmacia'),
                        (19, 'Alimentos  ORIENTE  S.A.',     'Ricardo Vaca',   '22334450', 'rvaca@aoriente.bo',  7,  'Alimentos'),
                        (20, 'Textil  BOLIVIANA  S.R.L.',    'Marcela Cobo',   '33445550', 'mcobo@tboliviana.bo',7,  'Industrial');

-- ============================================================
-- TABLA TRANSACCIONAL: Entregas (Hecho principal - Capa Bronze)
-- ============================================================
-- NOTA: Esta tabla recibe datos "sucios" del sistema operacional.
-- Los NULL, valores extremos y formatos inconsistentes son intencionales.
CREATE TABLE Entrega (
                         EntregaID            INT           NOT NULL CONSTRAINT PK_Entrega PRIMARY KEY IDENTITY(1,1),
                         FechaEntrega         DATE          NOT NULL,
                         HoraSalida           TIME(0)       NULL,      -- ~5% NULLs
                         HoraLlegada          TIME(0)       NULL,      -- ~5% NULLs
                         VehiculoID           SMALLINT      NOT NULL CONSTRAINT FK_Entr_Veh FOREIGN KEY REFERENCES Vehiculo(VehiculoID),
                         ConductorID          SMALLINT      NOT NULL CONSTRAINT FK_Entr_Cond FOREIGN KEY REFERENCES Conductor(ConductorID),
                         ClienteID            SMALLINT      NOT NULL CONSTRAINT FK_Entr_Cli FOREIGN KEY REFERENCES Cliente(ClienteID),
                         UbicacionOrigenID    SMALLINT      NOT NULL CONSTRAINT FK_Entr_Orig FOREIGN KEY REFERENCES Ubicacion(UbicacionID),
                         UbicacionDestinoID   SMALLINT      NOT NULL CONSTRAINT FK_Entr_Dest FOREIGN KEY REFERENCES Ubicacion(UbicacionID),
                         DistanciaKM          DECIMAL(8,2)  NULL,      -- ~3% NULLs: sensor GPS fallo
                         ConsumoCombLitros    DECIMAL(8,3)  NULL,      -- ~3% NULLs: no se registro
                         PesoCargaKG          DECIMAL(9,2)  NULL,      -- ~5% NULLs: no pesado
                         TiempoIdlingMin      INT           NULL,      -- ~5% NULLs
                         EstadoEntrega        VARCHAR(20)   NOT NULL,  -- Entregado / En_Ruta / Fallido / Pendiente
                         ObservacionRaw       VARCHAR(200)  NULL,      -- texto libre sin normalizar (sucio)
                         FechaRegistro        DATETIME      NOT NULL DEFAULT GETDATE()
);

-- ============================================================
-- GENERACION MASIVA DE DATOS: 5000 entregas con WHILE loop
-- ============================================================
-- Evita los 5000 INSERTs individuales. El loop genera datos
-- variados, realistas y con impurezas controladas (~5%).
-- ============================================================

DECLARE
    @i                  INT = 1,
    @MaxRows            INT = 5000,

    -- Auxiliares para calculo
    @VehicID            SMALLINT,
    @CondID             SMALLINT,
    @CliID              SMALLINT,
    @OrigID             SMALLINT,
    @DestID             SMALLINT,
    @Fecha              DATE,
    @Estado             VARCHAR(20),
    @DistKM             DECIMAL(8,2),
    @Consumo            DECIMAL(8,3),
    @PesoKG             DECIMAL(9,2),
    @IdlingMin          INT,
    @HoraSal            TIME(0),
    @HoraLleg           TIME(0),
    @Obs                VARCHAR(200),

    -- Contadores para distribucion de impurezas (5% ~ 1 de cada 20)
    @EsNuloGPS          BIT,
    @EsNuloConsumo      BIT,
    @EsNuloPeso         BIT,
    @EsNuloHora         BIT,
    @EsNuloIdling       BIT,

    -- Para estados coherentes con fechas
    @DiasSemana         TINYINT,
    @FactorRuta         DECIMAL(4,2);

-- Semilla para pseudo-aleatoriedad reproducible
SET NOCOUNT ON;

WHILE @i <= @MaxRows
    BEGIN
        -- ---- Dimensiones ----
        SET @VehicID  = ((@i * 7 + 3)  % 20) + 1;
        SET @CondID   = ((@i * 11 + 5) % 20) + 1;
        SET @CliID    = ((@i * 13 + 7) % 20) + 1;
        SET @OrigID   = ((@i * 3 + 1)  % 15) + 1;

        -- Destino distinto al origen
        SET @DestID = (((@i * 17 + 9) % 15) + 1);
        IF @DestID = @OrigID SET @DestID = (@DestID % 15) + 1;

        -- ---- Fecha: ultimos 3 anios (2022-2024) ----
        SET @Fecha = DATEADD(DAY, -((@i * 19 + 3) % 1095), CAST('2025-01-01' AS DATE));

        -- ---- Factor de ruta (distancia base segun par origen-destino) ----
        SET @FactorRuta = CASE
                              WHEN ABS(@OrigID - @DestID) <= 2  THEN 1.0   -- misma zona, corta
                              WHEN ABS(@OrigID - @DestID) <= 5  THEN 2.2   -- inter-ciudad
                              ELSE 3.8                                       -- inter-departamental
            END;

        -- ---- Distancia KM ----
        SET @EsNuloGPS = CASE WHEN (@i % 33) = 0 THEN 1 ELSE 0 END; -- ~3%
        IF @EsNuloGPS = 1
            SET @DistKM = NULL;
        ELSE
            SET @DistKM = CAST(
                    (15.0 + ((@i * 23 + @OrigID * 7) % 180)) * @FactorRuta
                AS DECIMAL(8,2));

        -- ---- Consumo de combustible ----
        SET @EsNuloConsumo = CASE WHEN (@i % 31) = 0 THEN 1 ELSE 0 END; -- ~3%
        IF @EsNuloConsumo = 1 OR @DistKM IS NULL
            SET @Consumo = NULL;
        ELSE
            -- L/km varia por tipo vehiculo: camiones ~0.15-0.22, furgonetas ~0.08-0.12
            SET @Consumo = CAST(
                    @DistKM * CASE
                                  WHEN @VehicID IN (1,2,3,4,10,11,13,15,17,18,20) THEN  -- Camiones
                                      0.150 + ((@i * 3) % 7) * 0.010
                                  ELSE                                                    -- Furgonetas
                                      0.085 + ((@i * 5) % 4) * 0.009
                        END
                AS DECIMAL(8,3));

        -- ---- Peso de carga ----
        SET @EsNuloPeso = CASE WHEN (@i % 19) = 0 THEN 1 ELSE 0 END; -- ~5%
        IF @EsNuloPeso = 1
            SET @PesoKG = NULL;
        ELSE
            BEGIN
                -- Peso coherente con capacidad del vehiculo (60-95% de carga)
                DECLARE @CapacidadBase DECIMAL(8,2);
                SET @CapacidadBase = CASE
                                         WHEN @VehicID IN (10,11,17)     THEN 8000.0   -- Volvo/Scania grandes
                                         WHEN @VehicID IN (3,4,15,20)    THEN 5000.0   -- HD65/HD78
                                         WHEN @VehicID IN (13)           THEN 6000.0   -- Hino 500
                                         WHEN @VehicID IN (1,2,5,18)     THEN 3500.0   -- Hino 300
                                         ELSE                                 850.0    -- Furgonetas
                    END;
                SET @PesoKG = CAST(
                        @CapacidadBase * (0.60 + (((@i * 7 + @CliID) % 36) * 0.01))
                    AS DECIMAL(9,2));
            END

        -- ---- Tiempo de Idling ----
        SET @EsNuloIdling = CASE WHEN (@i % 21) = 0 THEN 1 ELSE 0 END; -- ~5%
        IF @EsNuloIdling = 1
            SET @IdlingMin = NULL;
        ELSE
            -- Idling mayor en ciudades densas (La Paz, SCZ, CBB)
            SET @IdlingMin = CASE
                                 WHEN @DestID IN (1,2,7,4,5) THEN 8  + ((@i * 11) % 40)  -- ciudades grandes
                                 ELSE                              3  + ((@i * 7)  % 20)  -- ciudades pequenas
                END;

        -- ---- Hora salida y llegada ----
        SET @EsNuloHora = CASE WHEN (@i % 23) = 0 THEN 1 ELSE 0 END; -- ~4%
        IF @EsNuloHora = 1
            BEGIN
                SET @HoraSal  = NULL;
                SET @HoraLleg = NULL;
            END
        ELSE
            BEGIN
                SET @HoraSal = CAST(
                        CAST(DATEADD(MINUTE,
                                     ((@i * 17 + 360) % 720) + 240,  -- entre 04:00 y 16:00
                                     CAST('00:00:00' AS DATETIME)) AS TIME(0)) AS TIME(0));

                SET @HoraLleg = CAST(
                        CAST(DATEADD(MINUTE,
                                     CAST(DATEDIFF(MINUTE, '00:00:00', @HoraSal) AS INT)
                                         + 30 + ((@i * 13 + ISNULL(@IdlingMin,10)) % 300),
                                     CAST('00:00:00' AS DATETIME)) AS TIME(0)) AS TIME(0));
            END

        -- ---- Estado de entrega (coherente con patron temporal) ----
        SET @DiasSemana = DATEPART(WEEKDAY, @Fecha);
        SET @Estado = CASE
                          WHEN (@i % 100) < 72  THEN 'Entregado'    -- 72%
                          WHEN (@i % 100) < 84  THEN 'En_Ruta'      -- 12%
                          WHEN (@i % 100) < 92  THEN 'Fallido'      -- 8%
                          ELSE                       'Pendiente'     -- 8%
            END;
        -- Ajuste: lunes y viernes tienen mas fallidos
        IF @DiasSemana IN (2,6) AND (@i % 100) BETWEEN 70 AND 79
            SET @Estado = 'Fallido';

        -- ---- Observacion (texto sucio sin normalizar) ----
        SET @Obs = CASE (@i % 12)
                       WHEN 0  THEN 'entregado ok'
                       WHEN 1  THEN 'CLIENTE  AUSENTE - se dejo en porteria'
                       WHEN 2  THEN NULL
                       WHEN 3  THEN 'Retraso por trafico  en zona centro'
                       WHEN 4  THEN 'Entregado Sin Novedad'
                       WHEN 5  THEN 'camino en mal estado,  se demoro mas'
                       WHEN 6  THEN NULL
                       WHEN 7  THEN 'DEVOLUCION PARCIAL  - falta 1 bulto'
                       WHEN 8  THEN 'entrego el  ayudante'
                       WHEN 9  THEN 'Zona  de dificil acceso por lluvia'
                       WHEN 10 THEN NULL
                       ELSE         'Sin Observacion'
            END;

        -- ---- Insercion ----
        INSERT INTO Entrega (
            FechaEntrega, HoraSalida, HoraLlegada,
            VehiculoID, ConductorID, ClienteID,
            UbicacionOrigenID, UbicacionDestinoID,
            DistanciaKM, ConsumoCombLitros, PesoCargaKG,
            TiempoIdlingMin, EstadoEntrega, ObservacionRaw
        )
        VALUES (
                   @Fecha, @HoraSal, @HoraLleg,
                   @VehicID, @CondID, @CliID,
                   @OrigID, @DestID,
                   @DistKM, @Consumo, @PesoKG,
                   @IdlingMin, @Estado, @Obs
               );

        SET @i = @i + 1;
    END

PRINT 'Insercion completada: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' registros en Entrega.';
PRINT 'Total acumulado: 5000 registros (WHILE loop).';

-- ============================================================
-- VERIFICACION FINAL DE CAPA BRONZE
-- ============================================================
-- Ejecutar para confirmar el estado "sucio" antes de Silver

SELECT 'Entrega'      AS Tabla, COUNT(*) AS TotalRegistros FROM Entrega
UNION ALL
SELECT 'Vehiculo',    COUNT(*) FROM Vehiculo
UNION ALL
SELECT 'Conductor',   COUNT(*) FROM Conductor
UNION ALL
SELECT 'Cliente',     COUNT(*) FROM Cliente
UNION ALL
SELECT 'Ubicacion',   COUNT(*) FROM Ubicacion
UNION ALL
SELECT 'Departamento',COUNT(*) FROM Departamento;

-- Diagnostico de nulos (evidencia para Actividad 6 - Data Scrubbing)
SELECT
    'Entrega'                                  AS Tabla,
    SUM(CASE WHEN DistanciaKM         IS NULL THEN 1 ELSE 0 END) AS Nulos_DistanciaKM,
    SUM(CASE WHEN ConsumoCombLitros   IS NULL THEN 1 ELSE 0 END) AS Nulos_Consumo,
    SUM(CASE WHEN PesoCargaKG         IS NULL THEN 1 ELSE 0 END) AS Nulos_PesoKG,
    SUM(CASE WHEN TiempoIdlingMin     IS NULL THEN 1 ELSE 0 END) AS Nulos_Idling,
    SUM(CASE WHEN HoraSalida          IS NULL THEN 1 ELSE 0 END) AS Nulos_Hora,
    SUM(CASE WHEN ObservacionRaw      IS NULL THEN 1 ELSE 0 END) AS Nulos_Obs,
    COUNT(*)                                                       AS Total
FROM Entrega;

SELECT
    'Conductor'                                AS Tabla,
    SUM(CASE WHEN Telefono IS NULL THEN 1 ELSE 0 END) AS Nulos_Telefono,
    COUNT(*)                                           AS Total
FROM Conductor;

SELECT
    'Cliente'                                  AS Tabla,
    SUM(CASE WHEN Email    IS NULL THEN 1 ELSE 0 END) AS Nulos_Email,
    COUNT(*)                                           AS Total
FROM Cliente;

-- Muestra de "suciedad" en nombres (espacios extras, mayusculas mixtas)
SELECT TOP 10
    ConductorID,
    Nombre AS Nombre_Sucio,
    LEN(Nombre) - LEN(LTRIM(RTRIM(Nombre))) AS EspaciosExternos,
    CHARINDEX('  ', Nombre) AS TieneDobleEspacio
FROM Conductor
ORDER BY ConductorID;

PRINT '=== Capa Bronze lista. Proceder con capa_silver.py ===';
GO

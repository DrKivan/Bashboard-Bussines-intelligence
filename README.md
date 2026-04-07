# 🚚 Optimización Logística de Última Milla en Bolivia

**Materia:** Inteligencia de Negocios (BI) | UPDS  
**Grupo:** Familia Mallku  

Este proyecto implementa una arquitectura de Inteligencia de Negocios end-to-end para optimizar la logística de última milla a nivel nacional. Su objetivo es unificar datos operativos con métricas externas (CEPALSTAT) para mejorar la eficiencia, reducir costos y minimizar el impacto ambiental a través de un panel de decisiones proactivo.

---

## 🎯 OKRs y KPIs del Negocio

El proyecto está diseñado para monitorear y mejorar tres Resultados Clave (OKRs) mediante KPIs específicos:

1. **Eficiencia Operativa y de Costos:**
   - **KPIs:** Costo Promedio por Entrega (Bs), Costo por KM, Eficiencia (Litros/KM).
2. **Nivel de Servicio al Cliente:**
   - **KPIs:** OTIF (On Time In Full - % de entregas a tiempo), Utilización de Capacidad (% de carga).
3. **Sostenibilidad y Huella Ambiental:**
   - **KPIs:** Emisiones Totales (KG de CO₂), Emisiones por Vehículo, Tiempo de Idling (Ralentí), Benchmark vs CEPALSTAT.

---

## 🏗️ Arquitectura de Datos (Medallón)

El flujo de datos sigue un estándar empresarial de **Arquitectura de 3 Capas**:

- 🟤 **Capa Bronce (Operacional):** Base de datos relacional (OLTP) en SQL Server (`docs/database.sql`). Simula un entorno real capturando datos "sucios" con nulos deliberados, errores tipográficos y ruidos en sensores GPS.
- ⚪ **Capa Silver (ETL y Limpieza):** Un pipeline de Python (`capa_silver.py`) que extrae, estandariza textos, normaliza estados, e imputa valores faltantes (como distancias o consumos usando medianas operativas).
- 🟡 **Capa Gold (Analítica):** Modelo en Esquema Estrella (`docs/database_estrella.sql`, `docs/capagold.sql`). Crea dimensiones separadas y una tabla de hechos super-indexada. Entrega una vista denormalizada (`Vista_Gold_Dashboard`) optimizada para la lectura rápida desde el dashboard web.

---

## 🤖 Sistema Proactivo de Toma de Decisiones (IA)

A diferencia de un BI tradicional (reactivo/descriptivo), este proyecto integra un **Asistente Ejecutivo de IA** (Impulsado por OpenRouter / Gemini a través de FastAPI). 
*   **Funcionalidad:** Lee el contexto operacional (tendencia de entregas vs CO₂, caída en OTIF).
*   **Valor:** Emite diagnósticos, alerta sobre **riesgos inminentes** antes de que consoliden pérdidas económicas y propone **recomendaciones accionables a corto plazo** (ej. optimizar despachos, mantenimiento preventivo).

---

## 🚀 Tecnologías Utilizadas

*   **Motor de Base de Datos:** SQL Server (T-SQL).
*   **ETL y Procesamiento:** Python 3, Pandas, SQLAlchemy.
*   **Backend API:** FastAPI, Pydantic (Validación de esquemas).
*   **Frontend:** React (Vite, TypeScript, Tailwind) consumiendo la API REST.
*   **IA Generativa:** Modelos LLM a través de OpenRouter API y Google Gemini API.

---

## ⚙️ Instrucciones de Instalación y Ejecución

### 1. Base de Datos (SQL Server)
1. Ejecutar el script `docs/database.sql` para generar la Capa Bronce (creación de tablas y carga transaccional simulada).
2. Ejecutar `capa_silver.py` para realizar la limpieza ETL.
3. Ejecutar el script `docs/database_estrella.sql` para construir el esquema estrella (Capa Gold).
4. Ejecutar el script `docs/capagold.sql` para aplicar transformaciones finales, índices y vistas.

### 2. Backend (FastAPI)
1. Navegar a la carpeta del backend:
   ```bash
   cd web/back
   ```
2. Instalar las dependencias (asegúrate de tener activo tu entorno virtual `.venv`):
   ```bash
   pip install fastapi uvicorn pandas sqlalchemy pyodbc pydantic
   ```
3. Configurar las variables de entorno en un archivo `.env` dentro de `web/back/`:
   ```dotenv
   DB_SERVER=localhost
   DB_NAME=LogisticaUltimaMilla
   OPENROUTER_API_KEY=tu_api_key_aqui
   GEMINI_API_KEY=tu_api_key_aqui
   ```
4. Iniciar el servidor API:
   ```bash
   uvicorn main:app --reload
   ```
   *(El backend correrá en `http://localhost:8000`).*

### 3. Frontend (Dashboard React)
1. Abrir otra terminal y navegar a la carpeta del dashboard:
   ```bash
   cd web/dashboard
   ```
2. Instalar las dependencias (usando pnpm, npm o yarn):
   ```bash
   pnpm install
   ```
3. Iniciar el servidor de desarrollo:
   ```bash
   pnpm run dev
   ```
4. Acceder al dashboard interactivo desde el navegador (usualmente en `http://localhost:5173`).

---

## 📂 Estructura Principal del Repositorio

```text
├── capa_silver.py            # Pipeline de limpieza de datos ETL (Bronce -> Silver)
├── REPORTE_PROYECTO.md       # Documento con justificación técnica y negocio
├── docs/
│   ├── database.sql          # Creación base de datos operacional (Bronce)
│   ├── database_estrella.sql # Creación esquema estrella DWH (Gold)
│   └── capagold.sql          # Vistas e índices finales
└── web/
    ├── back/
    │   ├── main.py           # Backend FastAPI (Endpoints + IA Assistant)
    │   └── .env              # Claves de motor LLM y Base de Datos
    └── dashboard/            # Frontend Web App (React + Vite + TypeScript)
```
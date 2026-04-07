import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

app = FastAPI(
    title="Logística Última Milla API",
    description="Backend para Dashboard React (Capa Gold)",
    version="1.0.0",
)

# CORS: en producción reemplaza "*" por dominio frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def load_local_env() -> None:
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"").strip("'"))


load_local_env()

# Conexion SQL Server igual a capa_silver.py (Windows Auth, sin .env para DB)
DB_SERVER = "LAPTOP-0FE1UK8O"
DB_NAME = "LogisticaUltimaMilla"
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = os.getenv(
    "OPENROUTER_MODEL", "nvidia/nemotron-3-super-120b-a12b:free"
)
GEMINI_API_URL_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-flash-latest")

DECISION_ASSISTANT_CONTEXT = """
Principios de analisis para este dashboard:
- Prioriza claridad ejecutiva y decisiones accionables.
- Menos es mas: no saturar con detalles innecesarios.
- Conectar operacion (entregas, servicio, capacidad) con impacto ambiental.
- Evaluar tendencias, no solo fotos puntuales.
- Proponer acciones concretas de corto plazo (7-30 dias).
""".strip()

DECISION_ASSISTANT_SYSTEM_PROMPT = f"""Eres un asesor ejecutivo para un dashboard de logistica de ultima milla.

{DECISION_ASSISTANT_CONTEXT}

Reglas de respuesta:
- Responde siempre en espanol claro y profesional.
- Prioriza recomendaciones accionables para operacion, servicio y sostenibilidad.
- Si detectas riesgo, explica impacto de negocio y accion sugerida.
- Usa un formato breve:
  1) Diagnostico
  2) Riesgos
  3) Acciones recomendadas
  4) Pregunta de seguimiento
- No inventes datos fuera del contexto entregado.
""".strip()


class AssistantInputMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1)
    reasoning_details: Any | None = None


class DecisionAssistantRequest(BaseModel):
    messages: List[AssistantInputMessage] = Field(default_factory=list)
    dashboard_context: Dict[str, Any] = Field(default_factory=dict)
    provider: Literal["openrouter", "gemini"] = "openrouter"


class DecisionAssistantResponse(BaseModel):
    content: str
    reasoning_details: Any | None = None
    model: str


def build_dashboard_filters(
    start_date: str | None,
    end_date: str | None,
    route: str | None,
    status: str | None,
    table_alias: str | None = None,
) -> Tuple[str, Dict[str, Any]]:
    clauses: List[str] = []
    params: Dict[str, Any] = {}
    prefix = f"{table_alias}." if table_alias else ""

    if start_date:
        clauses.append(f"{prefix}Fecha >= :start_date")
        params["start_date"] = start_date

    if end_date:
        clauses.append(f"{prefix}Fecha <= :end_date")
        params["end_date"] = end_date

    if route:
        clauses.append(f"{prefix}Ruta = :route")
        params["route"] = route

    if status:
        clauses.append(f"{prefix}EstadoEntrega = :status")
        params["status"] = status

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_clause, params


def get_engine():
    conn_str = (
        f"mssql+pyodbc://{DB_SERVER}/{DB_NAME}"
        f"?driver={ODBC_DRIVER.replace(' ', '+')}"
        "&trusted_connection=yes"
        "&TrustServerCertificate=yes"
    )
    return create_engine(conn_str, fast_executemany=True)


def rows_to_json(df: pd.DataFrame) -> List[Dict[str, Any]]:
    df = df.where(pd.notnull(df), None)
    # Serializar fecha si existe
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
    return df.to_dict(orient="records")


def build_assistant_messages(
    payload: DecisionAssistantRequest,
) -> List[Dict[str, Any]]:
    # Limitar historial evita payloads excesivos y mantiene continuidad reciente.
    history_slice = payload.messages[-12:]
    context_json = json.dumps(payload.dashboard_context, ensure_ascii=False)

    messages: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": (
                f"{DECISION_ASSISTANT_SYSTEM_PROMPT}\n\n"
                f"Contexto del dashboard (JSON):\n{context_json}"
            ),
        }
    ]

    for item in history_slice:
        message: Dict[str, Any] = {
            "role": item.role,
            "content": item.content,
        }
        if item.reasoning_details is not None:
            message["reasoning_details"] = item.reasoning_details
        messages.append(message)

    return messages


def build_gemini_prompt(payload: DecisionAssistantRequest) -> str:
    history_slice = payload.messages[-12:]
    context_json = json.dumps(payload.dashboard_context, ensure_ascii=False)

    conversation_lines: List[str] = []
    for item in history_slice:
        speaker = "Usuario" if item.role == "user" else "Asistente"
        conversation_lines.append(f"{speaker}: {item.content}")

    conversation_text = "\n".join(conversation_lines)
    return (
        f"{DECISION_ASSISTANT_SYSTEM_PROMPT}\n\n"
        f"Contexto del dashboard (JSON):\n{context_json}\n\n"
        f"Conversacion reciente:\n{conversation_text}\n\n"
        "Responde solo como asistente ejecutivo en espanol."
    )


def call_openrouter_assistant(payload: DecisionAssistantRequest) -> Dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return {
            "content": (
                "Asistente disponible en modo guiado. Para habilitar respuestas con IA, "
                "configura OPENROUTER_API_KEY en web/back/.env o exportala en el entorno del backend."
            ),
            "reasoning_details": None,
            "model": "sin-configuracion-openrouter",
        }

    model = os.getenv("OPENROUTER_MODEL", OPENROUTER_MODEL)
    request_body = {
        "model": model,
        "messages": build_assistant_messages(payload),
        "extra_body": {
            "reasoning": {
                "enabled": True,
            }
        },
    }

    http_request = urllib_request.Request(
        OPENROUTER_API_URL,
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": "http://localhost:5173",
            "X-Title": "BI Decision Copilot Backend",
        },
        method="POST",
    )

    try:
        with urllib_request.urlopen(http_request, timeout=45) as response:
            payload_json = json.loads(response.read().decode("utf-8"))

        choice = (payload_json.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        content = str(message.get("content") or "").strip()

        if not content:
            raise HTTPException(
                status_code=502,
                detail="OpenRouter no devolvio contenido para el asistente",
            )

        return {
            "content": content,
            "reasoning_details": message.get("reasoning_details"),
            "model": str(payload_json.get("model") or model),
        }
    except urllib_error.HTTPError as err:
        detail_body = err.read().decode("utf-8", errors="ignore")
        if err.code == 429:
            rate_message = "OpenRouter esta temporalmente limitado (429). Reintenta en unos segundos o cambia a Gemini."
            try:
                parsed = json.loads(detail_body)
                nested_message = (
                    parsed.get("error", {})
                    .get("metadata", {})
                    .get("raw")
                )
                if nested_message:
                    rate_message = str(nested_message)
            except Exception:
                pass

            return {
                "content": rate_message,
                "reasoning_details": None,
                "model": "rate-limited-openrouter",
            }

        raise HTTPException(
            status_code=502,
            detail=f"OpenRouter HTTP {err.code}: {detail_body[:400]}",
        )
    except urllib_error.URLError as err:
        raise HTTPException(
            status_code=502,
            detail=f"No se pudo conectar con OpenRouter: {err.reason}",
        )


def call_gemini_assistant(payload: DecisionAssistantRequest) -> Dict[str, Any]:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {
            "content": (
                "Gemini no esta configurado. Define GEMINI_API_KEY en web/back/.env "
                "o vuelve al modo Open Model."
            ),
            "reasoning_details": None,
            "model": "sin-configuracion-gemini",
        }

    model = os.getenv("GEMINI_MODEL", GEMINI_MODEL)
    request_body = {
        "contents": [
            {
                "parts": [
                    {
                        "text": build_gemini_prompt(payload),
                    }
                ]
            }
        ]
    }

    http_request = urllib_request.Request(
        f"{GEMINI_API_URL_BASE}/{model}:generateContent",
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-goog-api-key": api_key,
        },
        method="POST",
    )

    try:
        with urllib_request.urlopen(http_request, timeout=45) as response:
            payload_json = json.loads(response.read().decode("utf-8"))

        candidates = payload_json.get("candidates") or []
        first_candidate = candidates[0] if candidates else {}
        parts = first_candidate.get("content", {}).get("parts") or []
        content = "\n".join(
            str(part.get("text", "")).strip() for part in parts if part.get("text")
        ).strip()

        if not content:
            raise HTTPException(
                status_code=502,
                detail="Gemini no devolvio contenido para el asistente",
            )

        returned_model = payload_json.get("modelVersion") or model
        return {
            "content": content,
            "reasoning_details": None,
            "model": str(returned_model),
        }
    except urllib_error.HTTPError as err:
        detail_body = err.read().decode("utf-8", errors="ignore")
        if err.code == 429:
            return {
                "content": "Gemini esta temporalmente limitado (429). Reintenta en unos segundos o cambia a Open Model.",
                "reasoning_details": None,
                "model": "rate-limited-gemini",
            }

        raise HTTPException(
            status_code=502,
            detail=f"Gemini HTTP {err.code}: {detail_body[:400]}",
        )
    except urllib_error.URLError as err:
        raise HTTPException(
            status_code=502,
            detail=f"No se pudo conectar con Gemini: {err.reason}",
        )


@app.get("/")
def root():
    return {"ok": True, "message": "API online"}


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/dashboard-data")
def dashboard_data(
    limit: int = Query(default=200, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    route: str | None = Query(default=None),
    status: str | None = Query(default=None),
):
    """
    Datos tabulares paginados (evita congelar Swagger/docs).
    """
    try:
        engine = get_engine()
        where_clause, filter_params = build_dashboard_filters(
            start_date, end_date, route, status
        )
        sql = text(
            f"""
            SELECT *
            FROM dbo.Vista_Gold_Dashboard
            {where_clause}
            ORDER BY EntregaKey
            OFFSET :offset ROWS
            FETCH NEXT :limit ROWS ONLY
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(
                sql,
                conn,
                params={"offset": offset, "limit": limit, **filter_params},
            )
        return {
            "limit": limit,
            "offset": offset,
            "count": len(df),
            "items": rows_to_json(df),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/kpis")
def kpis(
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    route: str | None = Query(default=None),
    status: str | None = Query(default=None),
):
    """
    KPIs principales del dashboard desde Capa Gold.
    """
    try:
        engine = get_engine()
        where_clause, filter_params = build_dashboard_filters(
            start_date, end_date, route, status, table_alias="vg"
        )
        sql = text(
            f"""
            SELECT
                COUNT(*) AS Total_Entregas,
                CAST(AVG(CAST(vg.Entregado_A_Tiempo AS FLOAT))*100 AS DECIMAL(6,2)) AS OTIF_Pct,
                CAST(AVG(vg.Utilizacion_Capacidad_Pct) AS DECIMAL(6,2)) AS Utilizacion_Pct,
                CAST(AVG(vg.Costo_Combustible_Bs) AS DECIMAL(10,2)) AS Costo_Por_Entrega_Bs,
                CAST(
                    SUM(vg.Costo_Combustible_Bs) / NULLIF(SUM(vg.Distancia_KM), 0)
                    AS DECIMAL(10,4)
                ) AS Costo_Por_KM_Bs,
                CAST(AVG(vg.Eficiencia_L_por_KM) AS DECIMAL(8,4)) AS Consumo_L_KM,
                CAST(SUM(vg.Emisiones_CO2_KG) AS DECIMAL(14,2)) AS CO2_Total_KG,
                CAST(AVG(vg.Emisiones_CO2_KG) AS DECIMAL(10,3)) AS CO2_Por_Entrega_KG,
                CAST(
                    AVG(
                        CASE
                            WHEN e.HoraSalida IS NOT NULL AND e.HoraLlegada IS NOT NULL THEN
                                CASE
                                    WHEN DATEDIFF(MINUTE, e.HoraSalida, e.HoraLlegada) >= 0
                                        THEN DATEDIFF(MINUTE, e.HoraSalida, e.HoraLlegada)
                                    ELSE DATEDIFF(MINUTE, e.HoraSalida, e.HoraLlegada) + 1440
                                END
                            ELSE NULL
                        END
                    ) AS DECIMAL(10,2)
                ) AS Tiempo_Ciclo_Promedio_Min
            FROM dbo.Vista_Gold_Dashboard vg
            LEFT JOIN dbo.Entrega e
                ON e.EntregaID = vg.ID_Entrega_Origen
            {where_clause}
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=filter_params)
        return rows_to_json(df)[0] if len(df) else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/co2-by-route")
def co2_by_route(
    top_n: int = Query(default=10, ge=1, le=50),
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    route: str | None = Query(default=None),
    status: str | None = Query(default=None),
):
    """
    Top rutas más contaminantes.
    """
    try:
        engine = get_engine()
        where_clause, filter_params = build_dashboard_filters(
            start_date, end_date, route, status
        )
        sql = text(
            f"""
            SELECT TOP (:top_n)
                Ruta,
                COUNT(*) AS Viajes,
                CAST(SUM(Emisiones_CO2_KG) AS DECIMAL(14,2)) AS CO2_Total_KG
            FROM dbo.Vista_Gold_Dashboard
            {where_clause}
            GROUP BY Ruta
            ORDER BY CO2_Total_KG DESC
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"top_n": top_n, **filter_params})
        return rows_to_json(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/monthly-trend")
def monthly_trend(
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    route: str | None = Query(default=None),
    status: str | None = Query(default=None),
):
    """
    Tendencia mensual de entregas y emisiones.
    """
    try:
        engine = get_engine()
        where_clause, filter_params = build_dashboard_filters(
            start_date, end_date, route, status
        )
        sql = text(
            f"""
            SELECT
                CONCAT(Anio, '-', RIGHT(CONCAT('0', Mes), 2)) AS Periodo,
                COUNT(*) AS Entregas,
                CAST(SUM(Emisiones_CO2_KG) AS DECIMAL(14,2)) AS Emisiones_CO2_KG
            FROM dbo.Vista_Gold_Dashboard
            {where_clause}
            GROUP BY Anio, Mes
            ORDER BY Periodo
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=filter_params)
        return rows_to_json(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cepal-emissions")
def cepal_emissions(
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    """
    Benchmark anual de emisiones Bolivia (FactEmisionesPais).
    """
    try:
        engine = get_engine()
        where_conditions = [
            "TRY_CONVERT(INT, Anio) IS NOT NULL",
            "TRY_CONVERT(DECIMAL(18,4), Emisiones_MtCO2e) IS NOT NULL",
            "Pais = 'Bolivia'",
        ]
        params: Dict[str, Any] = {}

        if start_date:
            where_conditions.append("TRY_CONVERT(INT, Anio) >= :start_year")
            params["start_year"] = int(start_date[:4])

        if end_date:
            where_conditions.append("TRY_CONVERT(INT, Anio) <= :end_year")
            params["end_year"] = int(end_date[:4])

        sql = text(
            f"""
            SELECT
                TRY_CONVERT(INT, Anio) AS Anio,
                SUM(TRY_CONVERT(DECIMAL(18,4), Emisiones_MtCO2e)) AS Emisiones_MtCO2e
            FROM dbo.FactEmisionesPais
            WHERE {' AND '.join(where_conditions)}
            GROUP BY TRY_CONVERT(INT, Anio)
            ORDER BY TRY_CONVERT(INT, Anio)
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
        return rows_to_json(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/filter-options")
def filter_options(
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    """
    Opciones para filtros de frontend: rutas, estados y rango disponible.
    """
    try:
        engine = get_engine()
        clauses: List[str] = []
        filter_params: Dict[str, Any] = {}
        if start_date:
            clauses.append("Fecha >= :start_date")
            filter_params["start_date"] = start_date
        if end_date:
            clauses.append("Fecha <= :end_date")
            filter_params["end_date"] = end_date

        routes_where = " AND ".join([*clauses, "Ruta IS NOT NULL"])
        status_where = " AND ".join([*clauses, "EstadoEntrega IS NOT NULL"])

        routes_sql = text(
            f"""
            SELECT DISTINCT Ruta
            FROM dbo.Vista_Gold_Dashboard
            WHERE {routes_where}
            ORDER BY Ruta
            """
        )

        status_sql = text(
            f"""
            SELECT DISTINCT EstadoEntrega
            FROM dbo.Vista_Gold_Dashboard
            WHERE {status_where}
            ORDER BY EstadoEntrega
            """
        )

        date_sql = text(
            """
            SELECT
                MIN(Fecha) AS MinFecha,
                MAX(Fecha) AS MaxFecha
            FROM dbo.Vista_Gold_Dashboard
            """
        )

        with engine.connect() as conn:
            routes_df = pd.read_sql(routes_sql, conn, params=filter_params)
            status_df = pd.read_sql(status_sql, conn, params=filter_params)
            date_df = pd.read_sql(date_sql, conn)

        min_date = None
        max_date = None
        if len(date_df):
            min_value = date_df.iloc[0].get("MinFecha")
            max_value = date_df.iloc[0].get("MaxFecha")
            if pd.notnull(min_value):
                min_date = pd.to_datetime(min_value).strftime("%Y-%m-%d")
            if pd.notnull(max_value):
                max_date = pd.to_datetime(max_value).strftime("%Y-%m-%d")

        return {
            "routes": [str(row["Ruta"]) for _, row in routes_df.iterrows()],
            "statuses": [
                str(row["EstadoEntrega"])
                for _, row in status_df.iterrows()
                if str(row["EstadoEntrega"]).strip().lower() != "pendiente"
            ],
            "min_date": min_date,
            "max_date": max_date,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/decision-assistant", response_model=DecisionAssistantResponse)
def decision_assistant(payload: DecisionAssistantRequest):
    """
    Proxy backend para OpenRouter/Gemini. Evita exponer API keys en frontend.
    """
    try:
        if payload.provider == "gemini":
            return call_gemini_assistant(payload)

        return call_openrouter_assistant(payload)
    except HTTPException:
        raise
    except Exception as err:
        raise HTTPException(
            status_code=500,
            detail=f"Error interno del asistente: {err}",
        )

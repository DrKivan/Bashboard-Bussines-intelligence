import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyodbc
import streamlit as st


st.set_page_config(
    page_title="Dashboard Logistica Ultima Milla",
    page_icon="",
    layout="wide",
)


SQL_QUERY_BASE = """
SELECT
    fe.EntregaKey,
    fe.ID_Entrega_Origen,
    f.Fecha,
    f.Anio,
    f.Mes,
    f.NombreMes,
    ee.EstadoEntrega,
    fe.Distancia_KM,
    fe.Consumo_Combustible_Litros,
    fe.Peso_Carga_KG,
    fe.Tiempo_Idling_Minutos,
    fe.Emisiones_CO2_KG,
    dv.Modelo AS ModeloVehiculo,
    dv.Tipo AS TipoVehiculo,
    dc.Nombre AS Conductor,
    dcl.Nombre_Empresa AS Cliente,
    (uo.Departamento + ' - ' + uo.Ciudad) AS Origen,
    (ud.Departamento + ' - ' + ud.Ciudad) AS Destino
FROM dbo.FactEntregas fe
INNER JOIN dbo.DimFecha f ON f.FechaKey = fe.FechaKey
INNER JOIN dbo.DimEstadoEntrega ee ON ee.EstadoEntregaKey = fe.EstadoEntregaKey
INNER JOIN dbo.DimVehiculo dv ON dv.VehiculoKey = fe.VehiculoKey
INNER JOIN dbo.DimConductor dc ON dc.ConductorKey = fe.ConductorKey
INNER JOIN dbo.DimCliente dcl ON dcl.ClienteKey = fe.ClienteKey
INNER JOIN dbo.DimUbicacion uo ON uo.UbicacionKey = fe.UbicacionOrigenKey
INNER JOIN dbo.DimUbicacion ud ON ud.UbicacionKey = fe.UbicacionDestinoKey;
"""

SQL_QUERY_EMISIONES_PAIS = """
SELECT
        TRY_CONVERT(INT, Anio) AS Anio,
        SUM(TRY_CONVERT(DECIMAL(18,4), Emisiones_MtCO2e)) AS Emisiones_MtCO2e
FROM dbo.FactEmisionesPais
WHERE TRY_CONVERT(INT, Anio) IS NOT NULL
    AND TRY_CONVERT(DECIMAL(18,4), Emisiones_MtCO2e) IS NOT NULL
GROUP BY TRY_CONVERT(INT, Anio)
ORDER BY TRY_CONVERT(INT, Anio);
"""


def build_connection_string(server: str, database: str) -> str:
    driver_candidates = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    for driver in driver_candidates:
        try:
            test_conn = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                "TRUSTED_CONNECTION=yes;"
                "TrustServerCertificate=yes;"
            )
            with pyodbc.connect(test_conn, timeout=3):
                pass
            return test_conn
        except pyodbc.Error:
            continue
    raise ValueError("No se encontro un driver ODBC compatible para SQL Server")


@st.cache_data(ttl=600, show_spinner=False)
def load_fact_table(server: str, database: str) -> pd.DataFrame:
    conn_str = build_connection_string(server, database)
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(SQL_QUERY_BASE, conn)

    df["Fecha"] = pd.to_datetime(df["Fecha"])
    return df


@st.cache_data(ttl=600, show_spinner=False)
def load_emisiones_pais(server: str, database: str) -> pd.DataFrame:
    conn_str = build_connection_string(server, database)
    with pyodbc.connect(conn_str) as conn:
        return pd.read_sql(SQL_QUERY_EMISIONES_PAIS, conn)


def kpi_card(label: str, value: str, help_text: str) -> None:
    st.metric(label=label, value=value, help=help_text)


def story_card(title: str, body: str) -> None:
    st.markdown(
        f"""
        <div style=\"border:1px solid #d9d9d9; border-radius:10px; padding:12px; background:#fafafa;\">
            <h4 style=\"margin:0 0 6px 0;\">{title}</h4>
            <p style=\"margin:0; color:#333;\">{body}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.title("Dashboard de Inteligencia de Negocios")
st.caption("Proyecto final | Cadena de valor: Capa Gold -> Capa de Visualizacion")

with st.sidebar:
    st.header("Conexion SQL Server")
    server_name = st.text_input("Servidor", value="DESKTOP-I8TQRVC")
    database_name = st.text_input("Base de datos", value="LogisticaUltimaMilla")

    connect_button = st.button("Conectar y actualizar", width="stretch")

if connect_button:
    st.cache_data.clear()


try:
    df_base = load_fact_table(server_name, database_name)
except Exception as ex:
    st.error("No fue posible conectar o consultar el modelo estrella.")
    st.exception(ex)
    st.stop()


min_date = df_base["Fecha"].min().date()
max_date = df_base["Fecha"].max().date()

with st.sidebar:
    st.header("Filtros globales")

    years_available = sorted(df_base["Anio"].dropna().unique().tolist())
    estados_available = sorted(df_base["EstadoEntrega"].dropna().unique().tolist())
    tipos_available = sorted(df_base["TipoVehiculo"].dropna().unique().tolist())

    selected_years = st.multiselect("Anio", options=years_available, default=years_available)
    selected_estados = st.multiselect("Estado de entrega", options=estados_available, default=estados_available)
    selected_tipos = st.multiselect("Tipo de vehiculo", options=tipos_available, default=tipos_available)
    selected_dates = st.date_input("Rango de fechas", value=(min_date, max_date), min_value=min_date, max_value=max_date)

if len(selected_dates) != 2:
    st.warning("Selecciona un rango de fechas valido para continuar.")
    st.stop()

start_date, end_date = selected_dates

filtro = (
    df_base["Anio"].isin(selected_years)
    & df_base["EstadoEntrega"].isin(selected_estados)
    & df_base["TipoVehiculo"].isin(selected_tipos)
    & (df_base["Fecha"].dt.date >= start_date)
    & (df_base["Fecha"].dt.date <= end_date)
)


_df = df_base.loc[filtro].copy()

if _df.empty:
    st.warning("No hay datos para los filtros seleccionados.")
    st.stop()


# Acto 1: Panorama operacional
st.subheader("Acto 1: Como estamos operando?")

entregas_total = len(_df)
pct_entregado = (_df["EstadoEntrega"].eq("Entregado").mean() * 100.0) if entregas_total > 0 else 0.0
emisiones_total = _df["Emisiones_CO2_KG"].sum()
consumo_promedio = (_df["Consumo_Combustible_Litros"].sum() / _df["Distancia_KM"].sum()) if _df["Distancia_KM"].sum() > 0 else 0

k1, k2, k3, k4 = st.columns(4)
with k1:
    kpi_card("Entregas", f"{entregas_total:,.0f}", "Cantidad de entregas en el periodo filtrado")
with k2:
    kpi_card("Tasa entregado", f"{pct_entregado:,.1f}%", "Proporcion de entregas exitosas")
with k3:
    kpi_card("CO2 total", f"{emisiones_total:,.2f} kg", "Emisiones acumuladas de la operacion")
with k4:
    kpi_card("Consumo por km", f"{consumo_promedio:,.3f} L/km", "Eficiencia energetica promedio")


col_story_1, col_story_2 = st.columns(2)
with col_story_1:
    story_card(
        "Lectura rapida",
        f"Se registran {entregas_total:,.0f} entregas en el periodo filtrado con una tasa de cumplimiento de {pct_entregado:,.1f}%.",
    )
with col_story_2:
    story_card(
        "Mensaje clave",
        f"La operacion acumula {emisiones_total:,.0f} kg de CO2. Mejorar consumo por km impacta costo y sostenibilidad al mismo tiempo.",
    )


st.markdown("### Distribucion de estados de entrega")
estado_df = (
    _df.groupby("EstadoEntrega", as_index=False)
    .agg(Total=("EntregaKey", "count"))
    .sort_values("Total", ascending=False)
)
fig_estado = px.pie(
    estado_df,
    names="EstadoEntrega",
    values="Total",
    hole=0.45,
)
fig_estado.update_traces(textposition="inside", textinfo="percent+label")
fig_estado.update_layout(margin=dict(l=20, r=20, t=20, b=20), legend_title_text="")
st.plotly_chart(fig_estado, width="stretch")


st.markdown("### Tendencia mensual de entregas y emisiones")
df_month = (
    _df.assign(Periodo=_df["Fecha"].dt.to_period("M").astype(str))
    .groupby("Periodo", as_index=False)
    .agg(
        Entregas=("EntregaKey", "count"),
        Emisiones_CO2_KG=("Emisiones_CO2_KG", "sum"),
    )
)

fig_tend = go.Figure()
fig_tend.add_trace(go.Bar(x=df_month["Periodo"], y=df_month["Entregas"], name="Entregas"))
fig_tend.add_trace(
    go.Scatter(
        x=df_month["Periodo"],
        y=df_month["Emisiones_CO2_KG"],
        name="Emisiones CO2 (kg)",
        mode="lines+markers",
        yaxis="y2",
    )
)
fig_tend.update_layout(
    xaxis_title="Periodo",
    yaxis_title="Entregas",
    yaxis2=dict(title="Emisiones CO2 (kg)", overlaying="y", side="right"),
    legend=dict(orientation="h"),
    margin=dict(l=20, r=20, t=20, b=20),
)
st.plotly_chart(fig_tend, width="stretch")


# Acto 2: Donde se gana o se pierde eficiencia
st.subheader("Acto 2: Deteccion de ineficiencias")

c1, c2 = st.columns(2)

with c1:
    st.markdown("### Rutas mas frecuentes")
    rutas = (
        _df.assign(Ruta=_df["Origen"] + " -> " + _df["Destino"])
        .groupby("Ruta", as_index=False)
        .agg(TotalViajes=("EntregaKey", "count"))
        .sort_values("TotalViajes", ascending=False)
        .head(10)
    )
    fig_rutas = px.bar(rutas.sort_values("TotalViajes"), x="TotalViajes", y="Ruta", orientation="h")
    fig_rutas.update_layout(margin=dict(l=20, r=20, t=20, b=20), yaxis_title="")
    st.plotly_chart(fig_rutas, width="stretch")

with c2:
    st.markdown("### Vehiculos con mayor emision")
    top_vehiculos = (
        _df.groupby(["ModeloVehiculo", "TipoVehiculo"], as_index=False)
        .agg(TotalEmisiones=("Emisiones_CO2_KG", "sum"))
        .sort_values("TotalEmisiones", ascending=False)
        .head(10)
    )
    top_vehiculos["Vehiculo"] = top_vehiculos["ModeloVehiculo"] + " (" + top_vehiculos["TipoVehiculo"] + ")"
    fig_veh = px.bar(top_vehiculos.sort_values("TotalEmisiones"), x="TotalEmisiones", y="Vehiculo", orientation="h")
    fig_veh.update_layout(margin=dict(l=20, r=20, t=20, b=20), yaxis_title="")
    st.plotly_chart(fig_veh, width="stretch")

st.markdown("### Relacion distancia vs emisiones")
scatter_df = _df.copy()
scatter_df["TamCarga"] = scatter_df["Peso_Carga_KG"].clip(lower=1)
fig_scatter = px.scatter(
    scatter_df,
    x="Distancia_KM",
    y="Emisiones_CO2_KG",
    size="TamCarga",
    color="EstadoEntrega",
    hover_data=["TipoVehiculo", "Origen", "Destino"],
    size_max=18,
    opacity=0.6,
)
fig_scatter.update_layout(margin=dict(l=20, r=20, t=20, b=20), xaxis_title="Distancia (km)", yaxis_title="CO2 (kg)")
st.plotly_chart(fig_scatter, width="stretch")

st.markdown("### Tiempo de idling por destino")
idling_ciudad = (
    _df.groupby("Destino", as_index=False)
    .agg(TiempoIdlingPromedio=("Tiempo_Idling_Minutos", "mean"))
    .sort_values("TiempoIdlingPromedio", ascending=False)
    .head(10)
)
fig_idle = px.bar(
    idling_ciudad.sort_values("TiempoIdlingPromedio"),
    x="TiempoIdlingPromedio",
    y="Destino",
    orientation="h",
)
fig_idle.update_layout(margin=dict(l=20, r=20, t=20, b=20), yaxis_title="")
st.plotly_chart(fig_idle, width="stretch")


# Acto 3: Impacto ambiental
st.subheader("Acto 3: Impacto ambiental y contexto pais")

logistica_anual = (
    _df.groupby("Anio", as_index=False)
    .agg(Emisiones_Logistica_KG=("Emisiones_CO2_KG", "sum"))
    .sort_values("Anio")
)

try:
    emisiones_pais = load_emisiones_pais(server_name, database_name)
    emisiones_pais["Anio"] = pd.to_numeric(emisiones_pais["Anio"], errors="coerce")
    emisiones_pais = emisiones_pais.dropna(subset=["Anio"]).copy()
    emisiones_pais["Anio"] = emisiones_pais["Anio"].astype(int)

    comparativo = logistica_anual.merge(emisiones_pais, on="Anio", how="left")

    fig_comp = go.Figure()
    fig_comp.add_trace(
        go.Bar(
            x=comparativo["Anio"],
            y=comparativo["Emisiones_Logistica_KG"],
            name="Logistica (kg CO2)",
        )
    )
    fig_comp.add_trace(
        go.Scatter(
            x=comparativo["Anio"],
            y=comparativo["Emisiones_MtCO2e"],
            name="Pais (MtCO2e)",
            mode="lines+markers",
            yaxis="y2",
        )
    )
    fig_comp.update_layout(
        xaxis_title="Anio",
        yaxis_title="Logistica (kg CO2)",
        yaxis2=dict(title="Pais (MtCO2e)", overlaying="y", side="right"),
        legend=dict(orientation="h"),
        margin=dict(l=20, r=20, t=20, b=20),
    )
    st.plotly_chart(fig_comp, width="stretch")
except Exception:
    st.info(
        "No fue posible cargar FactEmisionesPais. Puedes crear/llenar esa tabla para habilitar el comparativo con CEPAL."
    )


st.markdown("### Recomendaciones accionables")

ruta_critica = rutas.iloc[0]["Ruta"] if not rutas.empty else "Sin dato"
vehiculo_critico = top_vehiculos.iloc[0]["Vehiculo"] if not top_vehiculos.empty else "Sin dato"
ciudad_idling = idling_ciudad.iloc[0]["Destino"] if not idling_ciudad.empty else "Sin dato"

st.write(f"1. Priorizar optimizacion de la ruta con mayor demanda: {ruta_critica}.")
st.write(f"2. Revisar mantenimiento y uso del vehiculo con mas emisiones: {vehiculo_critico}.")
st.write(f"3. Implementar plan de reduccion de idling en destino: {ciudad_idling}.")
st.write("4. Monitorear mensualmente el indicador L/km para medir mejoras de eficiencia.")


with st.expander("Ver muestra de datos filtrados"):
    st.dataframe(_df.head(100), width="stretch")

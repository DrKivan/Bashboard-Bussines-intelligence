import { type FormEvent, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  Bot,
  Clock3,
  Gauge,
  Leaf,
  Loader2,
  MessageSquare,
  Route,
  SendHorizontal,
  Sparkles,
  Truck,
  X,
} from "lucide-react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  XAxis,
  YAxis,
} from "recharts"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"

type KpiData = {
  Total_Entregas?: number | string | null
  OTIF_Pct?: number | string | null
  Utilizacion_Pct?: number | string | null
  Costo_Por_Entrega_Bs?: number | string | null
  Costo_Por_KM_Bs?: number | string | null
  Consumo_L_KM?: number | string | null
  CO2_Total_KG?: number | string | null
  CO2_Por_Entrega_KG?: number | string | null
  Tiempo_Ciclo_Promedio_Min?: number | string | null
}

type Co2ByRoute = {
  Ruta?: string | null
  Viajes?: number | string | null
  CO2_Total_KG?: number | string | null
}

type MonthlyTrend = {
  Periodo?: string | null
  Entregas?: number | string | null
  Emisiones_CO2_KG?: number | string | null
}

type CepalEmission = {
  Anio?: number | string | null
  Emisiones_MtCO2e?: number | string | null
}

type FilterOptions = {
  routes: string[]
  statuses: string[]
  min_date: string | null
  max_date: string | null
}

type DashboardFilters = {
  startDate: string
  endDate: string
  route: string
  status: string
}

type AssistantMessage = {
  role: "user" | "assistant"
  content: string
  reasoningDetails?: unknown
}

type AssistantProvider = "openrouter" | "gemini"

type DecisionAssistantApiResponse = {
  content: string
  reasoning_details?: unknown
  model?: string
}

const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "") ?? "http://localhost:8000"
const DECISION_ASSISTANT_ENDPOINT = "/api/decision-assistant"

const DEFAULT_FILTERS: DashboardFilters = {
  startDate: "",
  endDate: "",
  route: "",
  status: "",
}

const EMPTY_FILTER_OPTIONS: FilterOptions = {
  routes: [],
  statuses: [],
  min_date: null,
  max_date: null,
}

const HEALTH_THRESHOLDS = {
  service: {
    healthyMin: 95,
    watchMin: 90,
  },
  capacity: {
    healthyMin: 75,
    healthyMax: 92,
    watchMin: 68,
    watchMax: 95,
  },
  environment: {
    healthyMax: 2,
    watchMax: 4,
  },
} as const

const monthlyChartConfig = {
  entregas: {
    label: "Entregas",
    color: "var(--chart-1)",
  },
  co2: {
    label: "CO2 kg",
    color: "var(--chart-2)",
  },
} satisfies ChartConfig

const routeChartConfig = {
  co2: {
    label: "CO2 por ruta",
    color: "var(--chart-3)",
  },
} satisfies ChartConfig

const benchmarkChartConfig = {
  interno: {
    label: "Interno (indice)",
    color: "var(--chart-2)",
  },
  cepal: {
    label: "CEPAL (indice)",
    color: "var(--chart-4)",
  },
} satisfies ChartConfig

const toNumber = (value: unknown): number => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0
  }

  if (typeof value === "string") {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : 0
  }

  return 0
}

const formatPeriod = (period: string): string => {
  const [year, month] = period.split("-")
  if (!year || !month) {
    return period
  }

  return `${month}/${year.slice(2)}`
}

const formatCycleTime = (minutesValue: unknown): string => {
  const minutes = toNumber(minutesValue)
  if (minutes <= 0) {
    return "Sin dato"
  }

  if (minutes >= 60) {
    return `${(minutes / 60).toFixed(1)} h`
  }

  return `${minutes.toFixed(0)} min`
}

const extractYear = (value: string): string | null => {
  const match = value.match(/^\d{4}/)
  return match ? match[0] : null
}

const fetchJson = async <T,>(
  endpoint: string,
  signal: AbortSignal,
): Promise<T> => {
  const response = await fetch(`${API_BASE_URL}${endpoint}`, { signal })

  if (!response.ok) {
    throw new Error(`Error ${response.status} al consultar ${endpoint}`)
  }

  return response.json() as Promise<T>
}

const buildQuery = (paramsObject: Record<string, string | undefined>): string => {
  const searchParams = new URLSearchParams()

  Object.entries(paramsObject).forEach(([key, value]) => {
    if (value) {
      searchParams.set(key, value)
    }
  })

  const query = searchParams.toString()
  return query ? `?${query}` : ""
}

function App() {
  const [kpis, setKpis] = useState<KpiData | null>(null)
  const [co2ByRoute, setCo2ByRoute] = useState<Co2ByRoute[]>([])
  const [monthlyTrend, setMonthlyTrend] = useState<MonthlyTrend[]>([])
  const [cepalEmissions, setCepalEmissions] = useState<CepalEmission[]>([])
  const [filterOptions, setFilterOptions] = useState<FilterOptions>(
    EMPTY_FILTER_OPTIONS,
  )
  const [filters, setFilters] = useState<DashboardFilters>(DEFAULT_FILTERS)
  const [loading, setLoading] = useState(true)
  const [loadingOptions, setLoadingOptions] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [assistantMessages, setAssistantMessages] = useState<AssistantMessage[]>([
    {
      role: "assistant",
      content:
        "Soy tu asistente de decisiones. Puedo interpretar KPIs, detectar riesgos y proponer acciones concretas con base en los filtros activos.",
    },
  ])
  const [assistantInput, setAssistantInput] = useState("")
  const [assistantLoading, setAssistantLoading] = useState(false)
  const [assistantError, setAssistantError] = useState<string | null>(null)
  const [assistantModel, setAssistantModel] = useState("configurado en backend")
  const [assistantOpen, setAssistantOpen] = useState(false)
  const [assistantProvider, setAssistantProvider] =
    useState<AssistantProvider>("openrouter")

  useEffect(() => {
    const controller = new AbortController()

    const query = buildQuery({
      start_date: filters.startDate || undefined,
      end_date: filters.endDate || undefined,
    })

    const loadFilterOptions = async () => {
      setLoadingOptions(true)
      try {
        const optionsPayload = await fetchJson<FilterOptions>(
          `/api/filter-options${query}`,
          controller.signal,
        )

        setFilterOptions(optionsPayload)

        setFilters((currentFilters) => {
          const shouldResetRoute =
            currentFilters.route &&
            !optionsPayload.routes.includes(currentFilters.route)
          const shouldResetStatus =
            currentFilters.status &&
            !optionsPayload.statuses.includes(currentFilters.status)

          if (!shouldResetRoute && !shouldResetStatus) {
            return currentFilters
          }

          return {
            ...currentFilters,
            route: shouldResetRoute ? "" : currentFilters.route,
            status: shouldResetStatus ? "" : currentFilters.status,
          }
        })
      } catch (requestError) {
        if (
          requestError instanceof Error &&
          requestError.name === "AbortError"
        ) {
          return
        }

        setError(
          requestError instanceof Error
            ? requestError.message
            : "No se pudieron cargar los filtros",
        )
      } finally {
        setLoadingOptions(false)
      }
    }

    void loadFilterOptions()

    return () => controller.abort()
  }, [filters.startDate, filters.endDate])

  useEffect(() => {
    const controller = new AbortController()

    const loadDashboard = async () => {
      if (
        filters.startDate &&
        filters.endDate &&
        filters.startDate > filters.endDate
      ) {
        setError("La fecha inicial no puede ser mayor que la fecha final")
        setLoading(false)
        return
      }

      setLoading(true)
      setError(null)

      const commonFilterParams = {
        start_date: filters.startDate || undefined,
        end_date: filters.endDate || undefined,
        route: filters.route || undefined,
        status: filters.status || undefined,
      }

      const sharedQuery = buildQuery(commonFilterParams)
      const co2Query = buildQuery({ top_n: "8", ...commonFilterParams })
      const cepalQuery = buildQuery({
        start_date: filters.startDate || undefined,
        end_date: filters.endDate || undefined,
      })

      try {
        const [kpiPayload, routePayload, monthlyPayload, cepalPayload] =
          await Promise.all([
            fetchJson<KpiData>(`/api/kpis${sharedQuery}`, controller.signal),
            fetchJson<Co2ByRoute[]>(
              `/api/co2-by-route${co2Query}`,
              controller.signal,
            ),
            fetchJson<MonthlyTrend[]>(
              `/api/monthly-trend${sharedQuery}`,
              controller.signal,
            ),
            fetchJson<CepalEmission[]>(
              `/api/cepal-emissions${cepalQuery}`,
              controller.signal,
            ),
          ])

        setKpis(kpiPayload)
        setCo2ByRoute(routePayload)
        setMonthlyTrend(monthlyPayload)
        setCepalEmissions(cepalPayload)
      } catch (requestError) {
        if (
          requestError instanceof Error &&
          requestError.name === "AbortError"
        ) {
          return
        }

        setError(
          requestError instanceof Error
            ? requestError.message
            : "No se pudo cargar el dashboard",
        )
      } finally {
        setLoading(false)
      }
    }

    void loadDashboard()

    return () => controller.abort()
  }, [filters.endDate, filters.route, filters.startDate, filters.status])

  const formattedKpis = useMemo(
    () => {
      const topRoute = co2ByRoute[0]
      const topRouteLabel = topRoute?.Ruta ? String(topRoute.Ruta) : "Sin ruta"
      const topRouteCo2 = toNumber(topRoute?.CO2_Total_KG)

      return [
        {
          id: "costo-transporte",
          title: "Costo de Transporte (Unidad / KM)",
          value: `Bs ${toNumber(kpis?.Costo_Por_Entrega_Bs).toLocaleString("es-BO", {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })} / entrega`,
          hint: `Bs ${toNumber(kpis?.Costo_Por_KM_Bs).toLocaleString("es-BO", {
            minimumFractionDigits: 3,
            maximumFractionDigits: 3,
          })} por km`,
          definition:
            "Mide eficiencia economica e identifica rutas o clientes con costo elevado.",
          icon: Route,
        },
        {
          id: "otif",
          title: "Tasa de Entrega a Tiempo (OTIF)",
          value: `${toNumber(kpis?.OTIF_Pct).toFixed(2)}%`,
          hint: "entregas a tiempo sobre entregas totales",
          definition:
            "Indicador central de calidad de servicio y de planificacion operativa.",
          icon: Truck,
        },
        {
          id: "utilizacion-carga",
          title: "Utilizacion de Capacidad de Carga",
          value: `${toNumber(kpis?.Utilizacion_Pct).toFixed(2)}%`,
          hint: "uso real sobre capacidad disponible",
          definition:
            "Baja utilizacion indica desperdicio; alta utilizacion mejora productividad.",
          icon: Gauge,
        },
        {
          id: "consumo-combustible",
          title: "Consumo de Combustible Promedio",
          value: `${toNumber(kpis?.Consumo_L_KM).toFixed(3)} L/KM`,
          hint: "litros consumidos por kilometro recorrido",
          definition:
            "Permite comparar rendimiento entre vehiculos y apoyar reduccion de costos.",
          icon: Leaf,
        },
        {
          id: "co2-ruta-entrega",
          title: "Emisiones de CO2 por Ruta / Entrega",
          value: `${toNumber(kpis?.CO2_Por_Entrega_KG).toLocaleString("es-BO", {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })} kg por entrega`,
          hint: `${topRouteLabel}: ${topRouteCo2.toLocaleString("es-BO", {
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
          })} kg acumulado`,
          definition:
            "KPI clave de sostenibilidad y cumplimiento ambiental en la operacion.",
          icon: AlertTriangle,
        },
        {
          id: "tiempo-ciclo",
          title: "Tiempo de Ciclo de Entrega",
          value: formatCycleTime(kpis?.Tiempo_Ciclo_Promedio_Min),
          hint: "promedio desde salida hasta entrega final",
          definition:
            "Ayuda a detectar cuellos de botella y mejorar velocidad de ultima milla.",
          icon: Clock3,
        },
      ]
    },
    [co2ByRoute, kpis],
  )

  const monthlyChartData = useMemo(
    () =>
      monthlyTrend.map((row) => ({
        period: formatPeriod(String(row.Periodo ?? "")),
        entregas: toNumber(row.Entregas),
        co2: toNumber(row.Emisiones_CO2_KG),
      })),
    [monthlyTrend],
  )

  const routeChartData = useMemo(
    () =>
      co2ByRoute.map((row) => ({
        route: String(row.Ruta ?? "Sin ruta"),
        co2: toNumber(row.CO2_Total_KG),
        trips: toNumber(row.Viajes),
      })),
    [co2ByRoute],
  )

  const benchmarkChartData = useMemo(() => {
    const internalByYear = new Map<string, number>()
    for (const row of monthlyTrend) {
      const year = extractYear(String(row.Periodo ?? ""))
      if (!year) {
        continue
      }

      const currentValue = internalByYear.get(year) ?? 0
      internalByYear.set(year, currentValue + toNumber(row.Emisiones_CO2_KG))
    }

    const cepalByYear = new Map<string, number>()
    for (const row of cepalEmissions) {
      const year = extractYear(String(row.Anio ?? ""))
      if (!year) {
        continue
      }

      cepalByYear.set(year, toNumber(row.Emisiones_MtCO2e))
    }

    const years = Array.from(
      new Set([...internalByYear.keys(), ...cepalByYear.keys()]),
    ).sort()

    if (!years.length) {
      return []
    }

    const internalBaseYear = years.find((year) => (internalByYear.get(year) ?? 0) > 0)
    const cepalBaseYear = years.find((year) => (cepalByYear.get(year) ?? 0) > 0)

    const internalBase = internalBaseYear
      ? internalByYear.get(internalBaseYear) ?? 0
      : 0
    const cepalBase = cepalBaseYear ? cepalByYear.get(cepalBaseYear) ?? 0 : 0

    return years.map((year) => {
      const internalValue = internalByYear.get(year)
      const cepalValue = cepalByYear.get(year)

      return {
        year,
        interno:
          internalValue !== undefined && internalBase > 0
            ? Number(((internalValue / internalBase) * 100).toFixed(2))
            : null,
        cepal:
          cepalValue !== undefined && cepalBase > 0
            ? Number(((cepalValue / cepalBase) * 100).toFixed(2))
            : null,
      }
    })
  }, [cepalEmissions, monthlyTrend])

  const hasData =
    formattedKpis.some((kpi) => kpi.value !== "0") ||
    monthlyChartData.length > 0 ||
    routeChartData.length > 0 ||
    benchmarkChartData.length > 0

  const activeFiltersCount =
    Number(Boolean(filters.startDate)) +
    Number(Boolean(filters.endDate)) +
    Number(Boolean(filters.route)) +
    Number(Boolean(filters.status))

  const dashboardAssistantContext = useMemo(
    () => ({
      filtros_activos: {
        fecha_desde: filters.startDate || null,
        fecha_hasta: filters.endDate || null,
        ruta: filters.route || null,
        estado: filters.status || null,
      },
      kpis_estrategicos: formattedKpis.map((kpi) => ({
        id: kpi.id,
        titulo: kpi.title,
        valor: kpi.value,
        detalle: kpi.hint,
      })),
      kpis_clave: {
        total_entregas: toNumber(kpis?.Total_Entregas),
        otif_pct: toNumber(kpis?.OTIF_Pct),
        utilizacion_pct: toNumber(kpis?.Utilizacion_Pct),
        costo_por_entrega_bs: toNumber(kpis?.Costo_Por_Entrega_Bs),
        costo_por_km_bs: toNumber(kpis?.Costo_Por_KM_Bs),
        consumo_l_km: toNumber(kpis?.Consumo_L_KM),
        co2_total_kg: toNumber(kpis?.CO2_Total_KG),
      },
      rutas_mayor_huella: routeChartData.slice(0, 6),
      tendencia_mensual_reciente: monthlyChartData.slice(-6),
      benchmark_indice_reciente: benchmarkChartData.slice(-6),
      umbrales_salud: HEALTH_THRESHOLDS,
    }),
    [
      benchmarkChartData,
      filters,
      formattedKpis,
      kpis,
      monthlyChartData,
      routeChartData,
    ],
  )

  const askDecisionAssistant = async (question: string) => {
    const trimmedQuestion = question.trim()
    if (!trimmedQuestion || assistantLoading) {
      return
    }

    const userMessage: AssistantMessage = {
      role: "user",
      content: trimmedQuestion,
    }
    const historySnapshot = [...assistantMessages, userMessage]

    setAssistantMessages(historySnapshot)
    setAssistantInput("")
    setAssistantError(null)
    setAssistantLoading(true)

    try {
      const response = await fetch(`${API_BASE_URL}${DECISION_ASSISTANT_ENDPOINT}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          provider: assistantProvider,
          messages: historySnapshot.map((message) => ({
            role: message.role,
            content: message.content,
            ...(message.reasoningDetails
              ? { reasoning_details: message.reasoningDetails }
              : {}),
          })),
          dashboard_context: dashboardAssistantContext,
        }),
      })

      if (!response.ok) {
        let detail = `Backend devolvio ${response.status}`
        try {
          const errorPayload = (await response.json()) as { detail?: string }
          if (errorPayload.detail) {
            detail = errorPayload.detail
          }
        } catch {
          // Si no viene JSON, mantenemos mensaje base.
        }
        throw new Error(detail)
      }

      const payload = (await response.json()) as DecisionAssistantApiResponse
      const assistantText = payload.content?.trim()

      if (!assistantText) {
        throw new Error("La IA no devolvio contenido")
      }

      setAssistantMessages((currentMessages) => [
        ...currentMessages,
        {
          role: "assistant",
          content: assistantText,
          reasoningDetails: payload.reasoning_details,
        },
      ])

      if (payload.model) {
        setAssistantModel(payload.model)
      }
    } catch (requestError) {
      setAssistantError(
        requestError instanceof Error
          ? requestError.message
          : "No se pudo consultar al asistente",
      )
    } finally {
      setAssistantLoading(false)
    }
  }

  const handleAssistantSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    void askDecisionAssistant(assistantInput)
  }

  const assistantQuickActions = [
    "Genera 3 acciones proactivas para los proximos 7 dias segun los riesgos actuales.",
    "Que ruta deberia priorizar para reducir CO2 sin afectar nivel de servicio?",
    "Explicame como mejorar OTIF y capacidad al mismo tiempo con estos datos.",
  ]

  return (
    <main
      className={`m-4 rounded-[28px] border border-emerald-900/10 bg-card/90 p-3 shadow-[0_24px_80px_-40px_rgba(0,56,28,0.45)] transition-[padding-right] duration-300 ease-out md:m-6 md:p-4 ${
        assistantOpen ? "lg:pr-[25rem]" : ""
      }`}
    >
      <div className="flex w-full flex-col gap-6">
        <section className="animate-rise flex flex-col gap-4 rounded-3xl border border-border/70 bg-card p-6 shadow-sm ring-1 ring-foreground/5 md:p-8">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <Badge variant="outline">Panel Ejecutivo</Badge>
            <Badge variant="secondary">API: {API_BASE_URL}</Badge>
          </div>
          <div className="flex flex-col gap-2">
            <h1 className="font-heading text-3xl leading-tight md:text-5xl">
              Desempeno logístico y sostenibilidad en una sola vista
            </h1>
            <p className="max-w-3xl text-muted-foreground">
              Primero revisa los 6 KPIs estrategicos. Luego valida
              tendencias por tiempo y rutas para definir acciones operativas.
            </p>
          </div>
        </section>

        <section className="grid gap-6 xl:grid-cols-[300px_minmax(0,1fr)]">
          <aside className="xl:sticky xl:top-6 xl:h-fit">
            <Card className="animate-rise border-emerald-950/70 bg-[linear-gradient(180deg,rgba(3,36,19,0.98),rgba(4,24,14,0.98))] text-emerald-50 shadow-xl">
              <CardHeader>
                <CardTitle className="text-emerald-50">Filtros del Dashboard</CardTitle>
                <CardDescription className="text-emerald-100/75">
                  Define el alcance del analisis antes de interpretar los
                  indicadores.
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-col gap-4">
                <div className="rounded-2xl border border-emerald-200/20 bg-emerald-50/10 p-3 text-xs text-emerald-100/80">
                  Paso recomendado: 1) Fecha, 2) Ruta/Estado, 3) Analiza KPIs y
                  graficos.
                </div>

                <div className="flex flex-col gap-3">
                  <div className="flex flex-col gap-1.5">
                    <label htmlFor="start-date" className="text-sm text-emerald-100/85">
                      Fecha desde
                    </label>
                    <input
                      id="start-date"
                      type="date"
                      value={filters.startDate}
                      min={filterOptions.min_date ?? undefined}
                      max={filters.endDate || filterOptions.max_date || undefined}
                      onChange={(event) =>
                        setFilters((currentFilters) => ({
                          ...currentFilters,
                          startDate: event.target.value,
                        }))
                      }
                      className="h-9 rounded-2xl border border-emerald-200/30 bg-emerald-950/45 px-3 text-sm text-emerald-50 focus-visible:ring-2 focus-visible:ring-emerald-300/60"
                      style={{ colorScheme: "dark" }}
                    />
                  </div>

                  <div className="flex flex-col gap-1.5">
                    <label htmlFor="end-date" className="text-sm text-emerald-100/85">
                      Fecha hasta
                    </label>
                    <input
                      id="end-date"
                      type="date"
                      value={filters.endDate}
                      min={filters.startDate || filterOptions.min_date || undefined}
                      max={filterOptions.max_date ?? undefined}
                      onChange={(event) =>
                        setFilters((currentFilters) => ({
                          ...currentFilters,
                          endDate: event.target.value,
                        }))
                      }
                      className="h-9 rounded-2xl border border-emerald-200/30 bg-emerald-950/45 px-3 text-sm text-emerald-50 focus-visible:ring-2 focus-visible:ring-emerald-300/60"
                      style={{ colorScheme: "dark" }}
                    />
                  </div>

                  <div className="flex flex-col gap-1.5">
                    <label htmlFor="route" className="text-sm text-emerald-100/85">
                      Ruta
                    </label>
                    <select
                      id="route"
                      value={filters.route}
                      onChange={(event) =>
                        setFilters((currentFilters) => ({
                          ...currentFilters,
                          route: event.target.value,
                        }))
                      }
                      disabled={loadingOptions}
                      className="h-9 rounded-2xl border border-emerald-200/30 bg-emerald-950/45 px-3 text-sm text-emerald-50 focus-visible:ring-2 focus-visible:ring-emerald-300/60"
                      style={{ colorScheme: "dark" }}
                    >
                      <option value="" className="bg-emerald-950 text-emerald-50">
                        Todas las rutas
                      </option>
                      {filterOptions.routes.map((routeOption) => (
                        <option
                          key={routeOption}
                          value={routeOption}
                          className="bg-emerald-950 text-emerald-50"
                        >
                          {routeOption}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="flex flex-col gap-1.5">
                    <label htmlFor="status" className="text-sm text-emerald-100/85">
                      Estado
                    </label>
                    <select
                      id="status"
                      value={filters.status}
                      onChange={(event) =>
                        setFilters((currentFilters) => ({
                          ...currentFilters,
                          status: event.target.value,
                        }))
                      }
                      disabled={loadingOptions}
                      className="h-9 rounded-2xl border border-emerald-200/30 bg-emerald-950/45 px-3 text-sm text-emerald-50 focus-visible:ring-2 focus-visible:ring-emerald-300/60"
                      style={{ colorScheme: "dark" }}
                    >
                      <option value="" className="bg-emerald-950 text-emerald-50">
                        Todos los estados
                      </option>
                      {filterOptions.statuses.map((statusOption) => (
                        <option
                          key={statusOption}
                          value={statusOption}
                          className="bg-emerald-950 text-emerald-50"
                        >
                          {statusOption}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="border-emerald-200/35 bg-emerald-50/10 text-emerald-50 hover:bg-emerald-50/20 hover:text-emerald-50"
                    onClick={() => {
                      setFilters(DEFAULT_FILTERS)
                      setError(null)
                    }}
                    disabled={
                      !filters.startDate &&
                      !filters.endDate &&
                      !filters.route &&
                      !filters.status
                    }
                  >
                    Limpiar filtros
                  </Button>
                  {filters.startDate ? (
                    <Badge variant="secondary" className="border-emerald-200/25 bg-emerald-100/15 text-emerald-50">
                      Desde: {filters.startDate}
                    </Badge>
                  ) : null}
                  {filters.endDate ? (
                    <Badge variant="secondary" className="border-emerald-200/25 bg-emerald-100/15 text-emerald-50">
                      Hasta: {filters.endDate}
                    </Badge>
                  ) : null}
                  {filters.route ? (
                    <Badge variant="secondary" className="border-emerald-200/25 bg-emerald-100/15 text-emerald-50">
                      Ruta: {filters.route}
                    </Badge>
                  ) : null}
                  {filters.status ? (
                    <Badge variant="secondary" className="border-emerald-200/25 bg-emerald-100/15 text-emerald-50">
                      Estado: {filters.status}
                    </Badge>
                  ) : null}
                </div>
              </CardContent>
            </Card>
          </aside>

          <div className="flex flex-col gap-6">
            {error ? (
              <Alert variant="destructive" className="animate-rise">
                <AlertTriangle />
                <AlertTitle>Fallo de conexion con FastAPI</AlertTitle>
                <AlertDescription>
                  {error}. Verifica que el backend este activo en {API_BASE_URL}.
                </AlertDescription>
              </Alert>
            ) : null}

            <section className="flex flex-wrap items-center justify-between gap-3 rounded-3xl border border-border/70 bg-[linear-gradient(90deg,rgba(7,70,37,0.96),rgba(27,135,76,0.94))] p-4 text-emerald-50">
              <div className="space-y-1">
                <h2 className="font-heading text-xl md:text-2xl">KPIs estrategicos (6)</h2>
                <p className="text-sm text-emerald-100/80">
                  Indicadores clave definidos en el documento del proyecto.
                </p>
              </div>
              <Badge variant="secondary" className="border-emerald-100/20 bg-emerald-100/10 text-emerald-50">
                {activeFiltersCount} filtros activos
              </Badge>
            </section>

            <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {loading
                ? Array.from({ length: 6 }).map((_, index) => (
                    <Card key={index} size="sm" className="animate-rise">
                      <CardHeader className="gap-2">
                        <Skeleton className="h-4 w-24" />
                        <Skeleton className="h-8 w-40" />
                      </CardHeader>
                    </Card>
                  ))
                : formattedKpis.map((kpi) => {
                    const Icon = kpi.icon
                    return (
                      <Card key={kpi.id} size="sm" className="animate-rise">
                        <CardHeader className="gap-2">
                          <div className="flex items-center justify-between gap-3">
                            <CardDescription>{kpi.title}</CardDescription>
                            <Icon className="size-4 text-muted-foreground" />
                          </div>
                          <CardTitle className="text-2xl md:text-3xl">
                            {kpi.value}
                          </CardTitle>
                          <CardDescription>{kpi.hint}</CardDescription>
                          <p className="text-xs text-muted-foreground">
                            {kpi.definition}
                          </p>
                        </CardHeader>
                      </Card>
                    )
                  })}
            </section>

            <Separator />

            <section className="space-y-1">
              <h2 className="font-heading text-xl md:text-2xl">Analisis visual</h2>
              <p className="text-sm text-muted-foreground">
                Tendencias temporales, comparacion por rutas y referencia macro.
              </p>
            </section>

            <section className="rounded-2xl border border-border/70 bg-muted/20 p-4 text-sm text-muted-foreground">
              Lectura ejecutiva: si el CO2 crece mas rapido que las entregas,
              hay perdida de eficiencia. Si una ruta concentra emisiones, es
              candidata prioritaria para optimizacion.
            </section>

            <section className="grid gap-6">
              <Card className="animate-rise">
                <CardHeader>
                  <CardTitle>Tendencia mensual de entregas vs CO2</CardTitle>
                  <CardDescription>
                    Evolucion operativa para identificar meses de mayor presion.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <ChartContainer
                    config={monthlyChartConfig}
                    className="min-h-[280px] w-full"
                  >
                    <LineChart accessibilityLayer data={monthlyChartData}>
                      <CartesianGrid vertical={false} />
                      <XAxis
                        dataKey="period"
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                      />
                      <YAxis tickLine={false} axisLine={false} tickMargin={8} />
                      <ChartTooltip content={<ChartTooltipContent />} />
                      <ChartLegend content={<ChartLegendContent />} />
                      <Line
                        dataKey="entregas"
                        type="monotone"
                        stroke="var(--color-entregas)"
                        strokeWidth={2.5}
                        dot={false}
                      />
                      <Line
                        dataKey="co2"
                        type="monotone"
                        stroke="var(--color-co2)"
                        strokeWidth={2.5}
                        dot={false}
                      />
                    </LineChart>
                  </ChartContainer>
                </CardContent>
              </Card>

              <Card className="animate-rise">
                <CardHeader>
                  <CardTitle>Rutas con mayor huella de carbono</CardTitle>
                  <CardDescription>
                    Ranking de impacto total y frecuencia de viajes. Se muestra
                    debajo de la tendencia mensual para lectura secuencial.
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <ChartContainer
                    config={routeChartConfig}
                    className="min-h-[280px] w-full"
                  >
                    <BarChart accessibilityLayer data={routeChartData}>
                      <CartesianGrid vertical={false} />
                      <XAxis
                        dataKey="route"
                        tickLine={false}
                        axisLine={false}
                        tickMargin={12}
                        interval={0}
                        angle={-12}
                        textAnchor="end"
                        height={52}
                      />
                      <YAxis tickLine={false} axisLine={false} tickMargin={8} />
                      <ChartTooltip
                        content={<ChartTooltipContent labelKey="co2" />}
                      />
                      <Bar dataKey="co2" fill="var(--color-co2)" radius={8} />
                    </BarChart>
                  </ChartContainer>
                </CardContent>
              </Card>

              <section className="grid gap-6 xl:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
                <Card className="animate-rise">
                  <CardHeader>
                    <CardTitle>Benchmark Interno vs CEPAL (Indice base 100)</CardTitle>
                    <CardDescription>
                      Compara tendencias en la misma escala: 100 representa el
                      primer valor observado de cada serie.
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <ChartContainer
                      config={benchmarkChartConfig}
                      className="min-h-[180px] w-full"
                    >
                      <LineChart accessibilityLayer data={benchmarkChartData}>
                        <CartesianGrid vertical={false} />
                        <XAxis
                          dataKey="year"
                          tickLine={false}
                          axisLine={false}
                          tickMargin={8}
                        />
                        <YAxis tickLine={false} axisLine={false} tickMargin={8} />
                        <ChartTooltip content={<ChartTooltipContent />} />
                        <ChartLegend content={<ChartLegendContent />} />
                        <Line
                          dataKey="interno"
                          type="monotone"
                          stroke="var(--color-interno)"
                          strokeWidth={2.6}
                          dot={false}
                        />
                        <Line
                          dataKey="cepal"
                          type="monotone"
                          stroke="var(--color-cepal)"
                          strokeWidth={2.6}
                          dot={false}
                        />
                      </LineChart>
                    </ChartContainer>
                  </CardContent>
                </Card>

                <Card className="animate-rise">
                  <CardHeader>
                    <CardTitle>Para que te sirve este benchmark</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3 text-sm text-muted-foreground">
                    <p>
                      1. Si la linea Interno sube por encima de CEPAL, tu
                      huella crece mas rapido que el entorno de referencia.
                    </p>
                    <p>
                      2. Si la linea Interno baja mientras CEPAL sube, tu
                      operacion mejora en terminos relativos.
                    </p>
                    <p>
                      3. Usa este visual para evaluar direccion de tendencia,
                      no para comparar valores absolutos de unidades distintas.
                    </p>
                  </CardContent>
                </Card>
              </section>
            </section>

            {!loading && !hasData ? (
              <Alert className="animate-rise">
                <AlertTriangle />
                <AlertTitle>Sin datos para mostrar</AlertTitle>
                <AlertDescription>
                  La API responde, pero no llegaron registros para construir las
                  visualizaciones.
                </AlertDescription>
              </Alert>
            ) : null}
          </div>
        </section>
      </div>

      <Button
        type="button"
        onClick={() => setAssistantOpen((open) => !open)}
        className="fixed right-6 bottom-6 z-40 h-12 rounded-full px-4 shadow-lg"
      >
        <MessageSquare className="mr-2 size-4" />
        {assistantOpen ? "Cerrar asistente" : "Asistente IA"}
      </Button>

      {assistantOpen ? (
        <button
          type="button"
          aria-label="Cerrar panel del asistente"
          onClick={() => setAssistantOpen(false)}
          className="fixed inset-0 z-30 bg-black/35 lg:hidden"
        />
      ) : null}

      <aside
        className={`fixed top-4 right-4 bottom-22 z-40 max-h-[calc(100dvh-6.5rem)] w-[min(100vw-2rem,400px)] transition-transform duration-300 ease-out lg:bottom-4 lg:max-h-[calc(100dvh-2rem)] ${
          assistantOpen ? "translate-x-0" : "translate-x-[110%]"
        }`}
      >
        <Card className="flex h-full flex-col border-primary/20 shadow-2xl">
          <CardHeader className="gap-1.5 border-b border-border/60 pb-2.5">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <Bot className="size-5 text-primary" />
                <CardTitle>Asistente IA de Decisiones</CardTitle>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="h-6 px-2 text-[10px] font-normal text-muted-foreground">
                  Modelo: {assistantModel}
                </Badge>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  onClick={() => setAssistantOpen(false)}
                  aria-label="Cerrar"
                >
                  <X className="size-4" />
                </Button>
              </div>
            </div>
          </CardHeader>

          <CardContent className="flex min-h-0 flex-1 flex-col gap-2.5 p-3.5">
            {assistantError ? (
              <Alert variant="destructive">
                <AlertTriangle />
                <AlertTitle>No se pudo consultar la IA</AlertTitle>
                <AlertDescription>{assistantError}</AlertDescription>
              </Alert>
            ) : null}

            <div className="min-h-0 flex-1 space-y-3 overflow-y-auto rounded-2xl border border-border/70 bg-muted/20 p-3">
              {assistantMessages.map((message, index) => (
                <div
                  key={`${message.role}-${index}`}
                  className={`flex ${message.role === "user" ? "justify-end" : "justify-start"}`}
                >
                  <div
                    className={`max-w-[95%] rounded-2xl px-3 py-2 text-sm ${
                      message.role === "user"
                        ? "bg-primary text-primary-foreground"
                        : "border border-border/70 bg-card"
                    }`}
                  >
                    <p className="mb-1 text-[11px] uppercase tracking-wide opacity-75">
                      {message.role === "user" ? "Tu" : "Asistente IA"}
                    </p>
                    {message.role === "assistant" ? (
                      <div className="assistant-markdown">
                        <ReactMarkdown remarkPlugins={[remarkGfm]}>
                          {message.content}
                        </ReactMarkdown>
                      </div>
                    ) : (
                      <p className="whitespace-pre-wrap">{message.content}</p>
                    )}
                  </div>
                </div>
              ))}

              {assistantLoading ? (
                <div className="flex justify-start">
                  <div className="flex items-center gap-2 rounded-2xl border border-border/70 bg-card px-3 py-2 text-sm">
                    <Loader2 className="size-4 animate-spin" />
                    Analizando datos y redactando recomendacion...
                  </div>
                </div>
              ) : null}
            </div>

            <details className="rounded-lg border border-border/70 bg-muted/10 p-2">
              <summary className="cursor-pointer select-none text-[11px] font-medium text-muted-foreground">
                Opciones del asistente
              </summary>

              <div className="mt-2 flex items-center justify-between rounded-md border border-border/60 bg-muted/20 px-2 py-1.5">
                <div className="text-[11px] text-muted-foreground">Proveedor IA</div>
                <div className="flex items-center gap-2">
                  <span
                    className={`text-[11px] ${
                      assistantProvider === "openrouter"
                        ? "font-semibold text-foreground"
                        : "text-muted-foreground"
                    }`}
                  >
                    Open model
                  </span>

                  <button
                    type="button"
                    role="switch"
                    aria-checked={assistantProvider === "gemini"}
                    onClick={() => {
                      setAssistantProvider((currentProvider) =>
                        currentProvider === "openrouter" ? "gemini" : "openrouter",
                      )
                      setAssistantError(null)
                    }}
                    className={`relative h-5 w-10 rounded-full transition-colors ${
                      assistantProvider === "gemini" ? "bg-primary" : "bg-muted"
                    }`}
                  >
                    <span
                      className={`absolute top-0.5 size-4 rounded-full bg-white shadow transition-transform ${
                        assistantProvider === "gemini"
                          ? "translate-x-5"
                          : "translate-x-0.5"
                      }`}
                    />
                  </button>

                  <span
                    className={`text-[11px] ${
                      assistantProvider === "gemini"
                        ? "font-semibold text-foreground"
                        : "text-muted-foreground"
                    }`}
                  >
                    Gemini
                  </span>
                </div>
              </div>

              <div className="mt-2 flex gap-2 overflow-x-auto pb-1">
                <Button
                  type="button"
                  size="sm"
                  className="h-7 shrink-0 px-2 text-[11px]"
                  disabled={assistantLoading}
                  onClick={() => {
                    void askDecisionAssistant(
                      "Genera una alerta ejecutiva: que esta en riesgo hoy y que hacer en las proximas 48 horas.",
                    )
                  }}
                >
                  <Sparkles className="mr-1.5 size-3.5" />
                  Recomendacion proactiva
                </Button>

                {assistantQuickActions.map((quickPrompt) => (
                  <Button
                    key={quickPrompt}
                    type="button"
                    size="sm"
                    variant="outline"
                    className="h-7 shrink-0 px-2 text-[11px]"
                    disabled={assistantLoading}
                    onClick={() => {
                      void askDecisionAssistant(quickPrompt)
                    }}
                  >
                    {quickPrompt}
                  </Button>
                ))}
              </div>
            </details>

            <form onSubmit={handleAssistantSubmit} className="space-y-2">
              <label htmlFor="assistant-input" className="text-xs font-medium text-muted-foreground">
                Consulta al asistente
              </label>
              <textarea
                id="assistant-input"
                value={assistantInput}
                onChange={(event) => setAssistantInput(event.target.value)}
                rows={2}
                placeholder="Ejemplo: Segun los filtros actuales, donde debo actuar primero y que impacto espero en OTIF y CO2?"
                className="w-full rounded-2xl border border-input bg-background px-3 py-2 text-sm"
              />
              <div className="flex justify-end">
                <Button type="submit" disabled={assistantLoading || !assistantInput.trim()}>
                  <SendHorizontal className="mr-2 size-4" />
                  Enviar pregunta
                </Button>
              </div>
            </form>
          </CardContent>
        </Card>
      </aside>
    </main>
  )
}

export default App

import axios from 'axios'

const API_BASE = '/api'

export interface TradeFlow {
  mineral: string
  reporter_country: string
  reporter_iso3: string | null
  partner_country: string
  partner_iso3: string | null
  flow_type: string
  value_usd: number
  quantity: number | null
  weight_kg: number | null
  period: string
}

export interface TradeFlowsResponse {
  flows: TradeFlow[]
  total: number
  total_value_usd: number
}

export interface CountryVolume {
  country: string
  iso3: string | null
  import_value: number
  export_value: number
  total_value: number
  minerals: string[]
}

export interface CountriesResponse {
  countries: CountryVolume[]
  total_countries: number
}

export interface SankeyNode {
  id: string
  name: string
  type: string
}

export interface SankeyLink {
  source: string
  target: string
  value: number
}

export interface SankeyResponse {
  nodes: SankeyNode[]
  links: SankeyLink[]
  mineral: string
}

export interface PortData {
  port_name: string
  port_code: string | null
  country: string | null
  region: string | null
  metric_type: string
  value: number
  unit: string
  period: string
  change_percent: number | null
}

export interface PortsResponse {
  ports: PortData[]
  total_ports: number
}

export interface TradeStats {
  total_trade_value: number
  total_flows: number
  minerals_tracked: string[]
  top_exporters: Array<{ country: string; value: number }>
  top_importers: Array<{ country: string; value: number }>
  period_range: { start: string | null; end: string | null }
}

// API functions
export async function fetchTradeFlows(params: {
  mineral?: string
  reporter?: string
  partner?: string
  flow_type?: string
  top_n?: number
}): Promise<TradeFlowsResponse> {
  const response = await axios.get(`${API_BASE}/trade/flows`, { params })
  return response.data
}

export async function fetchCountries(params: {
  mineral?: string
  period?: string
}): Promise<CountriesResponse> {
  const response = await axios.get(`${API_BASE}/trade/countries`, { params })
  return response.data
}

export async function fetchSankeyData(mineral: string): Promise<SankeyResponse> {
  const response = await axios.get(`${API_BASE}/trade/flows/sankey`, {
    params: { mineral, top_n: 20 },
  })
  return response.data
}

export async function fetchPorts(params: {
  region?: string
  metric_type?: string
  limit?: number
}): Promise<PortsResponse> {
  const response = await axios.get(`${API_BASE}/trade/ports`, { params })
  return response.data
}

export async function fetchTradeStats(period?: string): Promise<TradeStats> {
  const response = await axios.get(`${API_BASE}/trade/stats`, {
    params: period ? { period } : {},
  })
  return response.data
}

export async function fetchMinerals(): Promise<string[]> {
  const response = await axios.get(`${API_BASE}/trade/minerals`)
  return response.data
}

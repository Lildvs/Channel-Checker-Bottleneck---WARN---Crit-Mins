import axios from 'axios'

const API_BASE = '/api'

export interface Bottleneck {
  id: string
  category: string
  subcategory: string
  severity: number
  confidence: number
  strength: string
  affected_sectors: string[]
  affected_commodities?: string[]
  source_series?: string[]
  description: string
  detected_at: string
  status: string
  evidence?: Record<string, number | string>
}

export interface BottlenecksResponse {
  bottlenecks: Bottleneck[]
  total: number
  active_count: number
}

export interface SystemicRiskResponse {
  risk_scores: Record<string, number>
  highest_risk_sector: string
  lowest_risk_sector: string
  average_risk: number
}

export interface TrendDataPoint {
  date: string
  count: number
}

export interface BottleneckTrendResponse {
  data: TrendDataPoint[]
}

export interface DetectionResponse {
  total: number
  bottlenecks: Bottleneck[]
}

// API functions
export async function fetchActiveBottlenecks(params?: {
  category?: string
  min_severity?: number
  status?: string
}): Promise<BottlenecksResponse> {
  const response = await axios.get(`${API_BASE}/bottlenecks/active`, { params })
  return response.data
}

export async function fetchSystemicRisk(): Promise<SystemicRiskResponse> {
  const response = await axios.get(`${API_BASE}/sectors/risk/systemic`)
  return response.data
}

export async function fetchBottleneckTrend(): Promise<BottleneckTrendResponse> {
  const response = await axios.get(`${API_BASE}/bottlenecks/trend`)
  return response.data
}

export interface CollectionSummary {
  success: boolean
  total_collectors: number
  succeeded: number
  failed: number
  total_records: number
}

export interface FullDetectionResult {
  collection: CollectionSummary
  detection: DetectionResponse
}

export async function runAllCollectors(): Promise<CollectionSummary> {
  const response = await axios.post(`${API_BASE}/data/collectors/run-all`)
  return response.data
}

export async function triggerDetection(lookbackDays: number): Promise<DetectionResponse> {
  const response = await axios.get(`${API_BASE}/bottlenecks/detect`, {
    params: { lookback_days: lookbackDays },
  })
  return response.data
}

export async function collectAndDetect(lookbackDays: number): Promise<FullDetectionResult> {
  const collection = await runAllCollectors()
  const detection = await triggerDetection(lookbackDays)
  return { collection, detection }
}

export async function fetchBottleneckById(id: string): Promise<Bottleneck> {
  const response = await axios.get(`${API_BASE}/bottlenecks/${id}`)
  return response.data
}

export interface SectorImpact {
  sector_code: string
  sector_name: string
  impact_score: number
  impact_type: string
  propagation_path: string[]
  lag_days: number
}

export interface ImpactPropagationResponse {
  bottleneck_id: string
  impacts: SectorImpact[]
  total_sectors_affected: number
}

export interface PropagationPath {
  nodes: string[]
  node_names: string[]
  coefficients: number[]
  cumulative_impact: number
  hop_count: number
  has_cycle: boolean
}

export interface FullPropagationResponse {
  bottleneck_id: string
  origin_category: string
  origin_severity: number
  total_economic_impact: number
  propagation_rounds: number
  convergence_reached: boolean
  severity_classification: string
  amplification_detected: string[]
  affected_sectors: SectorImpact[]
  propagation_paths: PropagationPath[]
  analysis_timestamp: string
}

export async function fetchBottleneckImpact(id: string): Promise<ImpactPropagationResponse> {
  const response = await axios.get(`${API_BASE}/bottlenecks/${id}/impact`)
  return response.data
}

export async function fetchBottleneckPropagation(id: string): Promise<FullPropagationResponse> {
  const response = await axios.get(`${API_BASE}/bottlenecks/${id}/propagation`)
  return response.data
}

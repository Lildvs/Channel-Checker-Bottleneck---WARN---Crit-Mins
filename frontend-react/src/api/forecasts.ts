import axios from 'axios'

const API_BASE = '/api'

export interface DurationForecast {
  bottleneck_id: string
  expected_duration_days: number
  expected_resolution_date: string
  probability_persists_30_days: number
  probability_persists_60_days: number
  probability_persists_90_days: number
  confidence_lower_days: number
  confidence_upper_days: number
  model_used: string
  reasoning: string
}

export interface TrajectoryPoint {
  day: number
  date: string
  severity: number
  lower_bound: number
  upper_bound: number
}

export interface TrajectoryForecast {
  bottleneck_id: string
  trajectory: TrajectoryPoint[]
  expected_resolution_day: number
  final_severity: number
}

export interface HistoricalPrecedent {
  id: string
  category: string
  severity: number
  duration_days: number
  resolution_date: string
  similarity_score: number
}

export interface ResearchReport {
  bottleneck_id: string
  summary: string
  key_findings: string[]
  historical_precedents: HistoricalPrecedent[]
  data_sources: string[]
  confidence_level: string
  generated_at: string
}

// API functions
export async function fetchDurationForecast(bottleneckId: string): Promise<DurationForecast> {
  const response = await axios.post(`${API_BASE}/forecasts/duration`, {
    bottleneck_id: bottleneckId,
  })
  return response.data
}

export async function fetchTrajectoryForecast(
  bottleneckId: string,
  horizonDays: number = 30
): Promise<TrajectoryForecast> {
  const response = await axios.post(`${API_BASE}/forecasts/trajectory`, {
    bottleneck_id: bottleneckId,
    horizon_days: horizonDays,
  })
  return response.data
}

export async function fetchResearchReport(bottleneckId: string): Promise<ResearchReport> {
  const response = await axios.get(`${API_BASE}/forecasts/${bottleneckId}/research`)
  return response.data
}

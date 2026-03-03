import axios from 'axios'

const API_BASE = '/api'

export interface WarnStats {
  total_notices: number
  total_employees_affected: number
  states_reporting: number
  avg_employees_per_notice: number
  closures_count: number
  temporary_count: number
  date_range: {
    start: string
    end: string
  }
}

export interface TrendDataPoint {
  period: string
  notice_count: number
  employees_affected: number
}

export interface WarnTrendsResponse {
  granularity: string
  data: TrendDataPoint[]
}

export interface StateBreakdown {
  state: string
  state_name: string
  notice_count: number
  employees_affected: number
  pct_of_total: number
}

export interface StateBreakdownResponse {
  states: StateBreakdown[]
  total_states: number
}

export interface SectorBreakdown {
  sector: string
  notice_count: number
  employees_affected: number
  pct_of_total: number
  avg_employees_per_notice: number
}

export interface SectorBreakdownResponse {
  sectors: SectorBreakdown[]
}

export interface SizeBucket {
  min_employees: number
  max_employees: number | null
  label: string
  count: number
  pct_of_total: number
}

export interface SizeDistributionResponse {
  buckets: SizeBucket[]
  total_notices: number
}

export interface WarnNotice {
  id: string
  company_name: string
  state: string
  city: string
  notice_date: string
  effective_date: string
  employees_affected: number
  layoff_type: string
  sector_category: string
  is_closure: boolean
}

export interface WarnNoticesResponse {
  notices: WarnNotice[]
  total: number
  page: number
  page_size: number
  has_more: boolean
}

// API functions
export async function fetchWarnStats(params?: {
  start_date?: string
  end_date?: string
}): Promise<WarnStats> {
  const response = await axios.get(`${API_BASE}/warn/notices/stats`, { params })
  return response.data
}

export async function fetchWarnTrends(params?: {
  granularity?: string
  start_date?: string
  end_date?: string
}): Promise<WarnTrendsResponse> {
  const response = await axios.get(`${API_BASE}/warn/notices/trends`, { params })
  return response.data
}

export async function fetchWarnByState(params?: {
  start_date?: string
  end_date?: string
}): Promise<StateBreakdownResponse> {
  const response = await axios.get(`${API_BASE}/warn/notices/by-state`, { params })
  return response.data
}

export async function fetchWarnBySector(params?: {
  start_date?: string
  end_date?: string
}): Promise<SectorBreakdownResponse> {
  const response = await axios.get(`${API_BASE}/warn/notices/by-sector`, { params })
  return response.data
}

export async function fetchWarnSizes(params?: {
  start_date?: string
  end_date?: string
}): Promise<SizeDistributionResponse> {
  const response = await axios.get(`${API_BASE}/warn/notices/company-sizes`, { params })
  return response.data
}

export async function fetchWarnNotices(params?: {
  page?: number
  page_size?: number
  state?: string
  sector?: string
}): Promise<WarnNoticesResponse> {
  const response = await axios.get(`${API_BASE}/warn/notices`, { params })
  return response.data
}

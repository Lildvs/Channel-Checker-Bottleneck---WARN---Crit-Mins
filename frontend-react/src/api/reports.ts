import axios from 'axios'

const API_BASE = '/api'

export interface LogEntry {
  id: number
  timestamp: string
  level: string
  logger_name: string | null
  event: string
  source_module: string | null
  extra_data: Record<string, unknown>
}

export interface LogsResponse {
  logs: LogEntry[]
  total: number
  page: number
  page_size: number
  has_more: boolean
}

export interface LogStats {
  total_logs: number
  error_count: number
  warning_count: number
  critical_count: number
  info_count: number
  recent_errors: number
  top_sources: { source: string; count: number }[]
  oldest_log: string | null
  newest_log: string | null
}

export interface CollectorJobEntry {
  id: string
  collector_name: string
  started_at: string
  completed_at: string | null
  status: string
  records_collected: number
  error_message: string | null
  duration_seconds: number | null
}

export interface CollectorJobsResponse {
  jobs: CollectorJobEntry[]
  total: number
  page: number
  page_size: number
  has_more: boolean
}

// API functions

export async function fetchSystemLogs(params?: {
  page?: number
  page_size?: number
  level?: string
  source?: string
  search?: string
  start_date?: string
  end_date?: string
}): Promise<LogsResponse> {
  const response = await axios.get(`${API_BASE}/reports/logs`, { params })
  return response.data
}

export async function fetchLogStats(): Promise<LogStats> {
  const response = await axios.get(`${API_BASE}/reports/logs/stats`)
  return response.data
}

export async function fetchCollectionJobs(params?: {
  page?: number
  page_size?: number
  collector?: string
  status?: string
}): Promise<CollectorJobsResponse> {
  const response = await axios.get(`${API_BASE}/reports/collection-jobs`, { params })
  return response.data
}

import axios from 'axios'

const API_BASE = '/api'

export interface Paper {
  id: string
  doi: string | null
  arxiv_id: string | null
  title: string
  abstract: string | null
  authors: string[]
  institutions: string[]
  published_date: string
  source: string
  topics: string[]
  quick_score: number
  citation_count: number
  reference_count: number
  research_type: 'consensus' | 'emerging' | 'contrarian' | 'low_quality_contrarian'
  contrarian_confidence: number
  url: string
  pdf_url: string | null
  code_url: string | null
}

export interface PapersResponse {
  papers: Paper[]
  total: number
  page: number
  page_size: number
  has_more: boolean
}

export interface TopicStats {
  topic: string
  paper_count: number
  avg_score: number
  contrarian_count: number
  emerging_count: number
  recent_count: number
}

export interface TopicsResponse {
  topics: TopicStats[]
  total_papers: number
}

export interface ResearchStats {
  total_papers: number
  papers_last_7_days: number
  papers_last_30_days: number
  avg_quick_score: number
  contrarian_count: number
  emerging_count: number
  consensus_count: number
  topics_covered: number
  sources: Record<string, number>
  top_topics: Array<{ topic: string; count: number }>
}

export interface SignalPoint {
  timestamp: string
  topic: string
  paper_count: number
  new_paper_count: number
  contrarian_count: number
  emerging_count: number
  avg_quick_score: number
}

export interface SignalsResponse {
  signals: SignalPoint[]
  topics: string[]
}

// API functions
export async function fetchPapers(params: {
  page?: number
  page_size?: number
  topic?: string
  research_type?: string
  search?: string
  min_score?: number
}): Promise<PapersResponse> {
  const response = await axios.get(`${API_BASE}/research/papers`, { params })
  return response.data
}

export async function fetchPaper(id: string): Promise<Paper> {
  const response = await axios.get(`${API_BASE}/research/papers/${id}`)
  return response.data
}

export async function fetchTopics(): Promise<TopicsResponse> {
  const response = await axios.get(`${API_BASE}/research/topics`)
  return response.data
}

export async function fetchResearchStats(): Promise<ResearchStats> {
  const response = await axios.get(`${API_BASE}/research/stats`)
  return response.data
}

export async function fetchSignals(params: {
  topic?: string
  days?: number
}): Promise<SignalsResponse> {
  const response = await axios.get(`${API_BASE}/research/signals`, { params })
  return response.data
}

export async function fetchContrarianPapers(params: {
  page?: number
  page_size?: number
  include_emerging?: boolean
}): Promise<PapersResponse> {
  const response = await axios.get(`${API_BASE}/research/contrarian`, { params })
  return response.data
}

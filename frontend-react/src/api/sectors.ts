import axios from 'axios'

const API_BASE = '/api'

export interface SectorNode {
  id: string
  name: string
  category: string
  riskScore: number
  size: number // GDP contribution or other metric
}

export interface SectorEdge {
  source: string
  target: string
  weight: number
  dependencyType: 'supply' | 'demand' | 'both'
}

export interface PropagationStep {
  step: number
  sector: string
  impactLevel: number
  fromSector: string | null
}

export interface PropagationData {
  bottleneckId: string
  originSector: string
  steps: PropagationStep[]
  totalImpact: number
}

export interface GraphData {
  nodes: SectorNode[]
  edges: SectorEdge[]
}

// API client functions
export async function fetchGraphNodes(): Promise<SectorNode[]> {
  const response = await axios.get(`${API_BASE}/sectors/graph/nodes`)
  return response.data
}

export async function fetchGraphEdges(): Promise<SectorEdge[]> {
  const response = await axios.get(`${API_BASE}/sectors/graph/edges`)
  return response.data
}

export async function fetchPropagationData(bottleneckId: string): Promise<PropagationData> {
  const response = await axios.get(`${API_BASE}/sectors/graph/propagation/${bottleneckId}`)
  return response.data
}

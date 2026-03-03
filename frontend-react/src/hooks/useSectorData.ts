import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  fetchGraphNodes,
  fetchGraphEdges,
  fetchPropagationData,
  SectorNode,
  SectorEdge,
  PropagationData,
} from '../api/sectors'

export function useSectorData() {
  const [propagationData, setPropagationData] = useState<PropagationData | null>(null)

  const {
    data: nodes = [],
    isLoading: nodesLoading,
    error: nodesError,
  } = useQuery({
    queryKey: ['sectorNodes'],
    queryFn: fetchGraphNodes,
  })

  const {
    data: edges = [],
    isLoading: edgesLoading,
    error: edgesError,
  } = useQuery({
    queryKey: ['sectorEdges'],
    queryFn: fetchGraphEdges,
  })

  const isLoading = nodesLoading || edgesLoading
  const error = nodesError || edgesError

  const fetchPropagation = async (bottleneckId: string) => {
    try {
      const data = await fetchPropagationData(bottleneckId)
      setPropagationData(data)
    } catch (err) {
      console.error('Failed to fetch propagation:', err)
    }
  }

  const clearPropagation = () => {
    setPropagationData(null)
  }

  return {
    nodes,
    edges,
    isLoading,
    error,
    propagationData,
    fetchPropagation,
    clearPropagation,
  }
}

export type { SectorNode, SectorEdge, PropagationData }

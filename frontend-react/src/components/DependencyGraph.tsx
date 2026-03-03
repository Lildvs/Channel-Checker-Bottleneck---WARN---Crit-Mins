import { FC, useRef, useEffect, useState, useCallback, useMemo } from 'react'
import * as d3 from 'd3'
import { SectorNode, SectorEdge, PropagationData } from '../api/sectors'
import SectorTooltip from './SectorTooltip'

interface DependencyGraphProps {
  nodes: SectorNode[]
  edges: SectorEdge[]
  selectedSector: string | null
  onSectorClick: (sectorId: string) => void
  categoryFilter: string[]
  severityThreshold: number
  propagationData: PropagationData | null
  playbackSpeed: number
}

interface D3Node extends SectorNode {
  x?: number
  y?: number
  fx?: number | null
  fy?: number | null
}

interface D3Link {
  source: D3Node | string
  target: D3Node | string
  weight: number
  dependencyType: string
}

const DependencyGraph: FC<DependencyGraphProps> = ({
  nodes,
  edges,
  selectedSector,
  onSectorClick,
  categoryFilter,
  severityThreshold,
  propagationData,
  playbackSpeed,
}) => {
  const svgRef = useRef<SVGSVGElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const tooltipRef = useRef<HTMLDivElement>(null)
  const tooltipNodeRef = useRef<SectorNode | null>(null)
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 })
  const [tooltipVisible, setTooltipVisible] = useState(false)
  const [currentPropagationStep, setCurrentPropagationStep] = useState(0)

  const filteredNodes = nodes.filter(node => {
    if (categoryFilter.length > 0 && !categoryFilter.includes(node.category)) {
      return false
    }
    if (node.riskScore < severityThreshold) {
      return false
    }
    return true
  })

  const filteredNodeIds = new Set(filteredNodes.map(n => n.id))
  const filteredEdges = edges.filter(
    edge => filteredNodeIds.has(edge.source) && filteredNodeIds.has(edge.target)
  )

  const riskColor = useCallback((score: number): string => {
    if (score >= 0.7) return '#FF5F1F'
    if (score >= 0.4) return '#F89880'
    return '#4ecca3'
  }, [])

  // Size scale based on GDP contribution (memoized to prevent D3 effect re-runs)
  const sizeScale = useMemo(
    () => d3.scaleSqrt().domain([0, 1000]).range([20, 60]),
    [],
  )

  const getConnectedSectors = useCallback((sectorId: string): Set<string> => {
    const connected = new Set<string>([sectorId])
    edges.forEach(edge => {
      if (edge.source === sectorId) connected.add(edge.target)
      if (edge.target === sectorId) connected.add(edge.source)
    })
    return connected
  }, [edges])

  useEffect(() => {
    if (!propagationData) {
      setCurrentPropagationStep(0)
      return
    }

    const maxStep = Math.max(...propagationData.steps.map(s => s.step))
    let step = 0

    const interval = setInterval(() => {
      step = (step + 1) % (maxStep + 2) // +2 for pause at end
      setCurrentPropagationStep(step)
    }, 1000 / playbackSpeed)

    return () => clearInterval(interval)
  }, [propagationData, playbackSpeed])

  useEffect(() => {
    if (!svgRef.current || filteredNodes.length === 0) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const width = dimensions.width
    const height = dimensions.height

    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.3, 3])
      .on('zoom', (event) => {
        container.attr('transform', event.transform)
      })

    svg.call(zoom)

    const container = svg.append('g')

    const nodeData: D3Node[] = filteredNodes.map(d => ({ ...d }))
    const linkData: D3Link[] = filteredEdges.map(d => ({
      source: d.source,
      target: d.target,
      weight: d.weight,
      dependencyType: d.dependencyType,
    }))

    const simulation = d3.forceSimulation<D3Node>(nodeData)
      .force('link', d3.forceLink<D3Node, D3Link>(linkData)
        .id(d => d.id)
        .distance(d => 150 - d.weight * 50)
        .strength(d => d.weight * 0.5)
      )
      .force('charge', d3.forceManyBody().strength(-400))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide<D3Node>().radius(d => sizeScale(d.size) + 10))

    svg.append('defs').append('marker')
      .attr('id', 'arrowhead')
      .attr('viewBox', '-0 -5 10 10')
      .attr('refX', 20)
      .attr('refY', 0)
      .attr('orient', 'auto')
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .append('path')
      .attr('d', 'M 0,-5 L 10,0 L 0,5')
      .attr('fill', '#4a4a6a')

    const link = container.append('g')
      .attr('class', 'links')
      .selectAll('line')
      .data(linkData)
      .join('line')
      .attr('class', 'link')
      .attr('stroke-width', d => Math.max(1, d.weight * 4))
      .attr('marker-end', 'url(#arrowhead)')

    const node = container.append('g')
      .attr('class', 'nodes')
      .selectAll('g')
      .data(nodeData)
      .join('g')
      .attr('class', 'node')
      .call(d3.drag<SVGGElement, D3Node>()
        .on('start', (event, d) => {
          if (!event.active) simulation.alphaTarget(0.3).restart()
          d.fx = d.x
          d.fy = d.y
        })
        .on('drag', (event, d) => {
          d.fx = event.x
          d.fy = event.y
        })
        .on('end', (event, d) => {
          if (!event.active) simulation.alphaTarget(0)
          d.fx = null
          d.fy = null
        }) as any
      )

    node.append('circle')
      .attr('r', d => sizeScale(d.size))
      .attr('fill', d => riskColor(d.riskScore))
      .attr('stroke', '#2a2a4a')
      .attr('stroke-width', 2)
      .on('click', (event, d) => {
        event.stopPropagation()
        onSectorClick(d.id)
      })
      .on('mouseenter', (event, d) => {
        tooltipNodeRef.current = d
        setTooltipVisible(true)
        if (tooltipRef.current) {
          tooltipRef.current.style.left = `${event.pageX + 15}px`
          tooltipRef.current.style.top = `${event.pageY - 10}px`
        }
      })
      .on('mousemove', (event) => {
        if (tooltipRef.current) {
          tooltipRef.current.style.left = `${event.pageX + 15}px`
          tooltipRef.current.style.top = `${event.pageY - 10}px`
        }
      })
      .on('mouseleave', () => {
        tooltipNodeRef.current = null
        setTooltipVisible(false)
      })

    node.append('text')
      .attr('class', 'node-label')
      .attr('dy', 4)
      .text(d => d.name.length > 12 ? d.name.slice(0, 10) + '...' : d.name)

    simulation.on('tick', () => {
      link
        .attr('x1', d => (d.source as D3Node).x!)
        .attr('y1', d => (d.source as D3Node).y!)
        .attr('x2', d => (d.target as D3Node).x!)
        .attr('y2', d => (d.target as D3Node).y!)

      node.attr('transform', d => `translate(${d.x},${d.y})`)
    })

    const updateHighlights = () => {
      if (selectedSector) {
        const connected = getConnectedSectors(selectedSector)

        node.classed('selected', d => d.id === selectedSector)
        node.classed('highlighted', d => connected.has(d.id) && d.id !== selectedSector)
        node.classed('dimmed', d => !connected.has(d.id))

        link.classed('highlighted', d => {
          const sourceId = typeof d.source === 'string' ? d.source : (d.source as D3Node).id
          const targetId = typeof d.target === 'string' ? d.target : (d.target as D3Node).id
          return (sourceId === selectedSector || targetId === selectedSector)
        })
        link.classed('dimmed', d => {
          const sourceId = typeof d.source === 'string' ? d.source : (d.source as D3Node).id
          const targetId = typeof d.target === 'string' ? d.target : (d.target as D3Node).id
          return !(sourceId === selectedSector || targetId === selectedSector)
        })
      } else {
        node.classed('selected', false)
        node.classed('highlighted', false)
        node.classed('dimmed', false)
        link.classed('highlighted', false)
        link.classed('dimmed', false)
      }
    }

    updateHighlights()

    const updatePropagation = () => {
      if (!propagationData) {
        node.select('circle').attr('stroke', '#2a2a4a').attr('stroke-width', 2)
        link.classed('propagating', false)
        return
      }

      const activeSteps = propagationData.steps.filter(s => s.step <= currentPropagationStep)
      const activeSectors = new Set(activeSteps.map(s => s.sector))

      node.select('circle')
        .attr('stroke', d => activeSectors.has(d.id) ? '#FF5F1F' : '#2a2a4a')
        .attr('stroke-width', d => activeSectors.has(d.id) ? 4 : 2)

      link.classed('propagating', d => {
        const sourceId = typeof d.source === 'string' ? d.source : (d.source as D3Node).id
        const targetId = typeof d.target === 'string' ? d.target : (d.target as D3Node).id

        return activeSteps.some(s =>
          s.fromSector === sourceId && s.sector === targetId && s.step === currentPropagationStep
        )
      })
    }

    updatePropagation()

    return () => {
      simulation.stop()
    }
  }, [
    filteredNodes,
    filteredEdges,
    dimensions,
    selectedSector,
    getConnectedSectors,
    onSectorClick,
    propagationData,
    currentPropagationStep,
    sizeScale,
    riskColor,
  ])

  useEffect(() => {
    const handleResize = () => {
      if (containerRef.current) {
        const rect = containerRef.current.getBoundingClientRect()
        setDimensions({ width: rect.width, height: rect.height })
      }
    }

    handleResize()
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%', position: 'relative' }}>
      <svg
        ref={svgRef}
        className="graph-svg"
        width={dimensions.width}
        height={dimensions.height}
      />
      <SectorTooltip
        ref={tooltipRef}
        node={tooltipNodeRef.current}
        visible={tooltipVisible}
      />
    </div>
  )
}

export default DependencyGraph

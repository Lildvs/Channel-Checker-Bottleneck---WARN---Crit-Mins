import { FC, useRef, useEffect } from 'react'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal } from 'd3-sankey'
import { SankeyNode, SankeyLink } from '../../api/tradeFlows'

interface SankeyDiagramProps {
  nodes: SankeyNode[]
  links: SankeyLink[]
  onNodeHover?: (id: string | null) => void
}

const SankeyDiagram: FC<SankeyDiagramProps> = ({
  nodes,
  links,
  onNodeHover,
}) => {
  const svgRef = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!svgRef.current || nodes.length === 0) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const width = 400
    const height = Math.max(300, nodes.length * 25)
    const margin = { top: 10, right: 10, bottom: 10, left: 10 }

    svg.attr('width', width).attr('height', height)

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`)

    const innerWidth = width - margin.left - margin.right
    const innerHeight = height - margin.top - margin.bottom

    const nodeMap = new Map<string, number>()
    nodes.forEach((node, i) => nodeMap.set(node.id, i))

    const sankeyLinks = links
      .filter(link => nodeMap.has(link.source) && nodeMap.has(link.target))
      .map(link => ({
        source: nodeMap.get(link.source)!,
        target: nodeMap.get(link.target)!,
        value: link.value,
      }))

    if (sankeyLinks.length === 0) {
      g.append('text')
        .attr('x', innerWidth / 2)
        .attr('y', innerHeight / 2)
        .attr('text-anchor', 'middle')
        .attr('fill', '#a0a0a0')
        .text('No flow data available')
      return
    }

    const sankeyGenerator = sankey<any, any>()
      .nodeWidth(15)
      .nodePadding(10)
      .extent([[0, 0], [innerWidth, innerHeight]])
      .nodeSort(null)

    const { nodes: sankeyNodes, links: sankeyLinkData } = sankeyGenerator({
      nodes: nodes.map(n => ({ ...n })),
      links: sankeyLinks,
    })

    const colorScale = (type: string) => {
      switch (type) {
        case 'exporter': return '#4ecca3'  // Green for exporters
        case 'importer': return '#FF5F1F'  // Orange for importers
        default: return '#3d2a1f'
      }
    }

    const link = g.append('g')
      .attr('class', 'sankey-links')
      .attr('fill', 'none')
      .selectAll('path')
      .data(sankeyLinkData)
      .join('path')
      .attr('class', 'sankey-link')
      .attr('d', sankeyLinkHorizontal())
      .attr('stroke', '#FF5F1F')
      .attr('stroke-opacity', 0.3)
      .attr('stroke-width', (d: any) => Math.max(1, d.width))
      .on('mouseenter', function() {
        d3.select(this).attr('stroke-opacity', 0.6)
      })
      .on('mouseleave', function() {
        d3.select(this).attr('stroke-opacity', 0.3)
      })

    const node = g.append('g')
      .attr('class', 'sankey-nodes')
      .selectAll('g')
      .data(sankeyNodes)
      .join('g')
      .attr('class', 'sankey-node')
      .attr('transform', (d: any) => `translate(${d.x0},${d.y0})`)
      .style('cursor', 'pointer')
      .on('mouseenter', function(_, d: any) {
        link.attr('stroke-opacity', (l: any) =>
          l.source === d || l.target === d ? 0.7 : 0.1
        )
        onNodeHover?.(d.id)
      })
      .on('mouseleave', function() {
        link.attr('stroke-opacity', 0.3)
        onNodeHover?.(null)
      })

    node.append('rect')
      .attr('width', (d: any) => d.x1 - d.x0)
      .attr('height', (d: any) => Math.max(1, d.y1 - d.y0))
      .attr('fill', (d: any) => colorScale(d.type))
      .attr('rx', 2)

    node.append('text')
      .attr('x', (d: any) => d.x0 < innerWidth / 2 ? (d.x1 - d.x0) + 6 : -6)
      .attr('y', (d: any) => (d.y1 - d.y0) / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', (d: any) => d.x0 < innerWidth / 2 ? 'start' : 'end')
      .attr('fill', '#eaeaea')
      .attr('font-size', '10px')
      .text((d: any) => d.name)

    g.append('g')
      .attr('class', 'link-labels')
      .selectAll('text')
      .data(sankeyLinkData.filter((l: any) => l.width > 10))
      .join('text')
      .attr('x', (d: any) => (d.source.x1 + d.target.x0) / 2)
      .attr('y', (d: any) => (d.y0 + d.y1) / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', 'middle')
      .attr('fill', '#a0a0a0')
      .attr('font-size', '8px')
      .text((d: any) => `$${(d.value / 1e9).toFixed(1)}B`)

  }, [nodes, links, onNodeHover])

  return (
    <div className="sankey-container">
      <svg ref={svgRef} />
      <div className="sankey-legend" style={{ marginTop: '0.5rem', fontSize: '0.625rem', color: '#a0a0a0' }}>
        <span style={{ color: '#4ecca3' }}>● Exporters</span>
        {' → '}
        <span style={{ color: '#FF5F1F' }}>● Importers</span>
      </div>
    </div>
  )
}

export default SankeyDiagram

import { FC, useRef, useEffect } from 'react'
import * as d3 from 'd3'

interface SeverityGaugeProps {
  value: number // 0-1
  label?: string
  size?: number
  showPercentage?: boolean
}

const SeverityGauge: FC<SeverityGaugeProps> = ({
  value,
  label = 'Severity',
  size = 120,
  showPercentage = true,
}) => {
  const svgRef = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!svgRef.current) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const width = size
    const height = size * 0.7
    const radius = Math.min(width, height * 2) / 2 - 10

    svg.attr('width', width).attr('height', height)

    const g = svg.append('g')
      .attr('transform', `translate(${width / 2}, ${height - 10})`)

    const backgroundArc = d3.arc()
      .innerRadius(radius - 15)
      .outerRadius(radius)
      .startAngle(-Math.PI / 2)
      .endAngle(Math.PI / 2)

    g.append('path')
      .attr('d', backgroundArc as any)
      .attr('fill', '#2a2a4a')

    const colorScale = d3.scaleLinear<string>()
      .domain([0, 0.4, 0.7, 1])
      .range(['#4ecca3', '#ffc93c', '#F89880', '#FF5F1F'])

    const valueArc = d3.arc()
      .innerRadius(radius - 15)
      .outerRadius(radius)
      .startAngle(-Math.PI / 2)
      .endAngle(-Math.PI / 2 + Math.PI * Math.min(value, 1))

    g.append('path')
      .attr('d', valueArc as any)
      .attr('fill', colorScale(value))

    if (showPercentage) {
      g.append('text')
        .attr('text-anchor', 'middle')
        .attr('dy', '-0.5em')
        .attr('fill', '#eaeaea')
        .attr('font-size', `${size / 5}px`)
        .attr('font-weight', '600')
        .text(`${Math.round(value * 100)}%`)
    }

    if (label) {
      g.append('text')
        .attr('text-anchor', 'middle')
        .attr('dy', showPercentage ? '1em' : '0')
        .attr('fill', '#a0a0a0')
        .attr('font-size', '10px')
        .text(label)
    }

  }, [value, label, size, showPercentage])

  return (
    <div className="severity-gauge">
      <svg ref={svgRef} />
    </div>
  )
}

export default SeverityGauge

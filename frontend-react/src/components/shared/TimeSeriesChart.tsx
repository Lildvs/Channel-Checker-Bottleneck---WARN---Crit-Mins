import { FC, useRef, useEffect } from 'react'
import * as d3 from 'd3'

interface DataPoint {
  date: Date | string
  value: number
  label?: string
}

interface TimeSeriesChartProps {
  data: DataPoint[]
  height?: number
  showArea?: boolean
  showMovingAverage?: boolean
  movingAverageWindow?: number
  xAxisLabel?: string
  yAxisLabel?: string
  color?: string
  secondaryData?: DataPoint[]
  secondaryColor?: string
  secondaryLabel?: string
}

const TimeSeriesChart: FC<TimeSeriesChartProps> = ({
  data,
  height = 300,
  showArea = true,
  showMovingAverage = false,
  movingAverageWindow = 20,
  xAxisLabel,
  yAxisLabel,
  color = '#1f77b4',
  secondaryData,
  secondaryColor = '#F89880',
  // secondaryLabel is reserved for future legend use
}) => {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!svgRef.current || !containerRef.current || data.length === 0) return

    const container = containerRef.current
    const width = container.clientWidth
    const margin = { top: 20, right: 50, bottom: 40, left: 60 }
    const innerWidth = width - margin.left - margin.right
    const innerHeight = height - margin.top - margin.bottom

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg.attr('width', width).attr('height', height)

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`)

    const parsedData = data.map(d => ({
      ...d,
      date: typeof d.date === 'string' ? new Date(d.date) : d.date,
    }))

    const xScale = d3.scaleTime()
      .domain(d3.extent(parsedData, d => d.date) as [Date, Date])
      .range([0, innerWidth])

    const yScale = d3.scaleLinear()
      .domain([0, d3.max(parsedData, d => d.value) || 0])
      .nice()
      .range([innerHeight, 0])

    g.append('g')
      .attr('transform', `translate(0,${innerHeight})`)
      .call(d3.axisBottom(xScale).ticks(6))
      .attr('color', '#a0a0a0')

    g.append('g')
      .call(d3.axisLeft(yScale).ticks(5))
      .attr('color', '#a0a0a0')

    if (xAxisLabel) {
      svg.append('text')
        .attr('x', width / 2)
        .attr('y', height - 5)
        .attr('text-anchor', 'middle')
        .attr('fill', '#a0a0a0')
        .attr('font-size', '12px')
        .text(xAxisLabel)
    }

    if (yAxisLabel) {
      svg.append('text')
        .attr('transform', 'rotate(-90)')
        .attr('x', -height / 2)
        .attr('y', 15)
        .attr('text-anchor', 'middle')
        .attr('fill', '#a0a0a0')
        .attr('font-size', '12px')
        .text(yAxisLabel)
    }

    if (showArea) {
      const area = d3.area<typeof parsedData[0]>()
        .x(d => xScale(d.date))
        .y0(innerHeight)
        .y1(d => yScale(d.value))
        .curve(d3.curveMonotoneX)

      g.append('path')
        .datum(parsedData)
        .attr('fill', color)
        .attr('fill-opacity', 0.2)
        .attr('d', area)
    }

    const line = d3.line<typeof parsedData[0]>()
      .x(d => xScale(d.date))
      .y(d => yScale(d.value))
      .curve(d3.curveMonotoneX)

    g.append('path')
      .datum(parsedData)
      .attr('fill', 'none')
      .attr('stroke', color)
      .attr('stroke-width', 2)
      .attr('d', line)

    if (showMovingAverage && parsedData.length >= movingAverageWindow) {
      const maData = parsedData.map((d, i) => {
        if (i < movingAverageWindow - 1) return null
        const slice = parsedData.slice(i - movingAverageWindow + 1, i + 1)
        const avg = d3.mean(slice, s => s.value) || 0
        return { date: d.date, value: avg }
      }).filter(Boolean) as typeof parsedData

      g.append('path')
        .datum(maData)
        .attr('fill', 'none')
        .attr('stroke', '#F89880')
        .attr('stroke-width', 1.5)
        .attr('stroke-dasharray', '4,4')
        .attr('d', line)
    }

    if (secondaryData) {
      const parsedSecondary = secondaryData.map(d => ({
        ...d,
        date: typeof d.date === 'string' ? new Date(d.date) : d.date,
      }))

      const y2Scale = d3.scaleLinear()
        .domain([0, d3.max(parsedSecondary, d => d.value) || 0])
        .nice()
        .range([innerHeight, 0])

      g.append('g')
        .attr('transform', `translate(${innerWidth},0)`)
        .call(d3.axisRight(y2Scale).ticks(5))
        .attr('color', secondaryColor)

      const barWidth = innerWidth / parsedSecondary.length * 0.6
      g.selectAll('.bar')
        .data(parsedSecondary)
        .join('rect')
        .attr('class', 'bar')
        .attr('x', d => xScale(d.date) - barWidth / 2)
        .attr('y', d => y2Scale(d.value))
        .attr('width', barWidth)
        .attr('height', d => innerHeight - y2Scale(d.value))
        .attr('fill', secondaryColor)
        .attr('fill-opacity', 0.5)
    }

  }, [data, height, showArea, showMovingAverage, movingAverageWindow, color, secondaryData, secondaryColor, xAxisLabel, yAxisLabel])

  return (
    <div ref={containerRef} className="time-series-chart" style={{ width: '100%' }}>
      <svg ref={svgRef} />
    </div>
  )
}

export default TimeSeriesChart

import { FC, useRef, useEffect } from 'react'
import * as d3 from 'd3'
import { TopicStats } from '../../api/research'

interface TopicDistributionProps {
  topics: TopicStats[]
  selectedTopic: string | null
  onTopicClick: (topic: string) => void
}

const TopicDistribution: FC<TopicDistributionProps> = ({
  topics,
  selectedTopic,
  onTopicClick,
}) => {
  const svgRef = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!svgRef.current || topics.length === 0) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const width = 360
    const height = topics.length * 28 + 20
    const margin = { top: 10, right: 10, bottom: 10, left: 100 }
    const chartWidth = width - margin.left - margin.right
    const chartHeight = height - margin.top - margin.bottom

    svg.attr('width', width).attr('height', height)

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`)

    const sortedTopics = [...topics].sort((a, b) => b.paper_count - a.paper_count).slice(0, 15)

    const xScale = d3.scaleLinear()
      .domain([0, d3.max(sortedTopics, d => d.paper_count) || 0])
      .range([0, chartWidth])

    const yScale = d3.scaleBand()
      .domain(sortedTopics.map(d => d.topic))
      .range([0, chartHeight])
      .padding(0.2)

    g.selectAll('.bar')
      .data(sortedTopics)
      .join('rect')
      .attr('class', 'bar')
      .attr('x', 0)
      .attr('y', d => yScale(d.topic) || 0)
      .attr('width', d => xScale(d.paper_count))
      .attr('height', yScale.bandwidth())
      .attr('fill', d => d.topic === selectedTopic ? '#FF5F1F' : '#3d2a1f')
      .attr('rx', 3)
      .style('cursor', 'pointer')
      .on('click', (_, d) => onTopicClick(d.topic))
      .on('mouseenter', function() {
        d3.select(this).attr('fill', '#FF5F1F')
      })
      .on('mouseleave', function(_, d) {
        d3.select(this).attr('fill', d.topic === selectedTopic ? '#FF5F1F' : '#3d2a1f')
      })

    g.selectAll('.label')
      .data(sortedTopics)
      .join('text')
      .attr('class', 'label')
      .attr('x', -5)
      .attr('y', d => (yScale(d.topic) || 0) + yScale.bandwidth() / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', 'end')
      .attr('fill', '#eaeaea')
      .attr('font-size', '11px')
      .text(d => d.topic.replace('_', ' '))
      .style('cursor', 'pointer')
      .on('click', (_, d) => onTopicClick(d.topic))

    g.selectAll('.count')
      .data(sortedTopics)
      .join('text')
      .attr('class', 'count')
      .attr('x', d => xScale(d.paper_count) + 5)
      .attr('y', d => (yScale(d.topic) || 0) + yScale.bandwidth() / 2)
      .attr('dy', '0.35em')
      .attr('fill', '#a0a0a0')
      .attr('font-size', '10px')
      .text(d => d.paper_count)

  }, [topics, selectedTopic, onTopicClick])

  return (
    <div className="topic-distribution">
      <svg ref={svgRef} />
    </div>
  )
}

export default TopicDistribution

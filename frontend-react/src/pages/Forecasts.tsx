import { FC, useState, useRef, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import * as d3 from 'd3'
import { fetchActiveBottlenecks, Bottleneck } from '../api/bottlenecks'
import {
  fetchDurationForecast,
  fetchTrajectoryForecast,
  fetchResearchReport,
  DurationForecast,
  TrajectoryForecast,
} from '../api/forecasts'
import { MetricCard, LoadingSpinner } from '../components/shared'

type Tab = 'duration' | 'trajectory' | 'research'

const Forecasts: FC = () => {
  const [activeTab, setActiveTab] = useState<Tab>('duration')
  const [selectedBottleneckId, setSelectedBottleneckId] = useState<string | null>(null)
  const [horizonDays, setHorizonDays] = useState(30)

  const { data: bottleneckData, isLoading: loadingBottlenecks } = useQuery({
    queryKey: ['activeBottlenecks'],
    queryFn: () => fetchActiveBottlenecks(),
  })

  const bottlenecks = bottleneckData?.bottlenecks || []

  const durationMutation = useMutation({
    mutationFn: (bottleneckId: string) => fetchDurationForecast(bottleneckId),
  })

  const trajectoryMutation = useMutation({
    mutationFn: ({ bottleneckId, days }: { bottleneckId: string; days: number }) =>
      fetchTrajectoryForecast(bottleneckId, days),
  })

  const researchMutation = useMutation({
    mutationFn: (bottleneckId: string) => fetchResearchReport(bottleneckId),
  })

  const handleSelectBottleneck = (bottleneck: Bottleneck) => {
    setSelectedBottleneckId(bottleneck.id)
  }

  const handleRunDuration = () => {
    if (selectedBottleneckId) {
      durationMutation.mutate(selectedBottleneckId)
    }
  }

  const handleRunTrajectory = () => {
    if (selectedBottleneckId) {
      trajectoryMutation.mutate({ bottleneckId: selectedBottleneckId, days: horizonDays })
    }
  }

  const handleRunResearch = () => {
    if (selectedBottleneckId) {
      researchMutation.mutate(selectedBottleneckId)
    }
  }

  if (loadingBottlenecks) {
    return <LoadingSpinner message="Loading bottlenecks..." />
  }

  const selectedBottleneck = bottlenecks.find(b => b.id === selectedBottleneckId)

  return (
    <div className="page forecasts-page">
      <header className="page-header">
        <h2>Bottleneck Forecasts</h2>
        <p>AI-powered predictions of bottleneck duration and trajectory</p>
      </header>

      <section className="forecast-selector">
        <h3>Select Bottleneck</h3>
        {bottlenecks.length === 0 ? (
          <div className="empty-state">No active bottlenecks to forecast.</div>
        ) : (
          <div className="bottleneck-select-grid">
            {bottlenecks.map((b) => (
              <div
                key={b.id}
                className={`bottleneck-select-card ${selectedBottleneckId === b.id ? 'selected' : ''}`}
                onClick={() => handleSelectBottleneck(b)}
              >
                <span className="category">{b.category.replace(/_/g, ' ')}</span>
                <span className="severity">{Math.round(b.severity * 100)}%</span>
              </div>
            ))}
          </div>
        )}
      </section>

      {selectedBottleneck && (
        <section className="forecast-detail">
          <h3>Selected: {selectedBottleneck.category.replace(/_/g, ' ')}</h3>
          <p>{selectedBottleneck.description}</p>
        </section>
      )}

      <nav className="forecast-tabs">
        <button
          className={`tab-btn ${activeTab === 'duration' ? 'active' : ''}`}
          onClick={() => setActiveTab('duration')}
        >
          Duration Forecast
        </button>
        <button
          className={`tab-btn ${activeTab === 'trajectory' ? 'active' : ''}`}
          onClick={() => setActiveTab('trajectory')}
        >
          Trajectory Forecast
        </button>
        <button
          className={`tab-btn ${activeTab === 'research' ? 'active' : ''}`}
          onClick={() => setActiveTab('research')}
        >
          Research Report
        </button>
      </nav>

      <div className="forecast-content">
        {activeTab === 'duration' && (
          <DurationTab
            selectedId={selectedBottleneckId}
            data={durationMutation.data}
            isLoading={durationMutation.isPending}
            error={durationMutation.error}
            onRun={handleRunDuration}
          />
        )}
        {activeTab === 'trajectory' && (
          <TrajectoryTab
            selectedId={selectedBottleneckId}
            data={trajectoryMutation.data}
            isLoading={trajectoryMutation.isPending}
            error={trajectoryMutation.error}
            horizonDays={horizonDays}
            setHorizonDays={setHorizonDays}
            onRun={handleRunTrajectory}
          />
        )}
        {activeTab === 'research' && (
          <ResearchTab
            selectedId={selectedBottleneckId}
            data={researchMutation.data}
            isLoading={researchMutation.isPending}
            error={researchMutation.error}
            onRun={handleRunResearch}
          />
        )}
      </div>
    </div>
  )
}

interface DurationTabProps {
  selectedId: string | null
  data?: DurationForecast
  isLoading: boolean
  error: Error | null
  onRun: () => void
}

const DurationTab: FC<DurationTabProps> = ({ selectedId, data, isLoading, error, onRun }) => {
  return (
    <div className="forecast-tab duration-tab">
      <div className="tab-actions">
        <button
          className="btn btn-primary"
          disabled={!selectedId || isLoading}
          onClick={onRun}
        >
          {isLoading ? 'Generating...' : 'Generate Duration Forecast'}
        </button>
      </div>

      {error && (
        <div className="error-message">Error: {String(error)}</div>
      )}

      {data && (
        <div className="duration-results">
          <div className="metrics-row">
            <MetricCard
              label="Expected Duration"
              value={`${data.expected_duration_days} days`}
            />
            <MetricCard
              label="Expected Resolution"
              value={new Date(data.expected_resolution_date).toLocaleDateString()}
            />
            <MetricCard
              label="Model Used"
              value={data.model_used}
            />
          </div>

          <div className="probability-chart">
            <h4>Persistence Probabilities</h4>
            <div className="prob-bars">
              <ProbabilityBar label="30 Days" value={data.probability_persists_30_days} />
              <ProbabilityBar label="60 Days" value={data.probability_persists_60_days} />
              <ProbabilityBar label="90 Days" value={data.probability_persists_90_days} />
            </div>
          </div>

          <div className="confidence-interval">
            <h4>Confidence Interval</h4>
            <p>
              Duration range: {data.confidence_lower_days} - {data.confidence_upper_days} days
            </p>
          </div>

          <div className="reasoning-section">
            <h4>Model Reasoning</h4>
            <p>{data.reasoning}</p>
          </div>
        </div>
      )}
    </div>
  )
}

interface ProbabilityBarProps {
  label: string
  value: number
}

const ProbabilityBar: FC<ProbabilityBarProps> = ({ label, value }) => {
  const percentage = Math.round(value * 100)
  const color = value >= 0.7 ? '#FF5F1F' : value >= 0.4 ? '#F89880' : '#4ecca3'

  return (
    <div className="prob-bar">
      <span className="prob-label">{label}</span>
      <div className="prob-track">
        <div className="prob-fill" style={{ width: `${percentage}%`, backgroundColor: color }} />
      </div>
      <span className="prob-value">{percentage}%</span>
    </div>
  )
}

interface TrajectoryTabProps {
  selectedId: string | null
  data?: TrajectoryForecast
  isLoading: boolean
  error: Error | null
  horizonDays: number
  setHorizonDays: (days: number) => void
  onRun: () => void
}

const TrajectoryTab: FC<TrajectoryTabProps> = ({
  selectedId,
  data,
  isLoading,
  error,
  horizonDays,
  setHorizonDays,
  onRun,
}) => {
  return (
    <div className="forecast-tab trajectory-tab">
      <div className="tab-actions">
        <div className="filter-group">
          <label>Forecast Horizon: {horizonDays} days</label>
          <input
            type="range"
            min={7}
            max={90}
            step={7}
            value={horizonDays}
            onChange={(e) => setHorizonDays(parseInt(e.target.value))}
          />
        </div>
        <button
          className="btn btn-primary"
          disabled={!selectedId || isLoading}
          onClick={onRun}
        >
          {isLoading ? 'Generating...' : 'Generate Trajectory Forecast'}
        </button>
      </div>

      {error && (
        <div className="error-message">Error: {String(error)}</div>
      )}

      {data && (
        <div className="trajectory-results">
          <div className="metrics-row">
            <MetricCard
              label="Expected Resolution Day"
              value={`Day ${data.expected_resolution_day}`}
            />
            <MetricCard
              label="Final Severity"
              value={`${Math.round(data.final_severity * 100)}%`}
            />
          </div>

          <TrajectoryChart trajectory={data.trajectory} />
        </div>
      )}
    </div>
  )
}

interface TrajectoryChartProps {
  trajectory: { day: number; date: string; severity: number; lower_bound: number; upper_bound: number }[]
}

const TrajectoryChart: FC<TrajectoryChartProps> = ({ trajectory }) => {
  const svgRef = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!svgRef.current || trajectory.length === 0) return

    const width = 600
    const height = 300
    const margin = { top: 20, right: 30, bottom: 40, left: 50 }
    const innerWidth = width - margin.left - margin.right
    const innerHeight = height - margin.top - margin.bottom

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg.attr('width', width).attr('height', height)

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`)

    const xScale = d3.scaleLinear()
      .domain([0, d3.max(trajectory, d => d.day) || 30])
      .range([0, innerWidth])

    const yScale = d3.scaleLinear()
      .domain([0, 1])
      .range([innerHeight, 0])

    g.append('g')
      .attr('transform', `translate(0,${innerHeight})`)
      .call(d3.axisBottom(xScale).ticks(6))
      .attr('color', '#a0a0a0')

    g.append('g')
      .call(d3.axisLeft(yScale).tickFormat(d => `${+d * 100}%`))
      .attr('color', '#a0a0a0')

    const area = d3.area<typeof trajectory[0]>()
      .x(d => xScale(d.day))
      .y0(d => yScale(d.lower_bound))
      .y1(d => yScale(d.upper_bound))

    g.append('path')
      .datum(trajectory)
      .attr('fill', '#1f77b4')
      .attr('fill-opacity', 0.2)
      .attr('d', area)

    const line = d3.line<typeof trajectory[0]>()
      .x(d => xScale(d.day))
      .y(d => yScale(d.severity))
      .curve(d3.curveMonotoneX)

    g.append('path')
      .datum(trajectory)
      .attr('fill', 'none')
      .attr('stroke', '#1f77b4')
      .attr('stroke-width', 2)
      .attr('d', line)

    g.selectAll('.point')
      .data(trajectory)
      .join('circle')
      .attr('class', 'point')
      .attr('cx', d => xScale(d.day))
      .attr('cy', d => yScale(d.severity))
      .attr('r', 3)
      .attr('fill', '#1f77b4')

    svg.append('text')
      .attr('x', width / 2)
      .attr('y', height - 5)
      .attr('text-anchor', 'middle')
      .attr('fill', '#a0a0a0')
      .attr('font-size', '12px')
      .text('Days')

    svg.append('text')
      .attr('transform', 'rotate(-90)')
      .attr('x', -height / 2)
      .attr('y', 15)
      .attr('text-anchor', 'middle')
      .attr('fill', '#a0a0a0')
      .attr('font-size', '12px')
      .text('Severity')

  }, [trajectory])

  return (
    <div className="trajectory-chart">
      <h4>Severity Trajectory</h4>
      <svg ref={svgRef} />
    </div>
  )
}

interface ResearchTabProps {
  selectedId: string | null
  data?: Awaited<ReturnType<typeof fetchResearchReport>>
  isLoading: boolean
  error: Error | null
  onRun: () => void
}

const ResearchTab: FC<ResearchTabProps> = ({ selectedId, data, isLoading, error, onRun }) => {
  return (
    <div className="forecast-tab research-tab">
      <div className="tab-actions">
        <button
          className="btn btn-primary"
          disabled={!selectedId || isLoading}
          onClick={onRun}
        >
          {isLoading ? 'Generating...' : 'Generate Research Report'}
        </button>
      </div>

      {error && (
        <div className="error-message">Error: {String(error)}</div>
      )}

      {data && (
        <div className="research-results">
          <div className="research-summary">
            <h4>Summary</h4>
            <p>{data.summary}</p>
          </div>

          <div className="research-findings">
            <h4>Key Findings</h4>
            <ul>
              {data.key_findings.map((finding, i) => (
                <li key={i}>{finding}</li>
              ))}
            </ul>
          </div>

          {data.historical_precedents.length > 0 && (
            <div className="research-precedents">
              <h4>Historical Precedents</h4>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Category</th>
                    <th>Severity</th>
                    <th>Duration</th>
                    <th>Resolution</th>
                    <th>Similarity</th>
                  </tr>
                </thead>
                <tbody>
                  {data.historical_precedents.map((p) => (
                    <tr key={p.id}>
                      <td>{p.category}</td>
                      <td>{Math.round(p.severity * 100)}%</td>
                      <td>{p.duration_days} days</td>
                      <td>{new Date(p.resolution_date).toLocaleDateString()}</td>
                      <td>{Math.round(p.similarity_score * 100)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <div className="research-sources">
            <h4>Data Sources</h4>
            <ul>
              {data.data_sources.map((source, i) => (
                <li key={i}>{source}</li>
              ))}
            </ul>
          </div>

          <div className="research-meta">
            <p><strong>Confidence Level:</strong> {data.confidence_level}</p>
            <p><strong>Generated:</strong> {new Date(data.generated_at).toLocaleString()}</p>
          </div>
        </div>
      )}
    </div>
  )
}

export default Forecasts

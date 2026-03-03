import { FC } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  fetchActiveBottlenecks,
  fetchSystemicRisk,
  fetchBottleneckTrend,
  Bottleneck,
} from '../api/bottlenecks'
import { MetricCard, TimeSeriesChart, LoadingSpinner } from '../components/shared'

const Dashboard: FC = () => {
  const { data: bottleneckData, isLoading: loadingBottlenecks } = useQuery({
    queryKey: ['activeBottlenecks'],
    queryFn: () => fetchActiveBottlenecks(),
  })

  const { data: riskData, isLoading: loadingRisk } = useQuery({
    queryKey: ['systemicRisk'],
    queryFn: fetchSystemicRisk,
  })

  const { data: trendData, isLoading: loadingTrend } = useQuery({
    queryKey: ['bottleneckTrend'],
    queryFn: fetchBottleneckTrend,
  })

  if (loadingBottlenecks || loadingRisk) {
    return <LoadingSpinner message="Loading dashboard..." />
  }

  const bottlenecks = bottleneckData?.bottlenecks || []
  const activeCount = bottleneckData?.active_count || 0
  const maxSeverity = bottlenecks.length > 0 ? Math.max(...bottlenecks.map(b => b.severity)) : 0
  const avgRisk = riskData?.average_risk || 0
  const highestRiskSector = riskData?.highest_risk_sector || 'N/A'
  const riskScores = riskData?.risk_scores || {}

  const getSeverityColor = (severity: number) => {
    if (severity >= 0.7) return '#FF5F1F'
    if (severity >= 0.4) return '#F89880'
    return '#4ecca3'
  }

  const getCategoryDisplay = (category: string) => {
    return category.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
  }

  const trendChartData = (trendData?.data || []).map(d => ({
    date: d.date,
    value: d.count,
  }))

  const categoryCounts: Record<string, number> = {}
  bottlenecks.forEach(b => {
    const cat = getCategoryDisplay(b.category)
    categoryCounts[cat] = (categoryCounts[cat] || 0) + 1
  })

  return (
    <div className="page dashboard-page">
      <header className="page-header">
        <h2>Dashboard</h2>
        <p>Real-time overview of economic bottlenecks and sector health</p>
      </header>

      <section className="dashboard-metrics">
        <MetricCard
          label="Active Bottlenecks"
          value={activeCount}
          color={activeCount > 3 ? 'danger' : activeCount > 0 ? 'warning' : 'success'}
        />
        <MetricCard
          label="Max Severity"
          value={`${Math.round(maxSeverity * 100)}%`}
          color={maxSeverity >= 0.7 ? 'danger' : maxSeverity >= 0.4 ? 'warning' : 'success'}
        />
        <MetricCard
          label="Avg Systemic Risk"
          value={`${Math.round(avgRisk * 100)}%`}
          color={avgRisk >= 0.5 ? 'danger' : avgRisk >= 0.3 ? 'warning' : 'success'}
        />
        <MetricCard
          label="Highest Risk Sector"
          value={highestRiskSector}
        />
      </section>

      <div className="dashboard-grid">
        <section className="dashboard-bottlenecks">
          <h3>Active Bottlenecks</h3>
          {bottlenecks.length === 0 ? (
            <div className="empty-state">
              No active bottlenecks detected. The economy appears to be running smoothly.
            </div>
          ) : (
            <div className="bottleneck-cards">
              {bottlenecks.map((bottleneck: Bottleneck) => (
                <BottleneckCard key={bottleneck.id} bottleneck={bottleneck} />
              ))}
            </div>
          )}
        </section>

        <section className="dashboard-risk">
          <h3>Sector Risk Scores</h3>
          <div className="risk-bars">
            {Object.entries(riskScores)
              .sort(([, a], [, b]) => b - a)
              .map(([sector, score]) => (
                <div key={sector} className="risk-bar">
                  <span className="risk-bar__label">{sector}</span>
                  <div className="risk-bar__track">
                    <div
                      className="risk-bar__fill"
                      style={{
                        width: `${score * 100}%`,
                        backgroundColor: getSeverityColor(score),
                      }}
                    />
                  </div>
                  <span className="risk-bar__value">{Math.round(score * 100)}%</span>
                </div>
              ))}
          </div>
        </section>
      </div>

      <div className="dashboard-charts">
        <section className="dashboard-trend">
          <h3>Bottleneck Trend (Last 30 Days)</h3>
          {loadingTrend ? (
            <LoadingSpinner size="small" />
          ) : trendChartData.length > 0 ? (
            <TimeSeriesChart
              data={trendChartData}
              height={250}
              yAxisLabel="Active Bottlenecks"
              showArea
            />
          ) : (
            <div className="empty-state">No trend data available</div>
          )}
        </section>

        <section className="dashboard-categories">
          <h3>Bottlenecks by Category</h3>
          {Object.keys(categoryCounts).length > 0 ? (
            <div className="category-list">
              {Object.entries(categoryCounts).map(([category, count]) => (
                <div key={category} className="category-item">
                  <span className="category-item__name">{category}</span>
                  <span className="category-item__count">{count}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="empty-state">No bottlenecks to categorize</div>
          )}
        </section>
      </div>
    </div>
  )
}

interface BottleneckCardProps {
  bottleneck: Bottleneck
}

const BottleneckCard: FC<BottleneckCardProps> = ({ bottleneck }) => {
  const severity = bottleneck.severity
  const borderColor = severity >= 0.7 ? '#FF5F1F' : severity >= 0.4 ? '#F89880' : '#4ecca3'
  const bgColor = severity >= 0.7 ? 'rgba(233, 69, 96, 0.1)' : severity >= 0.4 ? 'rgba(255, 127, 14, 0.1)' : 'rgba(78, 204, 163, 0.1)'

  const categoryDisplay = bottleneck.category.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
  const sectorsDisplay = bottleneck.affected_sectors.slice(0, 3).join(', ')

  return (
    <div
      className="bottleneck-card"
      style={{
        borderLeft: `4px solid ${borderColor}`,
        background: bgColor,
      }}
    >
      <div className="bottleneck-card__content">
        <h4>{categoryDisplay}</h4>
        <p className="bottleneck-card__description">{bottleneck.description}</p>
        <p className="bottleneck-card__sectors">Affects: {sectorsDisplay}</p>
      </div>
      <div className="bottleneck-card__severity">
        <span className="severity-value" style={{ color: borderColor }}>
          {Math.round(severity * 100)}%
        </span>
        <span className="severity-label">Severity</span>
      </div>
    </div>
  )
}

export default Dashboard

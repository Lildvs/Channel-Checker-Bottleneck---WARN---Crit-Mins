import { FC, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { fetchActiveBottlenecks, fetchSystemicRisk } from '../api/bottlenecks'
import { fetchWarnStats } from '../api/warn'
import { MetricCard } from '../components/shared'

function currentMonthRange(): { start: string; end: string; label: string } {
  const now = new Date()
  const y = now.getFullYear()
  const m = now.getMonth()
  const start = new Date(y, m, 1)
  const end = new Date(y, m + 1, 0)
  const label = start.toLocaleString('default', { month: 'long' })
  const fmt = (d: Date) => d.toISOString().slice(0, 10)
  return { start: fmt(start), end: fmt(end), label }
}

const Home: FC = () => {
  const { data: bottleneckData } = useQuery({
    queryKey: ['activeBottlenecks'],
    queryFn: () => fetchActiveBottlenecks(),
  })

  const { data: riskData } = useQuery({
    queryKey: ['systemicRisk'],
    queryFn: fetchSystemicRisk,
  })

  const monthRange = useMemo(() => currentMonthRange(), [])

  const { data: warnData } = useQuery({
    queryKey: ['warnMonthly', monthRange.start],
    queryFn: () => fetchWarnStats({ start_date: monthRange.start, end_date: monthRange.end }),
  })

  const activeCount = bottleneckData?.active_count || 0
  const sectorsCount = Object.keys(riskData?.risk_scores || {}).length
  const avgRisk = riskData?.average_risk || 0
  const warnCount = warnData?.total_notices ?? 0

  const navigationCards = [
    {
      title: 'Dashboard',
      description: 'Real-time overview of bottlenecks and sector health',
      path: '/dashboard',
      icon: '📈',
    },
    {
      title: 'Bottlenecks',
      description: 'Detailed analysis of detected bottlenecks',
      path: '/bottlenecks',
      icon: '🚧',
    },
    {
      title: 'Sectors',
      description: 'Sector dependencies and risk visualization',
      path: '/sectors',
      icon: '🏭',
    },
    {
      title: 'Data Explorer',
      description: 'Browse and analyze raw time series data',
      path: '/data',
      icon: '📊',
    },
    {
      title: 'Forecasts',
      description: 'Bottleneck duration and trajectory predictions',
      path: '/forecasts',
      icon: '🔮',
    },
    {
      title: 'WARN Notices',
      description: 'Layoff trends and geographic analysis',
      path: '/warn',
      icon: '⚠️',
    },
    {
      title: 'Research Papers',
      description: 'Academic research and contrarian signals',
      path: '/research',
      icon: '📚',
    },
    {
      title: 'Commodity Flows',
      description: 'Trade flow maps and supply chain visualization',
      path: '/flows',
      icon: '🌍',
    },
  ]

  const { data: dataSourcesResponse } = useQuery({
    queryKey: ['dataSources'],
    queryFn: async () => {
      try {
        const response = await fetch('/api/data/sources')
        if (!response.ok) return []
        const data = await response.json()
        // Transform: backend returns {id, name, type}
        // Frontend needs {name, description, frequency}
        return data.map((s: { id: string; name: string; type: string }) => ({
          name: s.id.toUpperCase(),
          description: s.name,
          frequency: s.type === 'government' ? 'Varies' : 'Real-time',
        }))
      } catch {
        console.warn('Failed to fetch data sources')
        return []
      }
    },
  })

  const { data: sectorsResponse } = useQuery({
    queryKey: ['sectorsHome'],  // Different key to avoid conflict with other sector queries
    queryFn: async () => {
      try {
        const response = await fetch('/api/sectors/')
        if (!response.ok) return []
        const data = await response.json()
        // Transform: backend returns SectorInfo with key_indicators array
        // Frontend needs {name, indicators} where indicators is a string
        return data.map((s: { name: string; key_indicators: string[] }) => ({
          name: s.name,
          indicators: s.key_indicators.join(', '),
        }))
      } catch {
        console.warn('Failed to fetch sectors')
        return []
      }
    },
  })

  const dataSources = dataSourcesResponse || []
  const sectors = sectorsResponse || []

  return (
    <div className="page home-page">
      <header className="home-header">
        <h1>Channel Check Researcher</h1>
        <p>Bottom-up fundamental analysis for detecting economic bottlenecks</p>
      </header>

      <section className="home-metrics">
        <Link to="/bottlenecks" className="metric-link">
          <MetricCard
            label="Active Bottlenecks"
            value={activeCount}
            color={activeCount > 3 ? 'danger' : activeCount > 0 ? 'warning' : 'success'}
          />
        </Link>
        <Link to="/sectors" className="metric-link">
          <MetricCard
            label="Sectors Tracked"
            value={sectorsCount}
          />
        </Link>
        <Link to="/dashboard" className="metric-link">
          <MetricCard
            label="Avg Systemic Risk"
            value={`${Math.round(avgRisk * 100)}%`}
            color={avgRisk > 0.5 ? 'danger' : avgRisk > 0.3 ? 'warning' : 'success'}
          />
        </Link>
        <Link to="/reports" className="metric-link">
          <MetricCard
            label="System Status"
            value="Online"
            color="success"
          />
        </Link>
        <Link to="/warn" className="metric-link">
          <MetricCard
            label={`WARN Notices (${monthRange.label})`}
            value={warnCount}
            color={warnCount > 50 ? 'danger' : warnCount > 20 ? 'warning' : 'default'}
          />
        </Link>
      </section>

      <section className="home-navigation">
        <h2>Quick Navigation</h2>
        <div className="nav-cards">
          {navigationCards.map((card) => (
            <Link to={card.path} key={card.path} className="nav-card">
              <span className="nav-card__icon">{card.icon}</span>
              <h3>{card.title}</h3>
              <p>{card.description}</p>
            </Link>
          ))}
        </div>
      </section>

      <section className="home-info">
        <div className="info-panel">
          <h2>Tracked Sectors</h2>
          <table className="info-table">
            <thead>
              <tr>
                <th>Sector</th>
                <th>Key Indicators</th>
              </tr>
            </thead>
            <tbody>
              {sectors.map((sector: { name: string; indicators: string }) => (
                <tr key={sector.name}>
                  <td>{sector.name}</td>
                  <td>{sector.indicators}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="info-panel">
          <h2>Data Sources</h2>
          <table className="info-table">
            <thead>
              <tr>
                <th>Source</th>
                <th>Description</th>
                <th>Frequency</th>
              </tr>
            </thead>
            <tbody>
              {dataSources.map((source: { name: string; description: string; frequency: string }) => (
                <tr key={source.name}>
                  <td><strong>{source.name}</strong></td>
                  <td>{source.description}</td>
                  <td>{source.frequency}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}

export default Home

import { FC, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchActiveBottlenecks,
  collectAndDetect,
  Bottleneck,
} from '../api/bottlenecks'
import { MetricCard, SeverityGauge, LoadingSpinner } from '../components/shared'

const CATEGORIES = [
  'All',
  'Inventory Squeeze',
  'Price Spike',
  'Shipping Congestion',
  'Labor Tightness',
  'Capacity Ceiling',
  'Demand Surge',
  'Energy Crunch',
]

const Bottlenecks: FC = () => {
  const queryClient = useQueryClient()
  const [categoryFilter, setCategoryFilter] = useState('All')
  const [severityFilter, setSeverityFilter] = useState(0)
  const [statusFilter, setStatusFilter] = useState('Active')
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [lookbackDays, setLookbackDays] = useState(90)

  const { data, isLoading, error } = useQuery({
    queryKey: ['bottlenecks', categoryFilter, severityFilter],
    queryFn: () => fetchActiveBottlenecks({
      category: categoryFilter !== 'All' ? categoryFilter.toLowerCase().replace(/ /g, '_') : undefined,
      min_severity: severityFilter,
    }),
  })

  const detectionMutation = useMutation({
    mutationFn: (days: number) => collectAndDetect(days),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['bottlenecks'] })
    },
  })

  if (isLoading) {
    return <LoadingSpinner message="Loading bottlenecks..." />
  }

  if (error) {
    return (
      <div className="page error-page">
        <h2>Error Loading Bottlenecks</h2>
        <p>{String(error)}</p>
      </div>
    )
  }

  const bottlenecks = data?.bottlenecks || []
  const criticalCount = bottlenecks.filter(b => b.severity >= 0.7).length
  const avgSeverity = bottlenecks.length > 0
    ? bottlenecks.reduce((sum, b) => sum + b.severity, 0) / bottlenecks.length
    : 0
  const allSectors = new Set<string>()
  bottlenecks.forEach(b => b.affected_sectors.forEach(s => allSectors.add(s)))

  const handleDetection = () => {
    detectionMutation.mutate(lookbackDays)
  }

  const toggleExpand = (id: string) => {
    setExpandedId(expandedId === id ? null : id)
  }

  return (
    <div className="page bottlenecks-page">
      <header className="page-header">
        <h2>Bottleneck Analysis</h2>
        <p>Detailed analysis of detected economic bottlenecks and their impacts</p>
      </header>

      <section className="bottlenecks-filters">
        <div className="filter-group">
          <label>Category</label>
          <select
            value={categoryFilter}
            onChange={(e) => setCategoryFilter(e.target.value)}
          >
            {CATEGORIES.map((cat) => (
              <option key={cat} value={cat}>{cat}</option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <label>Minimum Severity: {Math.round(severityFilter * 100)}%</label>
          <input
            type="range"
            min={0}
            max={1}
            step={0.1}
            value={severityFilter}
            onChange={(e) => setSeverityFilter(parseFloat(e.target.value))}
          />
        </div>

        <div className="filter-group">
          <label>Status</label>
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            <option value="Active">Active</option>
            <option value="All">All</option>
            <option value="Resolved">Resolved</option>
          </select>
        </div>
      </section>

      <section className="bottlenecks-summary">
        <MetricCard label="Total Detected" value={bottlenecks.length} />
        <MetricCard
          label="Critical (≥70%)"
          value={criticalCount}
          color={criticalCount > 0 ? 'danger' : 'success'}
        />
        <MetricCard
          label="Avg Severity"
          value={`${Math.round(avgSeverity * 100)}%`}
        />
        <MetricCard label="Sectors Affected" value={allSectors.size} />
      </section>

      {bottlenecks.length > 0 && (
        <section className="bottlenecks-preview-section">
          <h3>Active Bottleneck Overview</h3>
          <div className="bottleneck-preview-grid">
            {bottlenecks.slice(0, 6).map((b) => {
              const catDisplay = b.category.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
              const sevPct = Math.round(b.severity * 100)
              const sevClass = b.severity >= 0.7 ? 'sev-critical' : b.severity >= 0.4 ? 'sev-warning' : 'sev-low'
              return (
                <div
                  key={b.id}
                  className={`bottleneck-preview-card ${sevClass}`}
                  onClick={() => toggleExpand(b.id)}
                  role="button"
                  tabIndex={0}
                  onKeyDown={(e) => { if (e.key === 'Enter') toggleExpand(b.id) }}
                >
                  <div className="bpc-top">
                    <span className="bpc-category">{catDisplay}</span>
                    <span className={`bpc-severity ${sevClass}`}>{sevPct}%</span>
                  </div>
                  <p className="bpc-description">{b.description}</p>
                  <div className="bpc-footer">
                    <span className="bpc-sectors">{b.affected_sectors.slice(0, 3).join(', ')}</span>
                    <span className="bpc-action">Expand Details {expandedId === b.id ? '▼' : '▶'}</span>
                  </div>
                </div>
              )
            })}
          </div>
        </section>
      )}

      <section className="bottlenecks-list">
        <h3>Detected Bottlenecks</h3>
        {bottlenecks.length === 0 ? (
          <div className="empty-state">No bottlenecks match the current filters.</div>
        ) : (
          <div className="bottleneck-details-list">
            {bottlenecks.map((bottleneck) => (
              <BottleneckDetailCard
                key={bottleneck.id}
                bottleneck={bottleneck}
                isExpanded={expandedId === bottleneck.id}
                onToggle={() => toggleExpand(bottleneck.id)}
              />
            ))}
          </div>
        )}
      </section>

      <section className="bottlenecks-detection">
        <h3>Run Detection</h3>
        <p>Collect fresh data from all sources, then run bottleneck detection.</p>
        <div className="detection-controls">
          <div className="filter-group">
            <label>Lookback Period: {lookbackDays} days</label>
            <input
              type="range"
              min={30}
              max={180}
              step={15}
              value={lookbackDays}
              onChange={(e) => setLookbackDays(parseInt(e.target.value))}
            />
          </div>
          <button
            className="btn btn-primary"
            onClick={handleDetection}
            disabled={detectionMutation.isPending}
          >
            {detectionMutation.isPending ? 'Collecting & Detecting...' : 'Run Detection Now'}
          </button>
        </div>
        {detectionMutation.isSuccess && (
          <div className="detection-result success">
            Collected {detectionMutation.data?.collection.total_records?.toLocaleString() || 0} records
            ({detectionMutation.data?.collection.succeeded}/{detectionMutation.data?.collection.total_collectors} collectors).
            Detection found {detectionMutation.data?.detection.total || 0} bottlenecks.
          </div>
        )}
        {detectionMutation.isError && (
          <div className="detection-result error">
            Detection failed: {String(detectionMutation.error)}
          </div>
        )}
      </section>
    </div>
  )
}

interface BottleneckDetailCardProps {
  bottleneck: Bottleneck
  isExpanded: boolean
  onToggle: () => void
}

const BottleneckDetailCard: FC<BottleneckDetailCardProps> = ({
  bottleneck,
  isExpanded,
  onToggle,
}) => {
  const categoryDisplay = bottleneck.category.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())

  return (
    <div className="bottleneck-detail-card">
      <div className="bottleneck-detail-card__header" onClick={onToggle}>
        <div className="header-left">
          <h4>{categoryDisplay}</h4>
          <span className="severity-badge">Severity: {Math.round(bottleneck.severity * 100)}%</span>
        </div>
        <span className="expand-icon">{isExpanded ? '▼' : '▶'}</span>
      </div>

      {isExpanded && (
        <div className="bottleneck-detail-card__content">
          <div className="detail-grid">
            <div className="detail-info">
              <p><strong>Description:</strong> {bottleneck.description}</p>
              <p><strong>Detected:</strong> {new Date(bottleneck.detected_at).toLocaleDateString()}</p>
              <p><strong>Confidence:</strong> {Math.round(bottleneck.confidence * 100)}%</p>
              <p><strong>Status:</strong> {bottleneck.status}</p>

              {bottleneck.affected_sectors.length > 0 && (
                <p><strong>Affected Sectors:</strong> {bottleneck.affected_sectors.join(', ')}</p>
              )}

              {bottleneck.affected_commodities && bottleneck.affected_commodities.length > 0 && (
                <p><strong>Affected Commodities:</strong> {bottleneck.affected_commodities.join(', ')}</p>
              )}

              {bottleneck.source_series && bottleneck.source_series.length > 0 && (
                <p><strong>Data Sources:</strong> {bottleneck.source_series.join(', ')}</p>
              )}

              {bottleneck.evidence && Object.keys(bottleneck.evidence).length > 0 && (
                <div className="evidence-section">
                  <strong>Evidence:</strong>
                  <ul>
                    {Object.entries(bottleneck.evidence).map(([key, value]) => (
                      <li key={key}>
                        {key.replace(/_/g, ' ')}:{' '}
                        {typeof value === 'number'
                          ? value.toFixed(3)
                          : typeof value === 'boolean'
                            ? String(value)
                            : typeof value === 'object' && value !== null
                              ? JSON.stringify(value)
                              : String(value ?? '')}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>

            <div className="detail-gauge">
              <SeverityGauge
                value={bottleneck.severity}
                label="Severity"
                size={140}
              />
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default Bottlenecks

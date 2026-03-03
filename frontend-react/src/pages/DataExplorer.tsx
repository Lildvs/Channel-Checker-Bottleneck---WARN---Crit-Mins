import { FC, useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  fetchSeriesData,
  fetchSeriesStats,
  fetchCollectorStatus,
  getSeriesCatalog,
  getAllSeries,
} from '../api/data'
import { MetricCard, TimeSeriesChart, LoadingSpinner } from '../components/shared'

const DataExplorer: FC = () => {
  const [selectedSeries, setSelectedSeries] = useState('DCOILWTICO')
  const [categoryFilter, setCategoryFilter] = useState('All')
  const [sourceFilter, setSourceFilter] = useState('All')
  const [dateRange, setDateRange] = useState(730) // days (0 = all data)

  const catalog = useMemo(() => getSeriesCatalog(), [])
  const allSeries = useMemo(() => getAllSeries(), [])
  const categories = useMemo(() => ['All', ...Object.keys(catalog)], [catalog])
  const sources = useMemo(
    () => ['All', ...Array.from(new Set(allSeries.map(s => s.source)))],
    [allSeries],
  )

  const filteredSeries = useMemo(() => {
    return allSeries.filter(s => {
      if (categoryFilter !== 'All' && s.category !== categoryFilter) return false
      if (sourceFilter !== 'All' && s.source !== sourceFilter) return false
      return true
    })
  }, [allSeries, categoryFilter, sourceFilter])

  const { startISO, endISO } = useMemo(() => {
    const end = new Date()
    if (dateRange === 0) return { startISO: undefined, endISO: end.toISOString() }
    const start = new Date()
    start.setDate(start.getDate() - dateRange)
    return { startISO: start.toISOString(), endISO: end.toISOString() }
  }, [dateRange])

  const {
    data: seriesData,
    isLoading: loadingSeries,
    isError: seriesError,
    error: seriesErrorDetail,
  } = useQuery({
    queryKey: ['seriesData', selectedSeries, startISO, endISO],
    queryFn: () => fetchSeriesData(selectedSeries, startISO, endISO),
  })

  const {
    data: stats,
    isLoading: loadingStats,
    isError: statsError,
  } = useQuery({
    queryKey: ['seriesStats', selectedSeries, dateRange],
    queryFn: () => fetchSeriesStats(selectedSeries, dateRange || 7300),
  })

  const { data: collectorStatus } = useQuery({
    queryKey: ['collectorStatus'],
    queryFn: fetchCollectorStatus,
    staleTime: 60000,
  })

  const selectedSeriesInfo = allSeries.find(s => s.id === selectedSeries)

  const chartData = (seriesData?.data_points || []).map(d => ({
    date: d.timestamp,
    value: d.value,
  }))

  const handleDownloadCSV = () => {
    if (!seriesData?.data_points) return

    const headers = ['timestamp', 'value', 'quality_score', 'is_preliminary']
    const rows = seriesData.data_points.map(d =>
      [d.timestamp, d.value, d.quality_score || '', d.is_preliminary || ''].join(',')
    )
    const csv = [headers.join(','), ...rows].join('\n')

    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${selectedSeries}_${startISO?.split('T')[0] ?? 'all'}_${endISO.split('T')[0]}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="page data-explorer-page">
      <header className="page-header">
        <h2>Data Explorer</h2>
        <p>Browse and analyze raw time series data from multiple sources</p>
      </header>

      <div className="data-explorer-layout">
        <aside className="series-sidebar">
          <h3>Series Catalog</h3>

          <div className="sidebar-filters">
            <div className="filter-group">
              <label>Source</label>
              <select
                value={sourceFilter}
                onChange={(e) => setSourceFilter(e.target.value)}
              >
                {sources.map(s => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            </div>

            <div className="filter-group">
              <label>Category</label>
              <select
                value={categoryFilter}
                onChange={(e) => setCategoryFilter(e.target.value)}
              >
                {categories.map(c => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
          </div>

          <div className="series-list">
            {filteredSeries.map((series) => (
              <div
                key={series.id}
                className={`series-item ${selectedSeries === series.id ? 'series-item--selected' : ''}`}
                onClick={() => setSelectedSeries(series.id)}
              >
                <span className="series-item__id">{series.id}</span>
                <span className="series-item__name">{series.name}</span>
                <span className="series-item__freq">{series.frequency}</span>
              </div>
            ))}
          </div>
        </aside>

        <main className="data-main">
          <section className="data-controls">
            <div className="filter-group">
              <label>Date Range</label>
              <select
                value={dateRange}
                onChange={(e) => setDateRange(parseInt(e.target.value))}
              >
                <option value={90}>Last 90 days</option>
                <option value={180}>Last 6 months</option>
                <option value={365}>Last 1 year</option>
                <option value={730}>Last 2 years</option>
                <option value={1825}>Last 5 years</option>
                <option value={3650}>Last 10 years</option>
                <option value={0}>All data</option>
              </select>
            </div>

            <button className="btn btn-secondary" onClick={handleDownloadCSV}>
              Download CSV
            </button>
          </section>

          <section className="data-chart">
            <h3>{selectedSeriesInfo?.name || selectedSeries}</h3>
            {loadingSeries ? (
              <LoadingSpinner size="small" />
            ) : seriesError ? (
              <div className="empty-state empty-state--error">
                No data collected yet for <strong>{selectedSeries}</strong>.
                {seriesErrorDetail && ' Run the collector or adjust the date range.'}
              </div>
            ) : chartData.length > 0 ? (
              <TimeSeriesChart
                data={chartData}
                height={350}
                showArea
                showMovingAverage
                movingAverageWindow={20}
                yAxisLabel="Value"
              />
            ) : (
              <div className="empty-state">No data available for this series and date range.</div>
            )}
          </section>

          <section className="data-stats">
            <h3>Statistics</h3>
            {loadingStats ? (
              <LoadingSpinner size="small" />
            ) : statsError ? (
              <div className="empty-state">No statistics collected yet for this series.</div>
            ) : stats && stats.count > 0 ? (
              <div className="stats-grid">
                <MetricCard label="Data Points" value={stats.count} />
                <MetricCard label="Mean" value={stats.mean?.toFixed(2) ?? '-'} />
                <MetricCard label="Std Dev" value={stats.stddev?.toFixed(2) ?? '-'} />
                <MetricCard label="Min" value={stats.min?.toFixed(2) ?? '-'} />
                <MetricCard label="Max" value={stats.max?.toFixed(2) ?? '-'} />
                <MetricCard label="Median" value={stats.median?.toFixed(2) ?? '-'} />
              </div>
            ) : (
              <div className="empty-state">No statistics available.</div>
            )}
          </section>

          {seriesData?.data_points && seriesData.data_points.length > 0 && (
            <section className="data-table-section">
              <h3>Recent Data Points</h3>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Timestamp</th>
                    <th>Value</th>
                    <th>Quality</th>
                    <th>Preliminary</th>
                  </tr>
                </thead>
                <tbody>
                  {seriesData.data_points.slice(0, 20).map((point, i) => (
                    <tr key={i}>
                      <td>{new Date(point.timestamp).toLocaleString()}</td>
                      <td>{point.value.toFixed(4)}</td>
                      <td>{point.quality_score?.toFixed(2) || '-'}</td>
                      <td>{point.is_preliminary ? 'Yes' : 'No'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}
        </main>
      </div>

      <section className="collector-status">
        <h3>Data Collection Status</h3>
        <div className="collector-grid">
          {(collectorStatus || []).map((collector) => (
            <div key={collector.name} className="collector-card">
              <span className={`collector-status-icon ${collector.status === 'scheduled' ? 'status-ok' : 'status-warn'}`}>
                {collector.status === 'scheduled' ? '●' : '○'}
              </span>
              <span className="collector-name">{collector.name}</span>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}

export default DataExplorer

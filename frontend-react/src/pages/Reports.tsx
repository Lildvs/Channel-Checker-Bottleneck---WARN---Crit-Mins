import { FC, useState, useCallback, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  fetchSystemLogs,
  fetchLogStats,
  fetchCollectionJobs,
  LogEntry,
} from '../api/reports'
import { MetricCard, LoadingSpinner, DataTable } from '../components/shared'

type TabId = 'logs' | 'jobs'
type LogLevel = '' | 'CRITICAL' | 'ERROR' | 'WARNING' | 'INFO'

const LEVEL_COLORS: Record<string, string> = {
  CRITICAL: '#ff4444',
  ERROR: '#ff6b6b',
  WARNING: '#ffa726',
  INFO: '#42a5f5',
}

const LEVEL_BADGES: Record<string, string> = {
  CRITICAL: 'badge-critical',
  ERROR: 'badge-error',
  WARNING: 'badge-warning',
  INFO: 'badge-info',
}

const STATUS_COLORS: Record<string, string> = {
  completed: '#4caf50',
  running: '#2196f3',
  failed: '#f44336',
}

const Reports: FC = () => {
  const [activeTab, setActiveTab] = useState<TabId>('logs')
  const [logPage, setLogPage] = useState(1)
  const [jobPage, setJobPage] = useState(1)
  const [levelFilter, setLevelFilter] = useState<LogLevel>('')
  const [sourceFilter, setSourceFilter] = useState('')
  const [searchFilter, setSearchFilter] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [jobStatusFilter, setJobStatusFilter] = useState('')
  const [expandedLogId, setExpandedLogId] = useState<number | null>(null)
  const pageSize = 50

  const { data: stats, isLoading: loadingStats } = useQuery({
    queryKey: ['logStats'],
    queryFn: fetchLogStats,
    refetchInterval: 30000,
  })

  const { data: logsData, isLoading: loadingLogs } = useQuery({
    queryKey: ['systemLogs', logPage, levelFilter, sourceFilter, searchFilter],
    queryFn: () => fetchSystemLogs({
      page: logPage,
      page_size: pageSize,
      level: levelFilter || undefined,
      source: sourceFilter || undefined,
      search: searchFilter || undefined,
    }),
    refetchInterval: 15000,
  })

  const { data: jobsData, isLoading: loadingJobs } = useQuery({
    queryKey: ['collectionJobs', jobPage, jobStatusFilter],
    queryFn: () => fetchCollectionJobs({
      page: jobPage,
      page_size: pageSize,
      status: jobStatusFilter || undefined,
    }),
    refetchInterval: 15000,
  })

  const handleSearch = useCallback(() => {
    setSearchFilter(searchInput)
    setLogPage(1)
  }, [searchInput])

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleSearch()
  }, [handleSearch])

  const formatTimestamp = (ts: string) => {
    const d = new Date(ts)
    return d.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    })
  }

  const formatDuration = (seconds: number | null) => {
    if (seconds === null) return '-'
    if (seconds < 60) return `${seconds.toFixed(1)}s`
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`
  }

  const logColumns = useMemo(() => [
    {
      key: 'level',
      header: 'Level',
      width: '8%',
      render: (value: unknown) => {
        const level = value as string
        return (
          <span
            className={`log-badge ${LEVEL_BADGES[level] || ''}`}
            style={{ color: LEVEL_COLORS[level] || '#ccc' }}
          >
            {level}
          </span>
        )
      },
    },
    {
      key: 'timestamp',
      header: 'Time',
      width: '14%',
      render: (value: unknown) => formatTimestamp(value as string),
    },
    {
      key: 'event',
      header: 'Event',
      width: '45%',
      render: (value: unknown, row: Record<string, unknown>) => {
        const entry = row as unknown as LogEntry
        const isExpanded = expandedLogId === entry.id
        return (
          <div className="log-event-cell">
            <span
              className="log-event-text"
              onClick={() => setExpandedLogId(isExpanded ? null : entry.id)}
              style={{ cursor: 'pointer' }}
            >
              {value as string}
            </span>
            {isExpanded && entry.extra_data && Object.keys(entry.extra_data).length > 0 && (
              <pre className="log-extra-data">
                {JSON.stringify(entry.extra_data, null, 2)}
              </pre>
            )}
          </div>
        )
      },
    },
    {
      key: 'source_module',
      header: 'Source',
      width: '18%',
      render: (value: unknown) => (
        <span className="log-source">{(value as string) || '-'}</span>
      ),
    },
  ], [expandedLogId])

  const jobColumns = useMemo(() => [
    {
      key: 'status',
      header: 'Status',
      width: '8%',
      render: (value: unknown) => {
        const status = value as string
        return (
          <span
            className="job-status-badge"
            style={{ color: STATUS_COLORS[status] || '#ccc' }}
          >
            {status}
          </span>
        )
      },
    },
    { key: 'collector_name', header: 'Collector', width: '15%' },
    {
      key: 'started_at',
      header: 'Started',
      width: '15%',
      render: (value: unknown) => formatTimestamp(value as string),
    },
    {
      key: 'duration_seconds',
      header: 'Duration',
      width: '10%',
      render: (value: unknown) => formatDuration(value as number | null),
    },
    {
      key: 'records_collected',
      header: 'Records',
      width: '10%',
      render: (value: unknown) => (value as number).toLocaleString(),
    },
    {
      key: 'error_message',
      header: 'Error',
      width: '35%',
      render: (value: unknown) => {
        const msg = value as string | null
        if (!msg) return <span className="text-muted">-</span>
        return <span className="log-error-text">{msg}</span>
      },
    },
  ], [])

  return (
    <div className="page reports-page">
      <header className="page-header">
        <h2>Reports</h2>
        <p>System logs, errors, and collection job history</p>
      </header>

      {/* Stats summary */}
      {loadingStats ? (
        <LoadingSpinner size="small" />
      ) : stats ? (
        <section className="reports-summary">
          <MetricCard
            label="Total Log Entries"
            value={stats.total_logs.toLocaleString()}
          />
          <MetricCard
            label="Errors (24h)"
            value={stats.recent_errors}
            color={stats.recent_errors > 0 ? 'danger' : 'default'}
          />
          <MetricCard
            label="Critical"
            value={stats.critical_count}
            color={stats.critical_count > 0 ? 'danger' : 'default'}
          />
          <MetricCard
            label="Errors"
            value={stats.error_count}
            color={stats.error_count > 10 ? 'warning' : 'default'}
          />
          <MetricCard
            label="Warnings"
            value={stats.warning_count}
          />
        </section>
      ) : null}

      {/* Tab navigation */}
      <div className="reports-tabs">
        <button
          className={`tab-btn ${activeTab === 'logs' ? 'tab-btn--active' : ''}`}
          onClick={() => setActiveTab('logs')}
        >
          System Logs
        </button>
        <button
          className={`tab-btn ${activeTab === 'jobs' ? 'tab-btn--active' : ''}`}
          onClick={() => setActiveTab('jobs')}
        >
          Collection Jobs
        </button>
      </div>

      {/* System Logs tab */}
      {activeTab === 'logs' && (
        <section className="reports-logs">
          <div className="reports-filters">
            <select
              value={levelFilter}
              onChange={e => { setLevelFilter(e.target.value as LogLevel); setLogPage(1) }}
              className="filter-select"
            >
              <option value="">All Levels</option>
              <option value="CRITICAL">Critical</option>
              <option value="ERROR">Error</option>
              <option value="WARNING">Warning</option>
              <option value="INFO">Info</option>
            </select>

            <input
              type="text"
              placeholder="Filter by source module..."
              value={sourceFilter}
              onChange={e => { setSourceFilter(e.target.value); setLogPage(1) }}
              className="filter-input"
            />

            <div className="search-group">
              <input
                type="text"
                placeholder="Search log events..."
                value={searchInput}
                onChange={e => setSearchInput(e.target.value)}
                onKeyDown={handleKeyDown}
                className="filter-input"
              />
              <button className="btn btn-primary btn-sm" onClick={handleSearch}>
                Search
              </button>
            </div>

            {(levelFilter || sourceFilter || searchFilter) && (
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => {
                  setLevelFilter('')
                  setSourceFilter('')
                  setSearchFilter('')
                  setSearchInput('')
                  setLogPage(1)
                }}
              >
                Clear Filters
              </button>
            )}
          </div>

          {loadingLogs ? (
            <LoadingSpinner message="Loading logs..." />
          ) : (
            <DataTable
              data={(logsData?.logs || []) as unknown as Record<string, unknown>[]}
              columns={logColumns as { key: string; header: string; width?: string; render?: (value: unknown, row: Record<string, unknown>) => React.ReactNode }[]}
              pagination={{
                page: logsData?.page || 1,
                pageSize,
                total: logsData?.total || 0,
                hasMore: logsData?.has_more || false,
                onPageChange: setLogPage,
              }}
              emptyMessage="No log entries found"
            />
          )}
        </section>
      )}

      {/* Collection Jobs tab */}
      {activeTab === 'jobs' && (
        <section className="reports-jobs">
          <div className="reports-filters">
            <select
              value={jobStatusFilter}
              onChange={e => { setJobStatusFilter(e.target.value); setJobPage(1) }}
              className="filter-select"
            >
              <option value="">All Statuses</option>
              <option value="completed">Completed</option>
              <option value="running">Running</option>
              <option value="failed">Failed</option>
            </select>
          </div>

          {loadingJobs ? (
            <LoadingSpinner message="Loading collection jobs..." />
          ) : (
            <DataTable
              data={(jobsData?.jobs || []) as unknown as Record<string, unknown>[]}
              columns={jobColumns as { key: string; header: string; width?: string; render?: (value: unknown, row: Record<string, unknown>) => React.ReactNode }[]}
              pagination={{
                page: jobsData?.page || 1,
                pageSize,
                total: jobsData?.total || 0,
                hasMore: jobsData?.has_more || false,
                onPageChange: setJobPage,
              }}
              emptyMessage="No collection jobs found"
            />
          )}
        </section>
      )}

      {/* Top error sources */}
      {stats && stats.top_sources.length > 0 && (
        <section className="reports-top-sources">
          <h3>Top Log Sources</h3>
          <div className="source-list">
            {stats.top_sources.map((s, i) => (
              <div
                key={i}
                className="source-item"
                onClick={() => {
                  setSourceFilter(s.source)
                  setActiveTab('logs')
                  setLogPage(1)
                }}
                style={{ cursor: 'pointer' }}
              >
                <span className="source-name">{s.source}</span>
                <span className="source-count">{s.count}</span>
              </div>
            ))}
          </div>
        </section>
      )}

      <style>{`
        .reports-page .reports-summary {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
          gap: 12px;
          margin-bottom: 20px;
        }

        .reports-tabs {
          display: flex;
          gap: 4px;
          margin-bottom: 16px;
          border-bottom: 2px solid #2a2a3e;
          padding-bottom: 0;
        }

        .tab-btn {
          background: transparent;
          border: none;
          color: #a0a0b0;
          padding: 10px 20px;
          cursor: pointer;
          font-size: 14px;
          font-weight: 500;
          border-bottom: 2px solid transparent;
          transition: all 0.2s;
          margin-bottom: -2px;
        }

        .tab-btn:hover {
          color: #e0e0f0;
        }

        .tab-btn--active {
          color: #7c4dff;
          border-bottom-color: #7c4dff;
        }

        .reports-filters {
          display: flex;
          gap: 10px;
          align-items: center;
          margin-bottom: 16px;
          flex-wrap: wrap;
        }

        .filter-select {
          background: #1a1a2e;
          color: #e0e0f0;
          border: 1px solid #333355;
          border-radius: 6px;
          padding: 8px 12px;
          font-size: 13px;
          min-width: 140px;
        }

        .filter-input {
          background: #1a1a2e;
          color: #e0e0f0;
          border: 1px solid #333355;
          border-radius: 6px;
          padding: 8px 12px;
          font-size: 13px;
          min-width: 180px;
        }

        .filter-input::placeholder {
          color: #666688;
        }

        .search-group {
          display: flex;
          gap: 6px;
          align-items: center;
        }

        .btn-sm {
          padding: 7px 14px;
          font-size: 13px;
        }

        .btn-primary {
          background: #7c4dff;
          color: white;
          border: none;
          border-radius: 6px;
          cursor: pointer;
        }

        .btn-primary:hover {
          background: #6a3de8;
        }

        .btn-secondary {
          background: #333355;
          color: #e0e0f0;
          border: none;
          border-radius: 6px;
          cursor: pointer;
        }

        .btn-secondary:hover {
          background: #444466;
        }

        .log-badge {
          font-weight: 700;
          font-size: 11px;
          text-transform: uppercase;
          letter-spacing: 0.5px;
        }

        .log-event-cell {
          max-width: 100%;
        }

        .log-event-text {
          display: block;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
          max-width: 500px;
        }

        .log-event-text:hover {
          color: #7c4dff;
        }

        .log-extra-data {
          background: #0d0d1a;
          border: 1px solid #2a2a3e;
          border-radius: 4px;
          padding: 8px;
          margin-top: 6px;
          font-size: 11px;
          color: #b0b0c0;
          max-height: 200px;
          overflow-y: auto;
          white-space: pre-wrap;
          word-break: break-all;
        }

        .log-source {
          font-family: monospace;
          font-size: 12px;
          color: #8888aa;
        }

        .log-error-text {
          color: #ff6b6b;
          font-size: 12px;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
          display: block;
          max-width: 350px;
        }

        .text-muted {
          color: #555566;
        }

        .job-status-badge {
          font-weight: 700;
          font-size: 12px;
          text-transform: uppercase;
        }

        .reports-top-sources {
          margin-top: 24px;
        }

        .reports-top-sources h3 {
          margin-bottom: 12px;
          color: #e0e0f0;
        }

        .source-list {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
          gap: 8px;
        }

        .source-item {
          display: flex;
          justify-content: space-between;
          align-items: center;
          background: #1a1a2e;
          border: 1px solid #2a2a3e;
          border-radius: 6px;
          padding: 8px 14px;
          transition: border-color 0.2s;
        }

        .source-item:hover {
          border-color: #7c4dff;
        }

        .source-name {
          font-family: monospace;
          font-size: 12px;
          color: #a0a0c0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          margin-right: 10px;
        }

        .source-count {
          font-weight: 700;
          color: #7c4dff;
          font-size: 13px;
          flex-shrink: 0;
        }
      `}</style>
    </div>
  )
}

export default Reports

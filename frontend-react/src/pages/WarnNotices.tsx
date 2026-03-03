import React, { FC, useState, useRef, useEffect, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import * as d3 from 'd3'
import { ComposableMap, Geographies, Geography } from 'react-simple-maps'
import {
  fetchWarnStats,
  fetchWarnTrends,
  fetchWarnByState,
  fetchWarnBySector,
  fetchWarnSizes,
  fetchWarnNotices,
  StateBreakdown,
} from '../api/warn'
import { MetricCard, TimeSeriesChart, DateRangeFilter, LoadingSpinner, DataTable } from '../components/shared'

const US_TOPO_JSON = 'https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json'

const STATE_FIPS: Record<string, string> = {
  '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
  '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
  '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
  '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
  '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
  '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
  '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
  '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
  '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
  '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
  '56': 'WY',
}

const WarnNotices: FC = () => {
  const [startDate, setStartDate] = useState<string | null>(null)
  const [endDate, setEndDate] = useState<string | null>(null)
  const [selectedState, setSelectedState] = useState<string | null>(null)
  const [selectedSector, setSelectedSector] = useState<string | null>(null)
  const [page, setPage] = useState(1)
  const pageSize = 20

  const dateParams = {
    start_date: startDate || undefined,
    end_date: endDate || undefined,
  }

  const { data: stats, isLoading: loadingStats } = useQuery({
    queryKey: ['warnStats', startDate, endDate],
    queryFn: () => fetchWarnStats(dateParams),
  })

  const { data: trends, isLoading: loadingTrends } = useQuery({
    queryKey: ['warnTrends', startDate, endDate],
    queryFn: () => fetchWarnTrends({ ...dateParams, granularity: 'monthly' }),
  })

  const { data: byState, isLoading: loadingByState } = useQuery({
    queryKey: ['warnByState', startDate, endDate],
    queryFn: () => fetchWarnByState(dateParams),
  })

  const { data: bySector, isLoading: loadingBySector } = useQuery({
    queryKey: ['warnBySector', startDate, endDate],
    queryFn: () => fetchWarnBySector(dateParams),
  })

  const { data: sizes, isLoading: loadingSizes } = useQuery({
    queryKey: ['warnSizes', startDate, endDate],
    queryFn: () => fetchWarnSizes(dateParams),
  })

  const { data: notices, isLoading: loadingNotices } = useQuery({
    queryKey: ['warnNotices', page, selectedState, selectedSector],
    queryFn: () => fetchWarnNotices({
      page,
      page_size: pageSize,
      state: selectedState || undefined,
      sector: selectedSector || undefined,
    }),
  })

  const handleDateRangeChange = useCallback((start: string | null, end: string | null) => {
    setStartDate(prev => prev === start ? prev : start)
    setEndDate(prev => prev === end ? prev : end)
    setPage(1)
  }, [])

  const isInitialLoad = loadingStats && !stats

  const stateData = byState?.states || []
  const sectorData = bySector?.sectors || []
  const trendData = (trends?.data || []).map(d => ({
    date: d.period,
    value: d.notice_count,
  }))

  const stateMap = new Map<string, StateBreakdown>()
  stateData.forEach(s => stateMap.set(s.state, s))

  const maxEmployees = Math.max(...stateData.map(s => s.employees_affected), 1)
  const colorScale = d3.scaleSequential(d3.interpolateReds)
    .domain([0, maxEmployees])

  const tableColumns = [
    { key: 'company_name', header: 'Company', width: '25%' },
    { key: 'state', header: 'State', width: '8%' },
    { key: 'city', header: 'City', width: '12%' },
    { key: 'notice_date', header: 'Notice Date', width: '12%',
      render: (value: unknown) => new Date(value as string).toLocaleDateString() },
    { key: 'employees_affected', header: 'Employees', width: '10%',
      render: (value: unknown) => (value as number).toLocaleString() },
    { key: 'layoff_type', header: 'Type', width: '10%' },
    { key: 'sector_category', header: 'Sector', width: '15%' },
  ]

  return (
    <div className="page warn-page">
      <header className="page-header">
        <h2>WARN Notices</h2>
        <p>Worker Adjustment and Retraining Notification Act filings analysis</p>
      </header>

      <section className="warn-filters">
        <DateRangeFilter onChange={handleDateRangeChange} defaultRange="Last 90 Days" />
      </section>

      {isInitialLoad ? (
        <LoadingSpinner message="Loading WARN data..." />
      ) : (
      <>
      <section className="warn-summary">
        <MetricCard
          label="Total Notices"
          value={(stats?.total_notices || 0).toLocaleString()}
        />
        <MetricCard
          label="Employees Affected"
          value={(stats?.total_employees_affected || 0).toLocaleString()}
          color={stats && stats.total_employees_affected > 50000 ? 'warning' : 'default'}
        />
        <MetricCard
          label="States Reporting"
          value={stats?.states_reporting || 0}
        />
        <MetricCard
          label="Avg per Notice"
          value={Math.round(stats?.avg_employees_per_notice || 0)}
        />
      </section>

      <div className="warn-grid">
        <section className="warn-map">
          <h3>Geographic Distribution</h3>
          {loadingByState ? (
            <LoadingSpinner size="small" />
          ) : (
          <>
          <ComposableMap projection="geoAlbersUsa" width={800} height={500}>
            <Geographies geography={US_TOPO_JSON}>
              {({ geographies }) =>
                geographies.map((geo) => {
                  const stateAbbr = STATE_FIPS[geo.id] || ''
                  const stateInfo = stateMap.get(stateAbbr)
                  const fill = stateInfo
                    ? colorScale(stateInfo.employees_affected)
                    : '#1a1a2e'

                  return (
                    <Geography
                      key={geo.rsmKey}
                      geography={geo}
                      fill={fill}
                      stroke="#333"
                      strokeWidth={0.5}
                      style={{
                        default: { outline: 'none' },
                        hover: { outline: 'none', fill: '#F89880' },
                        pressed: { outline: 'none' },
                      }}
                      onClick={() => setSelectedState(stateAbbr || null)}
                    />
                  )
                })
              }
            </Geographies>
          </ComposableMap>
          <div className="map-legend">
            <span>0</span>
            <div className="legend-gradient" style={{
              background: `linear-gradient(to right, ${colorScale(0)}, ${colorScale(maxEmployees)})`
            }} />
            <span>{maxEmployees.toLocaleString()}</span>
          </div>
          </>
          )}
        </section>

        <section className="warn-trend">
          <h3>Monthly Trend</h3>
          {loadingTrends ? (
            <LoadingSpinner size="small" />
          ) : trendData.length > 0 ? (
            <TimeSeriesChart
              data={trendData}
              height={250}
              showArea
              yAxisLabel="Notices"
            />
          ) : (
            <div className="empty-state">No trend data available</div>
          )}
        </section>
      </div>

      <div className="warn-charts">
        <section className="warn-sector-chart">
          <h3>By Sector</h3>
          {loadingBySector ? (
            <LoadingSpinner size="small" />
          ) : sectorData.length > 0 ? (
            <SectorBarChart data={sectorData} onSelect={setSelectedSector} />
          ) : (
            <div className="empty-state">No sector data available</div>
          )}
        </section>

        <section className="warn-sizes-chart">
          <h3>Company Size Distribution</h3>
          {loadingSizes ? (
            <LoadingSpinner size="small" />
          ) : sizes?.buckets && sizes.buckets.length > 0 ? (
            <SizeDistributionChart buckets={sizes.buckets} />
          ) : (
            <div className="empty-state">No size data available</div>
          )}
        </section>
      </div>

      <section className="warn-notices-list">
        <h3>Recent Notices {selectedState && `(${selectedState})`} {selectedSector && `- ${selectedSector}`}</h3>
        {selectedState || selectedSector ? (
          <button className="btn btn-secondary" onClick={() => { setSelectedState(null); setSelectedSector(null); }}>
            Clear Filters
          </button>
        ) : null}

        {loadingNotices ? (
          <LoadingSpinner size="small" />
        ) : (
          <DataTable
            data={(notices?.notices || []) as unknown as Record<string, unknown>[]}
            columns={tableColumns as { key: string; header: string; width?: string; render?: (value: unknown, row: Record<string, unknown>) => React.ReactNode }[]}
            pagination={{
              page: notices?.page || 1,
              pageSize,
              total: notices?.total || 0,
              hasMore: notices?.has_more || false,
              onPageChange: setPage,
            }}
            emptyMessage="No WARN notices found"
          />
        )}
      </section>
      </>
      )}
    </div>
  )
}

interface SectorBarChartProps {
  data: { sector: string; notice_count: number; employees_affected: number }[]
  onSelect: (sector: string | null) => void
}

const SectorBarChart: FC<SectorBarChartProps> = ({ data, onSelect }) => {
  const svgRef = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!svgRef.current || data.length === 0) return

    const margin = { top: 10, right: 100, bottom: 30, left: 150 }
    const width = 500
    const height = Math.max(200, data.length * 25)
    const innerWidth = width - margin.left - margin.right
    const innerHeight = height - margin.top - margin.bottom

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg.attr('width', width).attr('height', height)

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`)

    const sorted = [...data].sort((a, b) => b.employees_affected - a.employees_affected).slice(0, 10)

    const xScale = d3.scaleLinear()
      .domain([0, d3.max(sorted, d => d.employees_affected) || 0])
      .range([0, innerWidth])

    const yScale = d3.scaleBand()
      .domain(sorted.map(d => d.sector))
      .range([0, innerHeight])
      .padding(0.2)

    g.append('g')
      .call(d3.axisLeft(yScale))
      .attr('color', '#a0a0a0')
      .selectAll('text')
      .style('font-size', '11px')

    g.selectAll('.bar')
      .data(sorted)
      .join('rect')
      .attr('class', 'bar')
      .attr('y', d => yScale(d.sector) || 0)
      .attr('height', yScale.bandwidth())
      .attr('x', 0)
      .attr('width', d => xScale(d.employees_affected))
      .attr('fill', '#1f77b4')
      .attr('cursor', 'pointer')
      .on('click', (_, d) => onSelect(d.sector))

    g.selectAll('.label')
      .data(sorted)
      .join('text')
      .attr('class', 'label')
      .attr('y', d => (yScale(d.sector) || 0) + yScale.bandwidth() / 2)
      .attr('x', d => xScale(d.employees_affected) + 5)
      .attr('dy', '0.35em')
      .attr('fill', '#a0a0a0')
      .attr('font-size', '11px')
      .text(d => d.employees_affected.toLocaleString())

  }, [data, onSelect])

  return <svg ref={svgRef} />
}

interface SizeDistributionChartProps {
  buckets: { label: string; count: number; pct_of_total: number }[]
}

const SizeDistributionChart: FC<SizeDistributionChartProps> = ({ buckets }) => {
  const svgRef = useRef<SVGSVGElement>(null)

  useEffect(() => {
    if (!svgRef.current || buckets.length === 0) return

    const width = 400
    const height = 200
    const radius = Math.min(width, height) / 2 - 20

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg.attr('width', width).attr('height', height)

    const g = svg.append('g')
      .attr('transform', `translate(${width / 2},${height / 2})`)

    const color = d3.scaleOrdinal<string>()
      .domain(buckets.map(b => b.label))
      .range(d3.schemeTableau10)

    const pie = d3.pie<typeof buckets[0]>()
      .value(d => d.count)

    const arc = d3.arc<d3.PieArcDatum<typeof buckets[0]>>()
      .innerRadius(radius * 0.4)
      .outerRadius(radius)

    const arcs = g.selectAll('.arc')
      .data(pie(buckets))
      .join('g')
      .attr('class', 'arc')

    arcs.append('path')
      .attr('d', arc)
      .attr('fill', d => color(d.data.label))

    const legend = svg.append('g')
      .attr('transform', `translate(${width - 100}, 20)`)

    buckets.forEach((b, i) => {
      const row = legend.append('g')
        .attr('transform', `translate(0, ${i * 18})`)

      row.append('rect')
        .attr('width', 12)
        .attr('height', 12)
        .attr('fill', color(b.label))

      row.append('text')
        .attr('x', 16)
        .attr('y', 10)
        .attr('fill', '#a0a0a0')
        .attr('font-size', '10px')
        .text(b.label)
    })

  }, [buckets])

  return <svg ref={svgRef} />
}

export default WarnNotices

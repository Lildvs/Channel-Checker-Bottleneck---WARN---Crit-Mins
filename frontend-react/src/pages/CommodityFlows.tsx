import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchMinerals, fetchCountries, fetchSankeyData, fetchTradeStats } from '../api/tradeFlows'
import WorldMap from '../components/flows/WorldMap'
import SankeyDiagram from '../components/flows/SankeyDiagram'
import MineralSelector from '../components/flows/MineralSelector'

function CommodityFlows() {
  const [selectedMineral, setSelectedMineral] = useState<string>('lithium')
  const [highlightedCountry, setHighlightedCountry] = useState<string | null>(null)

  const { data: minerals } = useQuery({
    queryKey: ['minerals'],
    queryFn: fetchMinerals,
  })

  const { data: stats } = useQuery({
    queryKey: ['tradeStats'],
    queryFn: () => fetchTradeStats(),
  })

  const { data: countriesData } = useQuery({
    queryKey: ['countries', selectedMineral],
    queryFn: () => fetchCountries({ mineral: selectedMineral }),
  })

  const { data: sankeyData } = useQuery({
    queryKey: ['sankey', selectedMineral],
    queryFn: () => fetchSankeyData(selectedMineral),
  })

  const handleCountryHover = (iso3: string | null) => {
    setHighlightedCountry(iso3)
  }

  return (
    <div className="page commodity-flows">
      <header className="page-header">
        <h2>Commodity Flow Maps</h2>
        <p>Geographic visualization of critical mineral trade flows and supply chains</p>
      </header>

      {/* Toolbar */}
      <div className="flows-toolbar">
        <MineralSelector
          minerals={minerals || []}
          selected={selectedMineral}
          onChange={setSelectedMineral}
        />

        {stats && (
          <div className="toolbar-stats">
            <span>Total Trade: ${(stats.total_trade_value / 1e9).toFixed(1)}B</span>
            <span>|</span>
            <span>Flows: {stats.total_flows.toLocaleString()}</span>
          </div>
        )}
      </div>

      <div className="flows-content">
        {/* Map container */}
        <div className="map-container">
          <WorldMap
            countries={countriesData?.countries || []}
            highlightedCountry={highlightedCountry}
            onCountryHover={handleCountryHover}
            mineral={selectedMineral}
          />
        </div>

        {/* Sankey panel */}
        <div className="sankey-panel">
          <h3>Supply Chain Flow: {selectedMineral.replace('_', ' ')}</h3>
          {sankeyData && (
            <SankeyDiagram
              nodes={sankeyData.nodes}
              links={sankeyData.links}
              onNodeHover={handleCountryHover}
            />
          )}

          {stats && (
            <div className="flow-stats">
              <h4>Top Exporters</h4>
              <ul>
                {stats.top_exporters.slice(0, 5).map((exp) => (
                  <li key={exp.country}>
                    {exp.country}: ${(exp.value / 1e9).toFixed(2)}B
                  </li>
                ))}
              </ul>

              <h4>Top Importers</h4>
              <ul>
                {stats.top_importers.slice(0, 5).map((imp) => (
                  <li key={imp.country}>
                    {imp.country}: ${(imp.value / 1e9).toFixed(2)}B
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default CommodityFlows

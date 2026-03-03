import { FC, useState, useMemo } from 'react'
import {
  ComposableMap,
  Geographies,
  Geography,
  ZoomableGroup,
} from 'react-simple-maps'
import { CountryVolume } from '../../api/tradeFlows'

const geoUrl = 'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json'

interface WorldMapProps {
  countries: CountryVolume[]
  highlightedCountry: string | null
  onCountryHover: (iso3: string | null) => void
  mineral: string
}

const WorldMap: FC<WorldMapProps> = ({
  countries,
  highlightedCountry,
  onCountryHover,
  mineral,
}) => {
  const [tooltipContent, setTooltipContent] = useState<string>('')
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })

  const countryMap = useMemo(() => {
    const map = new Map<string, CountryVolume>()
    countries.forEach((c) => {
      if (c.iso3) {
        map.set(c.iso3, c)
      }
    })
    return map
  }, [countries])

  const maxValue = useMemo(() => {
    return Math.max(...countries.map((c) => c.total_value), 1)
  }, [countries])

  const getCountryColor = (iso3: string) => {
    const country = countryMap.get(iso3)
    if (!country) return '#16213e' // Default dark blue

    const intensity = Math.min(country.total_value / maxValue, 1)

    if (iso3 === highlightedCountry) {
      return '#FF5F1F' // Highlight color
    }

    const r = Math.floor(15 + intensity * 218)
    const g = Math.floor(33 + intensity * (69 - 33))
    const b = Math.floor(62 + intensity * (96 - 62))

    return `rgb(${r}, ${g}, ${b})`
  }

  const handleMouseEnter = (geo: any, event: React.MouseEvent) => {
    const iso3 = geo.properties.ISO_A3 || geo.id
    const country = countryMap.get(iso3)

    onCountryHover(iso3)

    if (country) {
      const content = `${country.country}: $${(country.total_value / 1e9).toFixed(2)}B\nExports: $${(country.export_value / 1e9).toFixed(2)}B\nImports: $${(country.import_value / 1e9).toFixed(2)}B`
      setTooltipContent(content)
    } else {
      setTooltipContent(geo.properties.NAME || geo.properties.name || '')
    }

    setTooltipPos({ x: event.clientX, y: event.clientY })
  }

  const handleMouseLeave = () => {
    onCountryHover(null)
    setTooltipContent('')
  }

  return (
    <div className="world-map-container" style={{ position: 'relative', width: '100%', height: '100%' }}>
      <ComposableMap
        projection="geoMercator"
        projectionConfig={{
          scale: 120,
          center: [0, 20],
        }}
        style={{ width: '100%', height: '100%' }}
      >
        <ZoomableGroup>
          <Geographies geography={geoUrl}>
            {({ geographies }) =>
              geographies.map((geo) => {
                const iso3 = geo.properties.ISO_A3 || geo.id

                return (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    fill={getCountryColor(iso3)}
                    stroke="#2a2a4a"
                    strokeWidth={0.5}
                    style={{
                      default: { outline: 'none' },
                      hover: { outline: 'none', fill: '#FF5F1F' },
                      pressed: { outline: 'none' },
                    }}
                    onMouseEnter={(e) => handleMouseEnter(geo, e)}
                    onMouseLeave={handleMouseLeave}
                  />
                )
              })
            }
          </Geographies>
        </ZoomableGroup>
      </ComposableMap>

      {/* Tooltip */}
      {tooltipContent && (
        <div
          className="map-tooltip"
          style={{
            position: 'fixed',
            left: tooltipPos.x + 10,
            top: tooltipPos.y - 10,
            background: '#16213e',
            border: '1px solid #2a2a4a',
            borderRadius: '4px',
            padding: '0.5rem 0.75rem',
            fontSize: '0.75rem',
            color: '#eaeaea',
            pointerEvents: 'none',
            whiteSpace: 'pre-line',
            zIndex: 1000,
          }}
        >
          {tooltipContent}
        </div>
      )}

      {/* Legend */}
      <div
        className="map-legend"
        style={{
          position: 'absolute',
          bottom: '1rem',
          left: '1rem',
          background: '#16213e',
          border: '1px solid #2a2a4a',
          borderRadius: '4px',
          padding: '0.5rem',
          fontSize: '0.625rem',
        }}
      >
        <div style={{ marginBottom: '0.25rem', fontWeight: 600 }}>Trade Volume ({mineral})</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <div style={{ width: '60px', height: '8px', background: 'linear-gradient(to right, #16213e, #FF5F1F)', borderRadius: '2px' }} />
          <span>Low → High</span>
        </div>
      </div>
    </div>
  )
}

export default WorldMap

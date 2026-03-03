import { FC } from 'react'

interface GraphControlsProps {
  categoryFilter: string[]
  onCategoryChange: (categories: string[]) => void
  severityThreshold: number
  onSeverityChange: (threshold: number) => void
  selectedSector: string | null
  onClearSelection: () => void
}

const CATEGORIES = [
  { id: 'utilities', label: 'Utilities & Energy' },
  { id: 'goods', label: 'Goods Production' },
  { id: 'services', label: 'Services' },
]

const GraphControls: FC<GraphControlsProps> = ({
  categoryFilter,
  onCategoryChange,
  severityThreshold,
  onSeverityChange,
  selectedSector,
  onClearSelection,
}) => {
  const handleCategoryToggle = (category: string) => {
    if (categoryFilter.includes(category)) {
      onCategoryChange(categoryFilter.filter(c => c !== category))
    } else {
      onCategoryChange([...categoryFilter, category])
    }
  }

  return (
    <>
      <div className="controls-section">
        <h3>Sector Categories</h3>
        <div className="checkbox-group">
          {CATEGORIES.map(cat => (
            <label key={cat.id}>
              <input
                type="checkbox"
                checked={categoryFilter.length === 0 || categoryFilter.includes(cat.id)}
                onChange={() => handleCategoryToggle(cat.id)}
              />
              {cat.label}
            </label>
          ))}
        </div>
      </div>

      <div className="controls-section">
        <h3>Risk Filter</h3>
        <div className="slider-control">
          <label>
            <span>Min Risk Score</span>
            <span>{(severityThreshold * 100).toFixed(0)}%</span>
          </label>
          <input
            type="range"
            min={0}
            max={100}
            value={severityThreshold * 100}
            onChange={e => onSeverityChange(parseInt(e.target.value) / 100)}
          />
        </div>
      </div>

      {selectedSector && (
        <div className="controls-section">
          <h3>Selection</h3>
          <p style={{ fontSize: '0.875rem', marginBottom: '0.5rem' }}>
            Selected: <strong>{selectedSector}</strong>
          </p>
          <button className="btn btn-text" onClick={onClearSelection}>
            Clear Selection
          </button>
        </div>
      )}

      <div className="controls-section legend">
        <h3>Legend</h3>
        <div className="legend-item">
          <div className="legend-color" style={{ background: '#4ecca3' }} />
          <span>Low Risk (0-40%)</span>
        </div>
        <div className="legend-item">
          <div className="legend-color" style={{ background: '#ffc93c' }} />
          <span>Medium Risk (40-70%)</span>
        </div>
        <div className="legend-item">
          <div className="legend-color" style={{ background: '#FF5F1F' }} />
          <span>High Risk (70-100%)</span>
        </div>
        <div className="legend-item" style={{ marginTop: '0.5rem' }}>
          <div className="legend-color" style={{ background: 'transparent', border: '2px solid #a0a0a0' }} />
          <span>Dependency link</span>
        </div>
      </div>
    </>
  )
}

export default GraphControls

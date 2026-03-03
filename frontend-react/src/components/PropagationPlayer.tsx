import { FC, useState, useEffect } from 'react'
import axios from 'axios'
import { PropagationData } from '../api/sectors'

interface BottleneckOption {
  id: string
  name: string
}

interface PropagationPlayerProps {
  isPlaying: boolean
  speed: number
  onSpeedChange: (speed: number) => void
  onPlay: (bottleneckId: string) => void
  onStop: () => void
  propagationData: PropagationData | null
}

const PropagationPlayer: FC<PropagationPlayerProps> = ({
  isPlaying,
  speed,
  onSpeedChange,
  onPlay,
  onStop,
  propagationData,
}) => {
  const [bottlenecks, setBottlenecks] = useState<BottleneckOption[]>([])
  const [selectedBottleneck, setSelectedBottleneck] = useState<string>('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function fetchBottlenecks() {
      try {
        const response = await axios.get('/api/bottlenecks/active')
        const data = response.data.bottlenecks || []
        const options: BottleneckOption[] = data.map((b: { id: string; category: string; subcategory?: string }) => ({
          id: b.id,
          name: `${b.category.replace('_', ' ')} (${b.subcategory || 'N/A'})`,
        }))
        setBottlenecks(options)
        if (options.length > 0) {
          setSelectedBottleneck(options[0].id)
        }
      } catch (err) {
        setError('Failed to load bottlenecks')
      } finally {
        setLoading(false)
      }
    }

    fetchBottlenecks()
  }, [])

  const handlePlay = () => {
    if (selectedBottleneck) {
      onPlay(selectedBottleneck)
    }
  }

  if (loading) {
    return (
      <div className="controls-section">
        <h3>Propagation Animation</h3>
        <div className="loading">Loading bottlenecks...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="controls-section">
        <h3>Propagation Animation</h3>
        <div className="error">{error}</div>
      </div>
    )
  }

  if (bottlenecks.length === 0) {
    return (
      <div className="controls-section">
        <h3>Propagation Animation</h3>
        <div className="empty-state">No active bottlenecks available.</div>
      </div>
    )
  }

  return (
    <div className="controls-section">
      <h3>Propagation Animation</h3>
      <div className="propagation-player">
        <select
          value={selectedBottleneck}
          onChange={e => setSelectedBottleneck(e.target.value)}
          disabled={isPlaying}
          style={{
            width: '100%',
            padding: '0.5rem',
            background: 'var(--bg-secondary)',
            border: '1px solid var(--border)',
            borderRadius: '4px',
            color: 'var(--text-primary)',
            fontSize: '0.875rem',
          }}
        >
          {bottlenecks.map(b => (
            <option key={b.id} value={b.id}>
              {b.name}
            </option>
          ))}
        </select>

        <div className="propagation-controls">
          {isPlaying ? (
            <button className="btn btn-secondary" onClick={onStop}>
              Stop
            </button>
          ) : (
            <button className="btn btn-primary" onClick={handlePlay}>
              Play Propagation
            </button>
          )}
        </div>

        <div className="slider-control" style={{ marginTop: '0.75rem' }}>
          <label>
            <span>Speed</span>
            <span>{speed}x</span>
          </label>
          <input
            type="range"
            min={0.5}
            max={3}
            step={0.5}
            value={speed}
            onChange={e => onSpeedChange(parseFloat(e.target.value))}
          />
        </div>

        {propagationData && (
          <div className="propagation-status">
            <p>Origin: {propagationData.originSector}</p>
            <p>Steps: {propagationData.steps.length}</p>
            <p>Total Impact: {(propagationData.totalImpact * 100).toFixed(0)}%</p>
          </div>
        )}
      </div>
    </div>
  )
}

export default PropagationPlayer

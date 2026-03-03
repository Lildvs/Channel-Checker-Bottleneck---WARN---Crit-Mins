import { useState } from 'react'
import DependencyGraph from '../components/DependencyGraph'
import GraphControls from '../components/GraphControls'
import PropagationPlayer from '../components/PropagationPlayer'
import { useSectorData } from '../hooks/useSectorData'

function SectorDependencies() {
  const [selectedSector, setSelectedSector] = useState<string | null>(null)
  const [categoryFilter, setCategoryFilter] = useState<string[]>([])
  const [severityThreshold, setSeverityThreshold] = useState(0)
  const [isPlaying, setIsPlaying] = useState(false)
  const [playbackSpeed, setPlaybackSpeed] = useState(1)

  const { nodes, edges, isLoading, error, propagationData, fetchPropagation } = useSectorData()

  const handleSectorClick = (sectorId: string) => {
    setSelectedSector(sectorId === selectedSector ? null : sectorId)
  }

  const handlePlayPropagation = async (bottleneckId: string) => {
    await fetchPropagation(bottleneckId)
    setIsPlaying(true)
  }

  const handleStopPropagation = () => {
    setIsPlaying(false)
  }

  if (error) {
    return (
      <div className="page error">
        <h1>Error Loading Data</h1>
        <p>{error.message}</p>
      </div>
    )
  }

  return (
    <div className="page sector-dependencies">
      <header className="page-header">
        <h2>Sector Dependency Graph</h2>
        <p>Interactive visualization of I-O relationships and bottleneck propagation</p>
      </header>

      <div className="page-layout">
        <aside className="controls-panel">
          <GraphControls
            categoryFilter={categoryFilter}
            onCategoryChange={setCategoryFilter}
            severityThreshold={severityThreshold}
            onSeverityChange={setSeverityThreshold}
            selectedSector={selectedSector}
            onClearSelection={() => setSelectedSector(null)}
          />

          <PropagationPlayer
            isPlaying={isPlaying}
            speed={playbackSpeed}
            onSpeedChange={setPlaybackSpeed}
            onPlay={handlePlayPropagation}
            onStop={handleStopPropagation}
            propagationData={propagationData}
          />
        </aside>

        <main className="graph-container">
          {isLoading ? (
            <div className="loading">Loading sector data...</div>
          ) : (
            <DependencyGraph
              nodes={nodes}
              edges={edges}
              selectedSector={selectedSector}
              onSectorClick={handleSectorClick}
              categoryFilter={categoryFilter}
              severityThreshold={severityThreshold}
              propagationData={isPlaying ? propagationData : null}
              playbackSpeed={playbackSpeed}
            />
          )}
        </main>
      </div>
    </div>
  )
}

export default SectorDependencies

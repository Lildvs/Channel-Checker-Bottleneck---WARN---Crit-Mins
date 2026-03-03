import { forwardRef } from 'react'
import { SectorNode } from '../api/sectors'

interface SectorTooltipProps {
  node: SectorNode | null
  visible: boolean
}

const SectorTooltip = forwardRef<HTMLDivElement, SectorTooltipProps>(
  ({ node, visible }, ref) => {
    if (!node) return <div ref={ref} className="tooltip" />

    const getRiskLevel = (score: number): { label: string; className: string } => {
      if (score >= 0.7) return { label: 'High Risk', className: 'high' }
      if (score >= 0.4) return { label: 'Medium Risk', className: 'medium' }
      return { label: 'Low Risk', className: 'low' }
    }

    const risk = getRiskLevel(node.riskScore)

    return (
      <div
        ref={ref}
        className={`tooltip ${visible ? 'visible' : ''}`}
      >
        <h4>{node.name}</h4>
        <p>Category: {node.category}</p>
        <p>Size: {node.size.toLocaleString()}</p>
        <p>
          Risk Score: {(node.riskScore * 100).toFixed(0)}%
          <span className={`risk-badge ${risk.className}`} style={{ marginLeft: '0.5rem' }}>
            {risk.label}
          </span>
        </p>
      </div>
    )
  },
)

SectorTooltip.displayName = 'SectorTooltip'

export default SectorTooltip

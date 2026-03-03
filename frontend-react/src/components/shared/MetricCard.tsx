import { FC, ReactNode } from 'react'

interface MetricCardProps {
  label: string
  value: string | number
  icon?: ReactNode
  trend?: {
    value: number
    direction: 'up' | 'down' | 'neutral'
  }
  color?: 'default' | 'success' | 'warning' | 'danger'
}

const MetricCard: FC<MetricCardProps> = ({
  label,
  value,
  icon,
  trend,
  color = 'default',
}) => {
  const colorClasses = {
    default: '',
    success: 'metric-card--success',
    warning: 'metric-card--warning',
    danger: 'metric-card--danger',
  }

  const trendColors = {
    up: '#4ecca3',
    down: '#FF5F1F',
    neutral: '#a0a0a0',
  }

  return (
    <div className={`metric-card ${colorClasses[color]}`}>
      {icon && <div className="metric-card__icon">{icon}</div>}
      <div className="metric-card__value">{value}</div>
      <div className="metric-card__label">{label}</div>
      {trend && (
        <div
          className="metric-card__trend"
          style={{ color: trendColors[trend.direction] }}
        >
          {trend.direction === 'up' && '↑'}
          {trend.direction === 'down' && '↓'}
          {trend.direction === 'neutral' && '→'}
          {' '}
          {Math.abs(trend.value)}%
        </div>
      )}
    </div>
  )
}

export default MetricCard

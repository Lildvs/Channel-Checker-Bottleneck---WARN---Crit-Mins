import { FC, useMemo } from 'react'
import { Marker, Line } from 'react-simple-maps'
import { TradeFlow } from '../../api/tradeFlows'

const countryCentroids: Record<string, [number, number]> = {
  COD: [23.6, -2.9],    // DR Congo
  AUS: [134, -25],      // Australia
  CHL: [-71, -35],      // Chile
  ARG: [-64, -34],      // Argentina
  ZAF: [25, -30],       // South Africa
  IDN: [118, -2],       // Indonesia
  BRA: [-53, -10],      // Brazil
  PHL: [122, 12],       // Philippines
  CAN: [-106, 56],      // Canada
  RUS: [100, 60],       // Russia

  CHN: [105, 35],       // China
  USA: [-98, 39],       // United States
  JPN: [138, 36],       // Japan
  KOR: [128, 36],       // South Korea
  DEU: [10, 51],        // Germany
  GBR: [-2, 54],        // UK
  FRA: [2, 47],         // France
  IND: [78, 22],        // India
  NLD: [5, 52],         // Netherlands
  BEL: [4, 51],         // Belgium
}

interface FlowArrowsProps {
  flows: TradeFlow[]
  maxValue: number
  animate?: boolean
}

const FlowArrows: FC<FlowArrowsProps> = ({
  flows,
  maxValue,
  animate = true,
}) => {
  const validFlows = useMemo(() => {
    return flows.filter(
      (f) => f.reporter_iso3 && f.partner_iso3 &&
        countryCentroids[f.reporter_iso3] &&
        countryCentroids[f.partner_iso3]
    )
  }, [flows])

  return (
    <>
      {/* Arrow definitions */}
      <defs>
        <marker
          id="arrowhead"
          markerWidth="10"
          markerHeight="7"
          refX="9"
          refY="3.5"
          orient="auto"
        >
          <polygon
            points="0 0, 10 3.5, 0 7"
            fill="#FF5F1F"
          />
        </marker>
      </defs>

      {/* Draw flow lines */}
      {validFlows.map((flow, i) => {
        const from = countryCentroids[flow.reporter_iso3!]
        const to = countryCentroids[flow.partner_iso3!]

        if (!from || !to) return null

        const normalizedValue = flow.value_usd / maxValue
        const strokeWidth = Math.max(1, Math.min(5, normalizedValue * 5))
        const opacity = 0.3 + normalizedValue * 0.5

        return (
          <Line
            key={`${flow.reporter_iso3}-${flow.partner_iso3}-${i}`}
            from={from}
            to={to}
            stroke="#FF5F1F"
            strokeWidth={strokeWidth}
            strokeOpacity={opacity}
            strokeLinecap="round"
            className={animate ? 'flow-arrow animated' : 'flow-arrow'}
          />
        )
      })}

      {/* Draw markers at endpoints */}
      {validFlows.slice(0, 10).map((flow, i) => {
        const to = countryCentroids[flow.partner_iso3!]
        if (!to) return null

        const normalizedValue = flow.value_usd / maxValue
        const radius = 2 + normalizedValue * 4

        return (
          <Marker key={`marker-${i}`} coordinates={to}>
            <circle
              r={radius}
              fill="#FF5F1F"
              fillOpacity={0.7}
              className={animate ? 'pulse-marker' : ''}
            />
          </Marker>
        )
      })}
    </>
  )
}

export default FlowArrows

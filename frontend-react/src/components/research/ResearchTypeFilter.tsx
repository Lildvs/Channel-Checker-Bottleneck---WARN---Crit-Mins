import { FC } from 'react'

interface ResearchTypeFilterProps {
  selected: string | null
  onChange: (type: string | null) => void
}

const RESEARCH_TYPES = [
  { value: '', label: 'All Types' },
  { value: 'consensus', label: 'Consensus' },
  { value: 'emerging', label: 'Emerging' },
  { value: 'contrarian', label: 'Contrarian' },
]

const ResearchTypeFilter: FC<ResearchTypeFilterProps> = ({ selected, onChange }) => {
  return (
    <select
      value={selected || ''}
      onChange={(e) => onChange(e.target.value || null)}
    >
      {RESEARCH_TYPES.map((type) => (
        <option key={type.value} value={type.value}>
          {type.label}
        </option>
      ))}
    </select>
  )
}

export default ResearchTypeFilter

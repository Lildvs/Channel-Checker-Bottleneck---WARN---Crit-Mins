import { FC } from 'react'

interface MineralSelectorProps {
  minerals: string[]
  selected: string
  onChange: (mineral: string) => void
}

const MineralSelector: FC<MineralSelectorProps> = ({
  minerals,
  selected,
  onChange,
}) => {
  const formatMineral = (mineral: string) => {
    return mineral
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ')
  }

  return (
    <div className="mineral-selector">
      <label htmlFor="mineral-select">Mineral:</label>
      <select
        id="mineral-select"
        value={selected}
        onChange={(e) => onChange(e.target.value)}
      >
        {minerals.map((mineral) => (
          <option key={mineral} value={mineral}>
            {formatMineral(mineral)}
          </option>
        ))}
      </select>
    </div>
  )
}

export default MineralSelector

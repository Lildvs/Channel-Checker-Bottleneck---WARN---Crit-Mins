import { FC, useState, useRef, useEffect } from 'react'

interface DateRangeFilterProps {
  onChange: (startDate: string | null, endDate: string | null) => void
  defaultRange?: string
}

const DATE_RANGES: Record<string, number | null> = {
  'Last 7 Days': 7,
  'Last 30 Days': 30,
  'Last 90 Days': 90,
  'Last 6 Months': 180,
  'Last Year': 365,
  'All Time': null,
}

function computeDateRange(rangeName: string, customStart: string, customEnd: string): [string | null, string | null] {
  if (rangeName === 'Custom') {
    return [customStart || null, customEnd || null]
  }
  const days = DATE_RANGES[rangeName]
  if (days === null) {
    return [null, null]
  }
  const end = new Date()
  end.setMilliseconds(0)
  end.setSeconds(0)
  const start = new Date(end)
  start.setDate(start.getDate() - days)
  return [start.toISOString(), end.toISOString()]
}

const DateRangeFilter: FC<DateRangeFilterProps> = ({
  onChange,
  defaultRange = 'Last 90 Days',
}) => {
  const [selectedRange, setSelectedRange] = useState(defaultRange)
  const [customStart, setCustomStart] = useState('')
  const [customEnd, setCustomEnd] = useState('')
  const [showCustom, setShowCustom] = useState(false)
  const lastEmitted = useRef<string>('')

  useEffect(() => {
    const [start, end] = computeDateRange(selectedRange, customStart, customEnd)
    const key = `${start}|${end}`
    if (key === lastEmitted.current) return
    lastEmitted.current = key
    onChange(start, end)
  }, [selectedRange, customStart, customEnd, onChange])

  const handleRangeChange = (range: string) => {
    setSelectedRange(range)
    setShowCustom(range === 'Custom')
  }

  return (
    <div className="date-range-filter">
      <select
        value={selectedRange}
        onChange={(e) => handleRangeChange(e.target.value)}
        className="date-range-filter__select"
      >
        {Object.keys(DATE_RANGES).map((range) => (
          <option key={range} value={range}>
            {range}
          </option>
        ))}
        <option value="Custom">Custom Range</option>
      </select>

      {showCustom && (
        <div className="date-range-filter__custom">
          <input
            type="date"
            value={customStart}
            onChange={(e) => setCustomStart(e.target.value)}
            placeholder="Start Date"
          />
          <span>to</span>
          <input
            type="date"
            value={customEnd}
            onChange={(e) => setCustomEnd(e.target.value)}
            placeholder="End Date"
          />
        </div>
      )}
    </div>
  )
}

export default DateRangeFilter

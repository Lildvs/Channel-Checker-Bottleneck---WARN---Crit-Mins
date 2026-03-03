import { ReactNode } from 'react'

interface Column<T> {
  key: keyof T | string
  header: string
  width?: string
  render?: (value: T[keyof T], row: T) => ReactNode
}

interface DataTableProps<T> {
  data: T[]
  columns: Column<T>[]
  isLoading?: boolean
  emptyMessage?: string
  onRowClick?: (row: T) => void
  pagination?: {
    page: number
    pageSize: number
    total: number
    hasMore: boolean
    onPageChange: (page: number) => void
  }
}

function DataTable<T extends Record<string, unknown>>({
  data,
  columns,
  isLoading = false,
  emptyMessage = 'No data available',
  onRowClick,
  pagination,
}: DataTableProps<T>) {
  if (isLoading) {
    return <div className="data-table__loading">Loading...</div>
  }

  if (data.length === 0) {
    return <div className="data-table__empty">{emptyMessage}</div>
  }

  const totalPages = pagination ? Math.ceil(pagination.total / pagination.pageSize) : 1

  return (
    <div className="data-table-container">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={String(col.key)} style={{ width: col.width }}>
                {col.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, rowIndex) => (
            <tr
              key={rowIndex}
              onClick={() => onRowClick?.(row)}
              className={onRowClick ? 'data-table__row--clickable' : ''}
            >
              {columns.map((col) => (
                <td key={String(col.key)}>
                  {col.render
                    ? col.render(row[col.key as keyof T], row)
                    : String(row[col.key as keyof T] ?? '')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>

      {pagination && (
        <div className="data-table__pagination">
          <button
            className="btn btn-secondary"
            disabled={pagination.page <= 1}
            onClick={() => pagination.onPageChange(pagination.page - 1)}
          >
            Previous
          </button>
          <span className="pagination-info">
            Page {pagination.page} of {totalPages} ({pagination.total} items)
          </span>
          <button
            className="btn btn-secondary"
            disabled={!pagination.hasMore}
            onClick={() => pagination.onPageChange(pagination.page + 1)}
          >
            Next
          </button>
        </div>
      )}
    </div>
  )
}

export default DataTable

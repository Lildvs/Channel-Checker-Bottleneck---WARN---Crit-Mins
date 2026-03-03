import { FC } from 'react'
import { Paper } from '../../api/research'
import PaperCard from './PaperCard'

interface PaperListProps {
  papers: Paper[]
  isLoading: boolean
  total: number
  page: number
  pageSize: number
  hasMore: boolean
  onPageChange: (page: number) => void
}

const PaperList: FC<PaperListProps> = ({
  papers,
  isLoading,
  total,
  page,
  pageSize,
  hasMore,
  onPageChange,
}) => {
  if (isLoading) {
    return <div className="loading">Loading papers...</div>
  }

  if (papers.length === 0) {
    return (
      <div className="empty-state">
        <p>No papers found matching your criteria.</p>
      </div>
    )
  }

  const totalPages = Math.ceil(total / pageSize)

  return (
    <div className="paper-list-container">
      <div className="paper-list">
        {papers.map((paper) => (
          <PaperCard key={paper.id} paper={paper} />
        ))}
      </div>

      {/* Pagination */}
      <div className="pagination">
        <button
          className="btn btn-secondary"
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
        >
          Previous
        </button>
        <span className="page-info">
          Page {page} of {totalPages} ({total} papers)
        </span>
        <button
          className="btn btn-secondary"
          disabled={!hasMore}
          onClick={() => onPageChange(page + 1)}
        >
          Next
        </button>
      </div>
    </div>
  )
}

export default PaperList

import { FC } from 'react'
import { Paper } from '../../api/research'

interface PaperCardProps {
  paper: Paper
}

const PaperCard: FC<PaperCardProps> = ({ paper }) => {
  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    })
  }

  const typeClass = paper.research_type.replace('_', '-')

  return (
    <div className="paper-card">
      <h4>
        {paper.title}
        <span className={`type-badge ${typeClass}`}>
          {paper.research_type.replace('_', ' ')}
        </span>
      </h4>

      <div className="authors">
        {paper.authors.slice(0, 3).join(', ')}
        {paper.authors.length > 3 && ` +${paper.authors.length - 3} more`}
      </div>

      <div className="paper-meta">
        <span>{formatDate(paper.published_date)}</span>
        <span>Score: {(paper.quick_score * 100).toFixed(0)}%</span>
        <span>Citations: {paper.citation_count}</span>
        {paper.code_url && <span className="has-code">Has Code</span>}
      </div>

      <div className="topics">
        {paper.topics.slice(0, 5).map((topic) => (
          <span key={topic} className="topic-tag">
            {topic.replace('_', ' ')}
          </span>
        ))}
      </div>

      <div className="paper-actions">
        <a
          href={paper.url}
          target="_blank"
          rel="noopener noreferrer"
          className="btn btn-text"
        >
          View Paper
        </a>
        {paper.pdf_url && (
          <a
            href={paper.pdf_url}
            target="_blank"
            rel="noopener noreferrer"
            className="btn btn-text"
          >
            PDF
          </a>
        )}
        {paper.code_url && (
          <a
            href={paper.code_url}
            target="_blank"
            rel="noopener noreferrer"
            className="btn btn-text"
          >
            Code
          </a>
        )}
      </div>
    </div>
  )
}

export default PaperCard

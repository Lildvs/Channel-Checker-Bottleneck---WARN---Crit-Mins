import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchPapers, fetchResearchStats, fetchTopics } from '../api/research'
import PaperList from '../components/research/PaperList'
import TopicDistribution from '../components/research/TopicDistribution'
import ResearchTypeFilter from '../components/research/ResearchTypeFilter'

function ResearchDashboard() {
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null)
  const [selectedType, setSelectedType] = useState<string | null>(null)
  const [page, setPage] = useState(1)

  const { data: stats } = useQuery({
    queryKey: ['researchStats'],
    queryFn: fetchResearchStats,
  })

  const { data: topicsData } = useQuery({
    queryKey: ['researchTopics'],
    queryFn: fetchTopics,
  })

  const { data: papersData, isLoading } = useQuery({
    queryKey: ['papers', page, selectedTopic, selectedType, searchTerm],
    queryFn: () => fetchPapers({
      page,
      page_size: 20,
      topic: selectedTopic || undefined,
      research_type: selectedType || undefined,
      search: searchTerm || undefined,
    }),
  })

  useEffect(() => {
    setPage(1)
  }, [selectedTopic, selectedType, searchTerm])

  const handleTopicClick = (topic: string) => {
    setSelectedTopic(topic === selectedTopic ? null : topic)
  }

  return (
    <div className="page research-dashboard">
      <header className="page-header">
        <h2>Research Paper Dashboard</h2>
        <p>Browse and analyze collected academic papers by topic and research type</p>
      </header>

      {/* Stats row */}
      {stats && (
        <div className="research-stats">
          <div className="stat-card">
            <div className="value">{stats.total_papers.toLocaleString()}</div>
            <div className="label">Total Papers</div>
          </div>
          <div className="stat-card">
            <div className="value">{stats.papers_last_7_days}</div>
            <div className="label">Last 7 Days</div>
          </div>
          <div className="stat-card">
            <div className="value">{stats.contrarian_count}</div>
            <div className="label">Contrarian</div>
          </div>
          <div className="stat-card">
            <div className="value">{stats.emerging_count}</div>
            <div className="label">Emerging</div>
          </div>
          <div className="stat-card">
            <div className="value">{(stats.avg_quick_score * 100).toFixed(0)}%</div>
            <div className="label">Avg Score</div>
          </div>
          <div className="stat-card">
            <div className="value">{stats.topics_covered}</div>
            <div className="label">Topics</div>
          </div>
        </div>
      )}

      <div className="research-content">
        {/* Papers panel */}
        <div className="papers-panel">
          {/* Filters */}
          <div className="paper-filters">
            <input
              type="text"
              placeholder="Search papers..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
            <ResearchTypeFilter
              selected={selectedType}
              onChange={setSelectedType}
            />
            {selectedTopic && (
              <button
                className="btn btn-text"
                onClick={() => setSelectedTopic(null)}
              >
                Clear topic: {selectedTopic}
              </button>
            )}
          </div>

          {/* Paper list */}
          <PaperList
            papers={papersData?.papers || []}
            isLoading={isLoading}
            total={papersData?.total || 0}
            page={page}
            pageSize={20}
            hasMore={papersData?.has_more || false}
            onPageChange={setPage}
          />
        </div>

        {/* Visualization panel */}
        <div className="visualization-panel">
          <h3>Topic Distribution</h3>
          {topicsData && (
            <TopicDistribution
              topics={topicsData.topics}
              selectedTopic={selectedTopic}
              onTopicClick={handleTopicClick}
            />
          )}
        </div>
      </div>
    </div>
  )
}

export default ResearchDashboard

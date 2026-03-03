import { useState, useEffect, useCallback } from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Navigation from './components/Navigation'
import Home from './pages/Home'
import Dashboard from './pages/Dashboard'
import Bottlenecks from './pages/Bottlenecks'
import SectorDependencies from './pages/SectorDependencies'
import DataExplorer from './pages/DataExplorer'
import Forecasts from './pages/Forecasts'
import WarnNotices from './pages/WarnNotices'
import ResearchDashboard from './pages/ResearchDashboard'
import CommodityFlows from './pages/CommodityFlows'
import Reports from './pages/Reports'
import GoCometCreditModal from './components/shared/GoCometCreditModal'

function App() {
  const [gocometModalOpen, setGocometModalOpen] = useState(false)

  const handleSSEMessage = useCallback((event: MessageEvent) => {
    try {
      const data = JSON.parse(event.data)
      if (data.type === 'gocomet_credit_prompt') {
        setGocometModalOpen(true)
      }
    } catch {
      // non-JSON SSE messages are fine to ignore
    }
  }, [])

  useEffect(() => {
    const eventSource = new EventSource('/api/bottlenecks/stream/alerts')
    eventSource.onmessage = handleSSEMessage
    eventSource.onerror = () => {
      eventSource.close()
    }
    return () => eventSource.close()
  }, [handleSSEMessage])

  return (
    <BrowserRouter>
      <div className="app">
        <Navigation />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/bottlenecks" element={<Bottlenecks />} />
            <Route path="/sectors" element={<SectorDependencies />} />
            <Route path="/data" element={<DataExplorer />} />
            <Route path="/forecasts" element={<Forecasts />} />
            <Route path="/warn" element={<WarnNotices />} />
            <Route path="/research" element={<ResearchDashboard />} />
            <Route path="/flows" element={<CommodityFlows />} />
            <Route path="/reports" element={<Reports />} />
          </Routes>
        </main>
        <GoCometCreditModal
          isOpen={gocometModalOpen}
          onClose={() => setGocometModalOpen(false)}
        />
      </div>
    </BrowserRouter>
  )
}

export default App

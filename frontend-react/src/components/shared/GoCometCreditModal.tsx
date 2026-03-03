import { FC, useState, useEffect, useCallback } from 'react'
import {
  getGoCometCredits,
  confirmGoCometQuery,
  declineGoCometQuery,
  type GoCometCreditStatus,
} from '../../api/collectors'

interface GoCometCreditModalProps {
  isOpen: boolean
  onClose: () => void
}

const GoCometCreditModal: FC<GoCometCreditModalProps> = ({ isOpen, onClose }) => {
  const [credits, setCredits] = useState<GoCometCreditStatus | null>(null)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<string | null>(null)

  const fetchCredits = useCallback(async () => {
    try {
      const status = await getGoCometCredits()
      setCredits(status)
    } catch {
      setCredits(null)
    }
  }, [])

  useEffect(() => {
    if (isOpen) {
      fetchCredits()
      setResult(null)
    }
  }, [isOpen, fetchCredits])

  const handleConfirm = async () => {
    setLoading(true)
    try {
      const response = await confirmGoCometQuery()
      setResult(response.message)
      await fetchCredits()
    } catch {
      setResult('Query failed. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  const handleDecline = async () => {
    setLoading(true)
    try {
      const response = await declineGoCometQuery()
      setResult(response.message)
    } catch {
      setResult('Error. Credits preserved.')
    } finally {
      setLoading(false)
      setTimeout(onClose, 1500)
    }
  }

  if (!isOpen) return null

  const formatRefreshTime = (hours: number) => {
    if (hours >= 48) return `${Math.round(hours / 24)} days`
    if (hours >= 24) return `1 day, ${hours - 24}h`
    return `${hours} hours`
  }

  return (
    <div className="gocomet-modal-overlay" onClick={onClose}>
      <div
        className="gocomet-modal"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="gocomet-modal__header">
          <h3>GoComet Port Congestion Query</h3>
          <button className="gocomet-modal__close" onClick={onClose}>
            &times;
          </button>
        </div>

        <div className="gocomet-modal__body">
          {result ? (
            <div className="gocomet-modal__result">
              <p>{result}</p>
            </div>
          ) : credits ? (
            <>
              <div className="gocomet-modal__credits">
                <div className="gocomet-modal__credit-display">
                  <span className="gocomet-modal__credit-count">
                    {credits.remaining_credits}
                  </span>
                  <span className="gocomet-modal__credit-label">
                    / {credits.total_credits} credits remaining
                  </span>
                </div>
                <div className="gocomet-modal__refresh-info">
                  Refreshes in {formatRefreshTime(credits.hours_until_refresh)} ({credits.refresh_day})
                </div>
              </div>

              <p className="gocomet-modal__question">
                Run a GoComet port congestion query? This will use 1 credit.
              </p>

              <div className="gocomet-modal__actions">
                <button
                  className="gocomet-modal__btn gocomet-modal__btn--confirm"
                  onClick={handleConfirm}
                  disabled={loading || credits.remaining_credits <= 0}
                >
                  {loading ? 'Querying...' : 'Yes, Run Query'}
                </button>
                <button
                  className="gocomet-modal__btn gocomet-modal__btn--decline"
                  onClick={handleDecline}
                  disabled={loading}
                >
                  No, Skip
                </button>
              </div>
            </>
          ) : (
            <p>Loading credit status...</p>
          )}
        </div>
      </div>
    </div>
  )
}

export default GoCometCreditModal

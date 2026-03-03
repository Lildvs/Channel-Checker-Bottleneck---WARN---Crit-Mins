import { FC } from 'react'

interface LoadingSpinnerProps {
  size?: 'small' | 'medium' | 'large'
  message?: string
}

const LoadingSpinner: FC<LoadingSpinnerProps> = ({
  size = 'medium',
  message = 'Loading...',
}) => {
  const sizeClasses = {
    small: 'loading-spinner--small',
    medium: 'loading-spinner--medium',
    large: 'loading-spinner--large',
  }

  return (
    <div className={`loading-spinner ${sizeClasses[size]}`}>
      <div className="loading-spinner__circle" />
      {message && <span className="loading-spinner__message">{message}</span>}
    </div>
  )
}

export default LoadingSpinner

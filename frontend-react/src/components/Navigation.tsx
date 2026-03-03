import { FC, useState } from 'react'
import { NavLink } from 'react-router-dom'

const Navigation: FC = () => {
  const [isMenuOpen, setIsMenuOpen] = useState(false)

  const navItems = [
    { path: '/', label: 'Home', icon: '🏠' },
    { path: '/dashboard', label: 'Dashboard', icon: '📈' },
    { path: '/bottlenecks', label: 'Bottlenecks', icon: '🚧' },
    { path: '/sectors', label: 'Sectors', icon: '🏭' },
    { path: '/data', label: 'Data Explorer', icon: '📊' },
    { path: '/forecasts', label: 'Forecasts', icon: '🔮' },
    { path: '/warn', label: 'WARN Notices', icon: '⚠️' },
    { path: '/research', label: 'Research', icon: '📚' },
    { path: '/flows', label: 'Trade Flows', icon: '🌍' },
    { path: '/reports', label: 'Reports', icon: '📋' },
  ]

  return (
    <nav className="main-nav">
      <div className="nav-brand">
        <NavLink to="/">
          <h1>Channel Check</h1>
        </NavLink>
      </div>

      <button
        className="nav-toggle"
        onClick={() => setIsMenuOpen(!isMenuOpen)}
        aria-label="Toggle navigation"
      >
        {isMenuOpen ? '✕' : '☰'}
      </button>

      <ul className={`nav-links ${isMenuOpen ? 'nav-links--open' : ''}`}>
        {navItems.map((item) => (
          <li key={item.path}>
            <NavLink
              to={item.path}
              className={({ isActive }) => isActive ? 'active' : ''}
              onClick={() => setIsMenuOpen(false)}
            >
              <span className="nav-icon">{item.icon}</span>
              <span className="nav-label">{item.label}</span>
            </NavLink>
          </li>
        ))}
      </ul>
    </nav>
  )
}

export default Navigation

import { useState, useEffect } from 'react'

const POLL_INTERVAL = 5000

export function useQueueStats() {
  const [queueStats, setQueueStats] = useState({
    high: 0,
    medium: 0,
    low: 0,
    dlq: 0,
    total: 0,
  })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const res = await fetch('/api/v1/queue/stats')
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        setQueueStats(data)
        setError(null)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }

    // Fetch immediately on mount
    fetchStats()

    // Then poll every 5 seconds
    const interval = setInterval(fetchStats, POLL_INTERVAL)
    return () => clearInterval(interval)
  }, [])

  return { queueStats, loading, error }
}
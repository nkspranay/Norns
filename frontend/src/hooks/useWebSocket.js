import { useState, useEffect, useRef, useCallback } from 'react'

const WS_URL = 'ws://localhost:8000/ws'
const RECONNECT_DELAY = 3000

export function useWebSocket() {
  const [isConnected, setIsConnected] = useState(false)
  const [jobEvents, setJobEvents] = useState([])
  const [workerEvents, setWorkerEvents] = useState({})
  const [stats, setStats] = useState({
    total: 0,
    running: 0,
    completed: 0,
    failed: 0,
  })

  const wsRef = useRef(null)
  const reconnectTimer = useRef(null)

  // ── Keep a ref to the latest handler so ws.onmessage never captures a stale closure ──
  // This is the root cause of stats not updating: ws.onmessage was bound once
  // at connect() time and held the initial (empty) state forever.
  const handleEventRef = useRef(null)

  // Update the ref every render so it always points to the freshest handler
  handleEventRef.current = useCallback((data) => {
    if (data.channel === 'norns:jobs') {
      // Add to event feed — newest first, max 100 events
      setJobEvents(prev => {
        const newEvent = {
          ...data,
          id: `${data.job_id}-${Date.now()}`,
          receivedAt: new Date().toISOString(),
        }
        return [newEvent, ...prev].slice(0, 100)
      })

      // Update summary stats
      setStats(prev => {
        const next = { ...prev }
        if (data.event === 'job_queued') {
          next.total = prev.total + 1
        } else if (data.event === 'job_started') {
          next.running = prev.running + 1
        } else if (data.event === 'job_completed') {
          next.completed = prev.completed + 1
          next.running = Math.max(0, prev.running - 1)
        } else if (data.event === 'job_dead') {
          next.failed = prev.failed + 1
          next.running = Math.max(0, prev.running - 1)
        } else if (data.event === 'job_retrying') {
          next.running = Math.max(0, prev.running - 1)
        }
        return next
      })
    } else if (data.channel === 'norns:workers') {
      setWorkerEvents(prev => ({
        ...prev,
        [data.worker_id]: {
          ...data,
          lastSeen: new Date().toISOString(),
        }
      }))
    }
  }, []) // no deps needed — all setters are stable references

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    const ws = new WebSocket(WS_URL)
    wsRef.current = ws

    ws.onopen = () => {
      setIsConnected(true)
      if (reconnectTimer.current) {
        clearTimeout(reconnectTimer.current)
        reconnectTimer.current = null
      }
    }

    ws.onclose = () => {
      setIsConnected(false)
      reconnectTimer.current = setTimeout(connect, RECONNECT_DELAY)
    }

    ws.onerror = () => {
      setIsConnected(false)
      ws.close()
    }

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        // Always call through the ref — gets the latest handler regardless
        // of when this closure was created. This is the fix.
        handleEventRef.current(data)
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e)
      }
    }
  }, []) // connect itself stays stable — it reads the ref, not the handler directly

  useEffect(() => {
    connect()
    return () => {
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current)
      if (wsRef.current) wsRef.current.close()
    }
  }, [connect])

  return {
    isConnected,
    jobEvents,
    workerEvents: Object.values(workerEvents),
    stats,
  }
}
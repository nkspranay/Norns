export function QueueStats({ queueStats }) {
  const queues = [
    { key: 'high', label: 'High', color: 'bg-red-500' },
    { key: 'medium', label: 'Medium', color: 'bg-amber-500' },
    { key: 'low', label: 'Low', color: 'bg-emerald-500' },
    { key: 'dlq', label: 'DLQ', color: 'bg-zinc-500' },
  ]

  const max = Math.max(
    queueStats.high + queueStats.medium + queueStats.low + queueStats.dlq,
    1
  )

  return (
    <div className="bg-zinc-800 border border-zinc-700 rounded-xl p-5">
      <p className="text-xs text-zinc-500 uppercase tracking-widest mb-4">
        Queue Depth
      </p>
      <div className="space-y-3">
        {queues.map(({ key, label, color }) => {
          const value = queueStats[key] || 0
          const pct = Math.max((value / max) * 100, value > 0 ? 2 : 0)
          return (
            <div key={key}>
              <div className="flex justify-between text-xs text-zinc-400 mb-1">
                <span>{label}</span>
                <span>{value}</span>
              </div>
              <div className="h-1.5 bg-zinc-700 rounded-full overflow-hidden">
                <div
                  className={`h-1.5 ${color} rounded-full transition-all duration-500`}
                  style={{ width: `${pct}%` }}
                />
              </div>
            </div>
          )
        })}
      </div>
      <p className="text-xs text-zinc-600 mt-4">
        Total waiting: {(queueStats.high || 0) + (queueStats.medium || 0) + (queueStats.low || 0)}
      </p>
    </div>
  )
}
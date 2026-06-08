export function WorkerPanel({ workers }) {
  if (workers.length === 0) {
    return (
      <div className="bg-zinc-800 border border-zinc-700 rounded-xl p-5">
        <p className="text-xs text-zinc-500 uppercase tracking-widest mb-4">
          Workers
        </p>
        <p className="text-zinc-600 text-sm text-center py-6">
          No workers connected
        </p>
      </div>
    )
  }

  return (
    <div className="bg-zinc-800 border border-zinc-700 rounded-xl p-5">
      <p className="text-xs text-zinc-500 uppercase tracking-widest mb-4">
        Workers
        <span className="ml-2 bg-zinc-700 text-zinc-300 px-2 py-0.5 rounded-full text-xs">
          {workers.length}
        </span>
      </p>
      <div className="space-y-3">
        {workers.map((worker) => {
          const isActive = worker.status === 'running' || worker.current_job_id
          const dotColor = isActive
            ? 'bg-emerald-400 shadow-emerald-400/50 shadow-sm'
            : 'bg-amber-400'

          return (
            <div
              key={worker.worker_id}
              className="flex items-start gap-3 p-3 bg-zinc-900 rounded-lg"
            >
              <div className={`w-2.5 h-2.5 rounded-full mt-1 flex-shrink-0 ${dotColor}`} />
              <div className="min-w-0">
                <p className="text-sm text-zinc-200 font-medium truncate">
                  {worker.worker_name || `worker-${worker.worker_id?.slice(0, 8)}`}
                </p>
                <p className="text-xs text-zinc-500 truncate">
                  {worker.current_job_id
                    ? `Running ${worker.current_job_id.slice(0, 8)}...`
                    : 'Idle — waiting for jobs'}
                </p>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
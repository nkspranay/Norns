const EVENT_STYLES = {
  job_queued: {
    badge: 'bg-zinc-700 text-zinc-300',
    label: 'QUEUED',
  },
  job_started: {
    badge: 'bg-violet-900 text-violet-300',
    label: 'RUNNING',
  },
  job_completed: {
    badge: 'bg-emerald-900 text-emerald-300',
    label: 'COMPLETED',
  },
  job_retrying: {
    badge: 'bg-amber-900 text-amber-300',
    label: 'RETRYING',
  },
  job_dead: {
    badge: 'bg-red-900 text-red-300',
    label: 'DEAD',
  },
}

function formatTime(isoString) {
  return new Date(isoString).toLocaleTimeString()
}

export function JobFeed({ events }) {
  if (events.length === 0) {
    return (
      <div className="bg-zinc-800 border border-zinc-700 rounded-xl p-5 flex-1">
        <p className="text-xs text-zinc-500 uppercase tracking-widest mb-4">
          Live Job Events
        </p>
        <p className="text-zinc-600 text-sm text-center py-12">
          Waiting for jobs...
        </p>
      </div>
    )
  }

  return (
    <div className="bg-zinc-800 border border-zinc-700 rounded-xl p-5 flex-1">
      <p className="text-xs text-zinc-500 uppercase tracking-widest mb-4">
        Live Job Events
        <span className="ml-2 bg-zinc-700 text-zinc-300 px-2 py-0.5 rounded-full text-xs">
          {events.length}
        </span>
      </p>
      <div className="space-y-2 max-h-96 overflow-y-auto pr-1">
        {events.map((event) => {
          const style = EVENT_STYLES[event.event] || {
            badge: 'bg-zinc-700 text-zinc-400',
            label: event.event?.toUpperCase() || 'EVENT',
          }
          return (
            <div
              key={event.id}
              className="flex items-start gap-3 p-3 bg-zinc-900 rounded-lg animate-fade-in"
            >
              <span className={`text-xs font-bold px-2 py-1 rounded flex-shrink-0 ${style.badge}`}>
                {style.label}
              </span>
              <div className="min-w-0 flex-1">
                <p className="text-sm text-zinc-200 font-mono truncate">
                  {event.job_id?.slice(0, 8)}...
                  <span className="text-zinc-500 ml-2 font-sans text-xs">
                    {event.job_type}
                  </span>
                </p>
                {event.worker_id && (
                  <p className="text-xs text-zinc-600 truncate">
                    worker-{event.worker_id.slice(0, 8)}
                  </p>
                )}
                {event.error && (
                  <p className="text-xs text-red-500 truncate mt-0.5">
                    {event.error}
                  </p>
                )}
              </div>
              <span className="text-xs text-zinc-600 flex-shrink-0">
                {formatTime(event.receivedAt)}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
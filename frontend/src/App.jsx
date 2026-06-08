import { useWebSocket } from './hooks/useWebSocket'
import { useQueueStats } from './hooks/useQueueStats'
import { StatCards } from './components/StatCards'
import { QueueStats } from './components/QueueStats'
import { WorkerPanel } from './components/WorkerPanel'
import { JobFeed } from './components/JobFeed'
import { JobSubmitForm } from './components/JobSubmitForm'

export default function App() {
  const { isConnected, jobEvents, workerEvents, stats } = useWebSocket()
  const { queueStats } = useQueueStats()

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 p-6">

      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            🔱 Norns
          </h1>
          <p className="text-xs text-zinc-500 mt-0.5">
            Distributed Job Scheduler
          </p>
        </div>
        <div className="flex items-center gap-2 text-xs px-3 py-1.5 rounded-full bg-zinc-800 border border-zinc-700">
          <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-emerald-400' : 'bg-red-400'}`} />
          <span className="text-zinc-400">
            {isConnected ? 'Connected' : 'Reconnecting...'}
          </span>
        </div>
      </div>

      {/* Stat cards */}
      <div className="mb-6">
        <StatCards stats={stats} />
      </div>

      {/* Main grid */}
      <div className="grid grid-cols-3 gap-4 mb-4">
        <div className="col-span-2 flex flex-col gap-4">
          <JobFeed events={jobEvents} />
        </div>
        <div className="flex flex-col gap-4">
          <QueueStats queueStats={queueStats} />
          <WorkerPanel workers={workerEvents} />
          <JobSubmitForm />
        </div>
      </div>

    </div>
  )
}
export function StatCards({ stats }) {
  const cards = [
    {
      label: 'Total Jobs',
      value: stats.total,
      color: 'text-white',
      bg: 'bg-zinc-800',
      border: 'border-zinc-700',
    },
    {
      label: 'Running',
      value: stats.running,
      color: 'text-violet-400',
      bg: 'bg-zinc-800',
      border: 'border-violet-800',
    },
    {
      label: 'Completed',
      value: stats.completed,
      color: 'text-emerald-400',
      bg: 'bg-zinc-800',
      border: 'border-emerald-800',
    },
    {
      label: 'Failed',
      value: stats.failed,
      color: 'text-red-400',
      bg: 'bg-zinc-800',
      border: 'border-red-800',
    },
  ]

  return (
    <div className="grid grid-cols-4 gap-4">
      {cards.map((card) => (
        <div
          key={card.label}
          className={`${card.bg} border ${card.border} rounded-xl p-5`}
        >
          <p className="text-xs text-zinc-500 uppercase tracking-widest mb-2">
            {card.label}
          </p>
          <p className={`text-4xl font-bold ${card.color}`}>
            {card.value}
          </p>
        </div>
      ))}
    </div>
  )
}
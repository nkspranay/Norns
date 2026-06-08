import { useState } from 'react'

const JOB_TYPES = ['email', 'report', 'data_pipeline', 'failing_job']
const PRIORITIES = ['high', 'medium', 'low']

export function JobSubmitForm() {
  const [form, setForm] = useState({
    name: '',
    job_type: 'email',
    priority: 'medium',
    max_retries: 3,
    payload: '{}',
  })
  const [status, setStatus] = useState(null) // 'success' | 'error' | null
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (e) => {
  e.preventDefault()
  setLoading(true)
  setStatus(null)

  try {
    let payload
    try {
      payload = JSON.parse(form.payload)
    } catch {
      setStatus('error')
      setLoading(false)
      return
    }

    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 30000)

    const res = await fetch('/api/v1/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: form.name,
        job_type: form.job_type,
        priority: form.priority,
        max_retries: Number(form.max_retries),
        payload,
      }),
      signal: controller.signal,
    })

    clearTimeout(timeout)

    if (res.ok) {
      setStatus('success')
      setForm(f => ({ ...f, name: '', payload: '{}' }))
    } else {
      setStatus('error')
    }
  } catch (err) {
    console.error('Submit error:', err)
    setStatus('error')
  } finally {
    setLoading(false)
    setTimeout(() => setStatus(null), 3000)
  }
}

  return (
    <div className="bg-zinc-800 border border-zinc-700 rounded-xl p-5">
      <p className="text-xs text-zinc-500 uppercase tracking-widest mb-4">
        Submit Job
      </p>
      <form onSubmit={handleSubmit} className="space-y-3">
        <input
          type="text"
          placeholder="Job name"
          value={form.name}
          onChange={e => setForm(f => ({ ...f, name: e.target.value }))}
          required
          className="w-full bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-zinc-200 placeholder-zinc-600 focus:outline-none focus:border-violet-600"
        />

        <div className="grid grid-cols-2 gap-3">
          <select
            value={form.job_type}
            onChange={e => setForm(f => ({ ...f, job_type: e.target.value }))}
            className="bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-zinc-200 focus:outline-none focus:border-violet-600"
          >
            {JOB_TYPES.map(t => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>

          <select
            value={form.priority}
            onChange={e => setForm(f => ({ ...f, priority: e.target.value }))}
            className="bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-zinc-200 focus:outline-none focus:border-violet-600"
          >
            {PRIORITIES.map(p => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-3">
          <label className="text-xs text-zinc-500 flex-shrink-0">
            Max retries
          </label>
          <input
            type="number"
            min={0}
            max={10}
            value={form.max_retries}
            onChange={e => setForm(f => ({ ...f, max_retries: e.target.value }))}
            className="w-20 bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-zinc-200 focus:outline-none focus:border-violet-600"
          />
        </div>

        <textarea
          placeholder='Payload JSON e.g. {"to": "user@example.com"}'
          value={form.payload}
          onChange={e => setForm(f => ({ ...f, payload: e.target.value }))}
          rows={3}
          className="w-full bg-zinc-900 border border-zinc-700 rounded-lg px-3 py-2 text-sm text-zinc-200 font-mono placeholder-zinc-600 focus:outline-none focus:border-violet-600 resize-none"
        />

        <button
          type="submit"
          disabled={loading}
          className="w-full bg-violet-700 hover:bg-violet-600 disabled:bg-zinc-700 text-white text-sm font-medium py-2 rounded-lg transition-colors"
        >
          {loading ? 'Submitting...' : 'Submit Job'}
        </button>

        {status === 'success' && (
          <p className="text-xs text-emerald-400 text-center">
            Job submitted successfully
          </p>
        )}
        {status === 'error' && (
          <p className="text-xs text-red-400 text-center">
            Failed to submit job
          </p>
        )}
      </form>
    </div>
  )
}
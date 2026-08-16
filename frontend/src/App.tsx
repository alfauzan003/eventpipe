import { FormEvent, useEffect, useState } from 'react'

interface EventEnvelope {
  eventId: string
  type: string
  payload: unknown
  timestamp: string
}

interface Counts {
  totalProcessed: number
  byType: Record<string, number>
}

type ConnectionState = 'connecting' | 'connected' | 'reconnecting'

const SSE_EVENT_NAME = 'event.processed'
const MAX_EVENTS = 50

function App() {
  const [events, setEvents] = useState<EventEnvelope[]>([])
  const [counts, setCounts] = useState<Counts | null>(null)
  const [connection, setConnection] = useState<ConnectionState>('connecting')
  const [error, setError] = useState<string | null>(null)
  const [type, setType] = useState('demo.event')
  const [payload, setPayload] = useState('{ "hello": "world" }')
  const [publishing, setPublishing] = useState(false)
  const [published, setPublished] = useState<EventEnvelope | null>(null)

  async function refreshCounts() {
    try {
      const res = await fetch('/api/events/counts')
      if (res.ok) {
        setCounts((await res.json()) as Counts)
      }
    } catch {
      // Transient; the next event or reload will retry.
    }
  }

  useEffect(() => {
    void refreshCounts()

    const stream = new EventSource('/api/events/stream')
    stream.onopen = () => setConnection('connected')
    stream.onerror = () =>
      setConnection((current) => (current === 'connected' ? 'reconnecting' : current))

    function onProcessed(e: MessageEvent) {
      try {
        const envelope = JSON.parse(e.data) as EventEnvelope
        setEvents((prev) => [envelope, ...prev].slice(0, MAX_EVENTS))
        void refreshCounts()
      } catch {
        // Ignore malformed frames.
      }
    }
    stream.addEventListener(SSE_EVENT_NAME, onProcessed)

    return () => {
      stream.removeEventListener(SSE_EVENT_NAME, onProcessed)
      stream.close()
    }
  }, [])

  async function publish(e: FormEvent) {
    e.preventDefault()
    setPublishing(true)
    setError(null)
    setPublished(null)
    let parsedPayload: unknown
    try {
      parsedPayload = JSON.parse(payload)
    } catch {
      parsedPayload = payload
    }
    try {
      const res = await fetch('/api/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: type.trim(), payload: parsedPayload }),
      })
      if (!res.ok) {
        throw new Error(`publish failed (HTTP ${res.status})`)
      }
      setPublished((await res.json()) as EventEnvelope)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'publish failed')
    } finally {
      setPublishing(false)
    }
  }

  const byTypeEntries = counts ? Object.entries(counts.byType) : []

  return (
    <main className="page">
      <header className="header">
        <h1>EventPipe</h1>
        <span className={`badge ${connection}`}>
          {connection === 'connected'
            ? 'live'
            : connection === 'reconnecting'
              ? 'reconnecting…'
              : 'connecting…'}
        </span>
      </header>

      <section className="grid">
        <div className="card">
          <h2>Counts</h2>
          <p className="total">
            {counts ? counts.totalProcessed : '—'} <small>processed</small>
          </p>
          {byTypeEntries.length > 0 && (
            <ul className="byType">
              {byTypeEntries.map(([typeName, count]) => (
                <li key={typeName}>
                  <span className="typeTag">{typeName}</span>
                  <span>{count}</span>
                </li>
              ))}
            </ul>
          )}
          {counts && byTypeEntries.length === 0 && (
            <p className="muted">No events processed yet.</p>
          )}
        </div>

        <div className="card">
          <h2>Publish</h2>
          <form onSubmit={publish} className="publishForm">
            <label>
              Type
              <input
                value={type}
                onChange={(e) => setType(e.target.value)}
                placeholder="order.created"
                required
              />
            </label>
            <label>
              Payload (JSON)
              <textarea
                value={payload}
                onChange={(e) => setPayload(e.target.value)}
                rows={3}
                required
              />
            </label>
            <button type="submit" disabled={publishing || connection === 'connecting'}>
              {publishing ? 'Publishing…' : 'Publish'}
            </button>
            {published && (
              <p className="ok">
                Published <span className="typeTag">{published.type}</span>{' '}
                <small>{published.eventId}</small>
              </p>
            )}
            {error && <p className="err">{error}</p>}
          </form>
        </div>
      </section>

      <section className="card">
        <h2>Live feed</h2>
        {events.length === 0 && <p className="muted">Waiting for events…</p>}
        <ul className="feed">
          {events.map((envelope) => (
            <li key={envelope.eventId}>
              <div className="feedRow">
                <span className="typeTag">{envelope.type}</span>
                <span className="muted">{new Date(envelope.timestamp).toLocaleString()}</span>
              </div>
              <div className="muted">id: {envelope.eventId}</div>
              <pre>{JSON.stringify(envelope.payload, null, 2)}</pre>
            </li>
          ))}
        </ul>
      </section>
    </main>
  )
}

export default App

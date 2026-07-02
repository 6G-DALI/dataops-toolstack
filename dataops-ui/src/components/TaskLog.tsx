import { useEffect, useState } from 'react'
import { getTaskLogs } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import type { LogContentEntry, LogEntry, TaskLogResponse } from '../types'
import '../styles/TaskLog.css'

function parseLog(text: string): LogEntry[] {
  // Airflow v2 returns {"content": [...], "continuation_token": "..."}
  try {
    const obj = JSON.parse(text) as TaskLogResponse
    if (Array.isArray(obj.content)) {
      return obj.content
        .filter(e => e.event !== '::endgroup::')
        .map(e => {
          const isGroup = e.event?.startsWith('::group::') ?? false
          return {
            timestamp: e.timestamp ?? null,
            level: e.level ? e.level.toUpperCase() : null,
            message: isGroup
              ? (e.event ?? '').replace(/^::group::/, '').trim()
              : (e.event ?? ''),
            logger: e.logger ?? null,
            source: e.filename ? `${e.filename}:${e.lineno ?? ''}` : null,
            isGroup,
            raw: false,
          }
        })
    }
  } catch {
    // fall through to line-by-line parsing below
  }

  // Fallback: try each line as JSON
  return text.split('\n').flatMap<LogEntry>(line => {
    const trimmed = line.trim()
    if (!trimmed) return []
    try {
      const obj = JSON.parse(trimmed) as LogContentEntry
      return [{
        timestamp: obj.timestamp ?? obj.time ?? null,
        level: obj.level ? obj.level.toUpperCase() : null,
        message: obj.event ?? obj.message ?? obj.msg ?? trimmed,
        logger: obj.logger ?? null,
        source: obj.filename ? `${obj.filename}:${obj.lineno ?? ''}` : null,
        isGroup: false,
        raw: false,
      }]
    } catch {
      return [{ timestamp: null, level: null, message: trimmed, logger: null, source: null, isGroup: false, raw: true }]
    }
  })
}

function formatTimestamp(ts: string | null): string | null {
  if (!ts) return null
  try {
    return new Date(ts).toISOString().replace('T', ' ').replace('Z', '').slice(0, 23)
  } catch {
    return ts
  }
}

const LEVEL_CLASS: Record<string, string> = {
  ERROR:    'log-level-error',
  CRITICAL: 'log-level-error',
  WARNING:  'log-level-warning',
  WARN:     'log-level-warning',
  INFO:     'log-level-info',
  DEBUG:    'log-level-debug',
}

interface TaskLogProps {
  dagId: string
  runId: string
  taskId: string
  tryNumber?: number
}

export default function TaskLog({ dagId, runId, taskId, tryNumber = 1 }: TaskLogProps) {
  const [entries, setEntries] = useState<LogEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getTaskLogs(dagId, runId, taskId, tryNumber)
      .then(text => setEntries(parseLog(text ?? '')))
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [dagId, runId, taskId, tryNumber])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  const lineCount = entries.filter(e => !e.isGroup).length

  return (
    <div>
      <p className="text-muted">
        Task: <strong>{taskId}</strong> &nbsp;|&nbsp; Try: {tryNumber}
        &nbsp;|&nbsp; {lineCount} line{lineCount !== 1 ? 's' : ''}
      </p>
      <div className="log-container">
        {entries.length === 0 ? (
          <span className="log-empty">(empty log)</span>
        ) : (
          entries.map((entry, i) => {
            if (entry.isGroup) {
              return (
                <div key={i} className="log-group-header">
                  <span className="log-group-label">{entry.message}</span>
                </div>
              )
            }
            return (
              <div key={i} className={`log-entry${entry.raw ? ' log-entry-raw' : ''}`}>
                <span className="log-index">{String(i + 1).padStart(4, '0')}</span>
                {entry.timestamp && (
                  <span className="log-timestamp">{formatTimestamp(entry.timestamp)}</span>
                )}
                {entry.level && (
                  <span className={`log-level ${LEVEL_CLASS[entry.level] ?? 'log-level-info'}`}>
                    {entry.level}
                  </span>
                )}
                <span className="log-message">{entry.message}</span>
                {entry.source && (
                  <span className="log-source">{entry.source}</span>
                )}
              </div>
            )
          })
        )}
      </div>
    </div>
  )
}

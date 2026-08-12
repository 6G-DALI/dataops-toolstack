import { useEffect, useState } from 'react'
import { getStats } from '../api/airflow'
import StateBadge from './StateBadge'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import '../styles/Button.css'

function StatCard({ label, value, sub, color, onClick }) {
  return (
    <div
      onClick={onClick}
      style={{
        background: '#fff',
        border: '1px solid var(--dali-gray)',
        borderRadius: '8px',
        padding: '20px 24px',
        boxShadow: '0 1px 4px rgba(0,0,0,0.07)',
        borderLeft: `4px solid ${color}`,
        cursor: onClick ? 'pointer' : 'default',
        transition: 'box-shadow 0.15s',
      }}
      onMouseEnter={e => onClick && (e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.12)')}
      onMouseLeave={e => onClick && (e.currentTarget.style.boxShadow = '0 1px 4px rgba(0,0,0,0.07)')}
    >
      <div style={{ fontSize: '13px', color: 'var(--dali-muted)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.4px', marginBottom: '8px' }}>
        {label}
      </div>
      <div style={{ fontSize: '36px', fontWeight: 700, color: 'var(--dali-navy)', lineHeight: 1 }}>
        {value ?? '—'}
      </div>
      {sub && (
        <div style={{ fontSize: '12px', color: 'var(--dali-muted)', marginTop: '6px' }}>{sub}</div>
      )}
    </div>
  )
}

export default function HomePage({ onNavigate }) {
  const [stats, setStats] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    getStats()
      .then(setStats)
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  const { dags, tasks, run_states, recent_runs } = stats

  const runStateColors = {
    success: '#16a34a',
    failed: '#dc2626',
    running: 'var(--dali-blue)',
    queued: '#d97706',
  }

  return (
    <div>
      <div style={{ marginBottom: '28px' }}>
        <h1 className="page-title" style={{ marginBottom: '4px' }}>Dashboard</h1>
        <p className="page-subtitle" style={{ marginBottom: 0 }}>Overview of your 6G-DALI DataOps deployment</p>
      </div>

      {/* Stat cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '16px', marginBottom: '32px' }}>
        <StatCard
          label="Total DAGs"
          value={dags.total}
          sub={`${dags.active} active · ${dags.paused} paused`}
          color="var(--dali-blue)"
          onClick={() => onNavigate('dags', {})}
        />
        <StatCard
          label="Active DAGs"
          value={dags.active}
          color="#16a34a"
          onClick={() => onNavigate('dags', {})}
        />
        <StatCard
          label="Paused DAGs"
          value={dags.paused}
          color="var(--dali-muted)"
          onClick={() => onNavigate('dags', {})}
        />
        <StatCard
          label="Custom Tasks"
          value={tasks.custom}
          color="var(--dali-orange)"
          onClick={() => onNavigate('all-tasks', {})}
        />
        {Object.entries(run_states).map(([state, count]) => (
          <StatCard
            key={state}
            label={`Runs ${state}`}
            value={count}
            color={runStateColors[state] || 'var(--dali-gray)'}
          />
        ))}
      </div>

      {/* Recent runs */}
      {recent_runs.length > 0 && (
        <div>
          <h2 style={{ fontSize: '16px', fontWeight: 700, color: 'var(--dali-navy)', marginBottom: '12px' }}>
            Recent Runs
          </h2>
          <div style={{ background: '#fff', border: '1px solid var(--dali-gray)', borderRadius: '8px', boxShadow: '0 1px 4px rgba(0,0,0,0.07)', overflow: 'hidden' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
              <thead>
                <tr>
                  {['DAG', 'Run ID', 'Type', 'State', 'Started', 'Duration'].map(h => (
                    <th key={h} style={{ background: 'var(--dali-navy)', color: 'rgba(255,255,255,0.9)', padding: '10px 14px', textAlign: 'left', fontWeight: 600, borderBottom: '2px solid var(--dali-blue)', whiteSpace: 'nowrap' }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {recent_runs.map((run, i) => {
                  const started = run.start_date ? new Date(run.start_date) : null
                  const ended = run.end_date ? new Date(run.end_date) : null
                  const duration = started && ended
                    ? `${Math.round((ended - started) / 1000)}s`
                    : run.state === 'running' ? 'running…' : '—'
                  return (
                    <tr key={i} style={{ borderBottom: '1px solid #f0f2f5', transition: 'background 0.1s' }}
                      onMouseEnter={e => e.currentTarget.style.background = '#eef3fb'}
                      onMouseLeave={e => e.currentTarget.style.background = ''}>
                      <td style={{ padding: '9px 14px' }}>
                        <a onClick={() => onNavigate('runs', { dagId: run.dag_id })}>{run.dag_id}</a>
                      </td>
                      <td style={{ padding: '9px 14px', maxWidth: '220px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        <code style={{ fontSize: '11px' }} title={run.dag_run_id}>{run.dag_run_id}</code>
                      </td>
                      <td style={{ padding: '9px 14px' }}>{run.run_type || '—'}</td>
                      <td style={{ padding: '9px 14px' }}><StateBadge state={run.state} /></td>
                      <td style={{ padding: '9px 14px', whiteSpace: 'nowrap' }}>
                        {started ? started.toLocaleString() : '—'}
                      </td>
                      <td style={{ padding: '9px 14px' }}>{duration}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Quick actions */}
      <div style={{ marginTop: '32px', display: 'flex', gap: '10px' }}>
        <button className="btn btn-primary" onClick={() => onNavigate('dag-builder', {})}>+ Build DAG</button>
        <button className="btn btn-secondary" onClick={() => onNavigate('task-creator', {})}>+ Create Task</button>
        <button className="btn btn-secondary" onClick={() => onNavigate('datasets', {})}>View Datasets</button>
      </div>
    </div>
  )
}

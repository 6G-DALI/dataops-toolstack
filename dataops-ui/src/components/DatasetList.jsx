import { useEffect, useState } from 'react'
import { getDatasets } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import '../styles/Table.css'

export default function DatasetList() {
  const [datasets, setDatasets] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    getDatasets()
      .then(data => setDatasets(data.datasets || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <h1 className="page-title">Datasets</h1>
      <p className="page-subtitle">{datasets.length} dataset{datasets.length !== 1 ? 's' : ''} registered</p>
      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Project</th>
              <th>URI</th>
              <th>Format</th>
              <th>License</th>
              <th>Produced by</th>
              <th>Consumed by</th>
              <th>Last Updated</th>
            </tr>
          </thead>
          <tbody>
            {datasets.length === 0 ? (
              <tr>
                <td colSpan={8} style={{ textAlign: 'center', color: '#888', padding: '24px' }}>
                  No datasets found
                </td>
              </tr>
            ) : datasets.map(ds => (
              <tr key={ds.id}>
                <td>{ds.name || '—'}</td>
                <td>{ds.sns_project_name || '—'}</td>
                <td><code style={{ wordBreak: 'break-all' }}>{ds.uri}</code></td>
                <td>{ds.extra?.format || '—'}</td>
                <td>{ds.extra?.license || '—'}</td>
                <td>{(ds.producing_dags || []).map(d => d.dag_id).join(', ') || '—'}</td>
                <td>{(ds.consuming_dags || []).map(d => d.dag_id).join(', ') || '—'}</td>
                <td>{ds.updated_at ? new Date(ds.updated_at).toLocaleString() : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

import { useEffect, useState } from 'react'
import { getDatasets } from '../api/airflow'
import LoadingSpinner from './LoadingSpinner'
import ErrorMessage from './ErrorMessage'
import '../styles/Modal.css'
import '../styles/Button.css'

export default function TriggerModal({ dagId, onConfirm, onCancel }) {
  const [datasets, setDatasets] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    getDatasets()
      .then(data => setDatasets(data.datasets || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  return (
    <div className="modal-overlay" onClick={onCancel}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <span className="modal-title">Trigger DAG</span>
          <button className="modal-close" onClick={onCancel}>✕</button>
        </div>

        <div className="modal-body">
          <p className="modal-subtitle">
            Select a source dataset for <strong>{dagId}</strong>
          </p>

          {loading && <LoadingSpinner />}
          {error && <ErrorMessage message={error} />}

          {!loading && !error && (
            <div className="dataset-options">
              {datasets.map(ds => (
                <label
                  key={ds.id}
                  className={`dataset-option${selected?.id === ds.id ? ' selected' : ''}`}
                >
                  <input
                    type="radio"
                    name="dataset"
                    value={ds.id}
                    checked={selected?.id === ds.id}
                    onChange={() => setSelected(ds)}
                  />
                  <div className="dataset-option-body">
                    {ds.name && <span style={{ fontWeight: 600, fontSize: '13px' }}>{ds.name}</span>}
                    <code className="dataset-uri">{ds.uri}</code>
                    {ds.extra?.description && (
                      <span className="dataset-desc">{ds.extra.description}</span>
                    )}
                  </div>
                </label>
              ))}
            </div>
          )}
        </div>

        <div className="modal-footer">
          <button className="btn btn-secondary" onClick={onCancel}>Cancel</button>
          <button
            className="btn btn-primary"
            disabled={!selected}
            onClick={() => onConfirm({ dataset_uri: selected.uri, dataset_id: selected.id })}
          >
            Trigger
          </button>
        </div>
      </div>
    </div>
  )
}

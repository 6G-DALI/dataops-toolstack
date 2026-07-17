import { useEffect, useState } from 'react'
import { deleteDistribution, getDistributions } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import LoadingSpinner from './LoadingSpinner'
import Modal from './Modal'
import type { Dataset, Distribution } from '../types'

interface ManageDistributionsModalProps {
  dataset: Dataset
  onClose: () => void
  onChanged: () => void
}

export default function ManageDistributionsModal({ dataset, onClose, onChanged }: ManageDistributionsModalProps) {
  const [distributions, setDistributions] = useState<Distribution[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [confirmingId, setConfirmingId] = useState<string | null>(null)

  const datasetId = dataset.dataset_id || dataset.id
  const catalogueId = dataset.catalog_id

  function load() {
    setLoading(true)
    getDistributions(datasetId, catalogueId)
      .then(data => setDistributions(data.distributions || []))
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    load()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function handleDelete(dist: Distribution) {
    if (!dist.asset_id) {
      setError('This distribution has no asset_id on record — cannot delete it safely.')
      return
    }
    if (!catalogueId) {
      setError('This dataset has no catalogue_id — cannot delete its distributions.')
      return
    }
    setConfirmingId(null)
    setDeletingId(dist.id)
    setError(null)
    try {
      await deleteDistribution(datasetId, catalogueId, dist.asset_id)
      setDistributions(prev => prev.filter(d => d.id !== dist.id))
      onChanged()
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setDeletingId(null)
    }
  }

  return (
    <Modal
      title={`Distributions — ${dataset.name || dataset.id}`}
      onClose={onClose}
      width={640}
      footer={<button className="btn btn-secondary" onClick={onClose}>Close</button>}
    >
      {error && <ErrorMessage message={error} />}
      {loading ? (
        <LoadingSpinner />
      ) : distributions.length === 0 ? (
        <p className="text-muted small mb-0">This dataset has no distributions.</p>
      ) : (
        <div className="table-responsive">
          <table className="table table-sm align-middle mb-0">
            <thead>
              <tr>
                <th>Title</th>
                <th>Asset ID</th>
                <th style={{ width: 90 }}></th>
              </tr>
            </thead>
            <tbody>
              {distributions.map(dist => (
                <tr key={dist.id}>
                  <td>{dist.asset_title || dist.name || dist.id}</td>
                  <td><code className="small">{dist.asset_id || '—'}</code></td>
                  <td>
                    {confirmingId === dist.id ? (
                      <div className="d-flex gap-1">
                        <button
                          className="btn btn-sm btn-danger"
                          disabled={deletingId === dist.id}
                          onClick={() => handleDelete(dist)}
                        >
                          {deletingId === dist.id ? '…' : 'Confirm'}
                        </button>
                        <button className="btn btn-sm btn-outline-secondary" onClick={() => setConfirmingId(null)}>Cancel</button>
                      </div>
                    ) : (
                      <button
                        className="btn btn-sm btn-outline-danger"
                        disabled={!dist.asset_id || deletingId !== null}
                        onClick={() => setConfirmingId(dist.id)}
                      >
                        Delete
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Modal>
  )
}

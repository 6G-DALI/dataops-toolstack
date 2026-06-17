import { useEffect, useState } from 'react'
import { getDatasets } from '../api/airflow'
import { Modal, Button, Radio, Form, Input, Spin } from 'antd'
import 'antd/dist/reset.css'

export default function TriggerModal({ dagId, onConfirm, onCancel }) {
  const [datasets, setDatasets] = useState([])
  const [loading, setLoading] = useState(true)
  const [selected, setSelected] = useState(null)
  const [connId, setConnId] = useState('dali-dataspace')

  useEffect(() => {
    getDatasets()
      .then(data => setDatasets(data.datasets || []))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  function handleSelect(ds) {
    setSelected(ds)
  }

  function handleConfirm() {
    onConfirm({
      conn_id: connId,
      input_key: selected.input_key,
      catalogue_id: selected.catalog_id,
      expectations: [],
    })
  }

  return (
    <Modal
      open
      title={<>Trigger <code style={{ fontSize: '13px' }}>{dagId}</code></>}
      onCancel={onCancel}
      width={640}
      footer={[
        <Button key="cancel" onClick={onCancel}>Cancel</Button>,
        <Button key="trigger" type="primary" disabled={!selected} onClick={handleConfirm}>
          Trigger
        </Button>,
      ]}
    >
      {loading ? (
        <div style={{ textAlign: 'center', padding: 32 }}><Spin /></div>
      ) : (
        <>
          <p style={{ marginBottom: 12, color: '#6b7280', fontSize: '13px' }}>
            Select a source dataset:
          </p>

          <Radio.Group
            value={selected?.id}
            onChange={e => handleSelect(datasets.find(d => d.id === e.target.value))}
            style={{ display: 'flex', flexDirection: 'column', gap: 8, marginBottom: 20 }}
          >
            {datasets.map(ds => (
              <Radio key={ds.id} value={ds.id} style={{ alignItems: 'flex-start' }}>
                <div>
                  <div style={{ fontWeight: 600, fontSize: '13px' }}>{ds.name || ds.asset_title || ds.id}</div>
                  <code style={{ fontSize: '11px', color: '#6b7280' }}>{ds.asset_id}</code>
                </div>
              </Radio>
            ))}
          </Radio.Group>

          {selected && (
            <>
              <p style={{ marginBottom: 8, color: '#6b7280', fontSize: '13px', fontWeight: 600 }}>
                DAG Configuration
              </p>
              <Form layout="vertical" size="small">
                <Form.Item label="conn_id">
                  <Input value={connId} onChange={e => setConnId(e.target.value)} />
                </Form.Item>
                <Form.Item label="input_key">
                  <Input value={selected.input_key} readOnly />
                </Form.Item>
                <Form.Item label="catalogue_id">
                  <Input value={selected.catalog_id} readOnly />
                </Form.Item>
                <Form.Item label="expectations">
                  <Input value="[]" readOnly />
                </Form.Item>
              </Form>
            </>
          )}
        </>
      )}
    </Modal>
  )
}

import { useEffect, useState } from 'react'
import { getServices, registerAllServices, registerService, deregisterService } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import { Table, Button, Tag, Input, Space, Typography } from 'antd'
import { SearchOutlined } from '@ant-design/icons'
import 'antd/dist/reset.css'

const SERVICE_TYPE_COLORS = {
  QualityCheck:          'green',
  Transformation:        'blue',
  Augmentation:          'purple',
  Aggregation:           'gold',
  Anonymisation:         'magenta',
  'Feature Engineering': 'cyan',
}

const COLUMNS = (registering, results, onRegister, onDeregister) => [
  {
    title: 'Service',
    key: 'service',
    render: (_, svc) => (
      <div>
        <div style={{ fontWeight: 600, marginBottom: 2 }}>{svc.title}</div>
        <code style={{ fontSize: '11px', color: '#6b7280' }}>{svc.service_id}</code>
        {svc.description && (
          <Typography.Paragraph
            type="secondary"
            style={{ fontSize: '12px', marginBottom: 0, marginTop: 4 }}
            ellipsis={{ rows: 2, expandable: true }}
          >
            {svc.description}
          </Typography.Paragraph>
        )}
      </div>
    ),
  },
  {
    title: 'Type',
    dataIndex: 'service_type',
    key: 'service_type',
    width: 160,
    render: type => <Tag color={SERVICE_TYPE_COLORS[type] ?? 'default'}>{type}</Tag>,
  },
  {
    title: 'Framework',
    dataIndex: 'framework',
    key: 'framework',
    width: 120,
    render: v => v ? <Tag>{v}</Tag> : '—',
  },
  {
    title: 'Input',
    dataIndex: 'input_format',
    key: 'input_format',
    width: 90,
    render: v => v ? <code style={{ fontSize: '11px' }}>{v}</code> : '—',
  },
  {
    title: 'Output',
    dataIndex: 'output_format',
    key: 'output_format',
    width: 90,
    render: v => v ? <code style={{ fontSize: '11px' }}>{v}</code> : '—',
  },
  {
    title: 'Module',
    key: 'module',
    width: 160,
    render: (_, svc) => (
      <div style={{ fontSize: '11px' }}>
        {svc.module && <div><code>{svc.module}</code></div>}
        {svc.function && <div style={{ color: '#9ca3af' }}>{svc.function}()</div>}
      </div>
    ),
  },
  {
    title: 'Catalogue',
    key: 'actions',
    width: 160,
    render: (_, svc) => {
      const status = results[svc.service_id]
      const busy = registering[svc.service_id]
      return (
        <div>
          {status && (
            <Tag
              color={status.startsWith('error') ? 'error' : status === 'registered' ? 'success' : 'default'}
              style={{ marginBottom: 6, display: 'block' }}
            >
              {status}
            </Tag>
          )}
          <Space size={4}>
            <Button size="small" loading={busy} onClick={() => onRegister(svc.service_id)}>
              Register
            </Button>
            <Button size="small" loading={busy} danger onClick={() => onDeregister(svc.service_id)}>
              Remove
            </Button>
          </Space>
        </div>
      )
    },
  },
]

export default function ServiceList() {
  const [services, setServices] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [registering, setRegistering] = useState({})
  const [results, setResults] = useState({})
  const [search, setSearch] = useState('')

  useEffect(() => {
    getServices()
      .then(data => setServices(data.services || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  async function handleRegisterAll() {
    setRegistering(prev => ({ ...prev, _all: true }))
    try {
      const data = await registerAllServices()
      const map = {}
      for (const r of data.results || []) {
        map[r.service_id] = r.status === 'registered' ? 'registered' : `error: ${r.detail}`
      }
      setResults(map)
    } catch (e) {
      setError(e.message)
    } finally {
      setRegistering(prev => ({ ...prev, _all: false }))
    }
  }

  async function handleRegister(serviceId) {
    setRegistering(prev => ({ ...prev, [serviceId]: true }))
    try {
      await registerService(serviceId)
      setResults(prev => ({ ...prev, [serviceId]: 'registered' }))
    } catch (e) {
      setResults(prev => ({ ...prev, [serviceId]: `error: ${e.message}` }))
    } finally {
      setRegistering(prev => ({ ...prev, [serviceId]: false }))
    }
  }

  async function handleDeregister(serviceId) {
    setRegistering(prev => ({ ...prev, [serviceId]: true }))
    try {
      await deregisterService(serviceId)
      setResults(prev => ({ ...prev, [serviceId]: 'deregistered' }))
    } catch (e) {
      setResults(prev => ({ ...prev, [serviceId]: `error: ${e.message}` }))
    } finally {
      setRegistering(prev => ({ ...prev, [serviceId]: false }))
    }
  }

  if (error) return <ErrorMessage message={error} />

  const visible = search
    ? services.filter(s =>
        [s.service_id, s.title, s.service_type, s.framework]
          .some(v => v?.toLowerCase().includes(search.toLowerCase()))
      )
    : services

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <h1 className="page-title" style={{ marginBottom: 0 }}>Data Processing Services</h1>
          <p className="page-subtitle" style={{ margin: '4px 0 0' }}>
            {visible.length}{visible.length !== services.length ? ` of ${services.length}` : ''} service{services.length !== 1 ? 's' : ''} available
          </p>
        </div>
        <Space>
          <Input
            placeholder="Search services…"
            prefix={<SearchOutlined />}
            value={search}
            onChange={e => setSearch(e.target.value)}
            allowClear
            style={{ width: 240 }}
          />
          <Button type="primary" loading={registering._all} onClick={handleRegisterAll}>
            Register All
          </Button>
        </Space>
      </div>

      <Table
        rowKey="service_id"
        columns={COLUMNS(registering, results, handleRegister, handleDeregister)}
        dataSource={visible}
        loading={loading}
        pagination={{ pageSize: 20, showSizeChanger: true, pageSizeOptions: [10, 20, 50] }}
        size="small"
        scroll={{ x: true }}
      />
    </div>
  )
}

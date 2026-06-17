import { useEffect, useState, useMemo } from 'react'
import { getDatasets } from '../api/airflow'
import ErrorMessage from './ErrorMessage'
import { Table, Button, Modal, Tag, Input } from 'antd'
import { SearchOutlined } from '@ant-design/icons'
import 'antd/dist/reset.css'
import { FiFile } from 'react-icons/fi'
import { BsFiletypeCsv, BsFiletypePdf, BsFiletypeJson, BsFiletypeXml, BsFiletypeTxt } from 'react-icons/bs'

const FORMAT_ICONS = {
  csv:  <BsFiletypeCsv size={14} />,
  pdf:  <BsFiletypePdf size={14} />,
  json: <BsFiletypeJson size={14} />,
  xml:  <BsFiletypeXml size={14} />,
  txt:  <BsFiletypeTxt size={14} />,
}

const FORMAT_COLORS = {
  csv: 'green', pdf: 'red', json: 'blue', xml: 'orange', txt: 'default',
}

function FormatTag({ format }) {
  if (!format) return '—'
  const key = format.toLowerCase().trim()
  return (
    <Tag color={FORMAT_COLORS[key] ?? 'default'} icon={FORMAT_ICONS[key] ?? <FiFile size={14} />}>
      {format.toUpperCase()}
    </Tag>
  )
}

const COLUMNS = (onInspect) => [
  {
    title: 'Dataset',
    key: 'dataset',
    render: (_, ds) => (
      <div>
        <div style={{ fontWeight: 600, marginBottom: 2 }}>{ds.name || '—'}</div>
        {ds.sns_project_name && (
          <div style={{ fontSize: '12px', color: '#6b7280' }}>{ds.sns_project_name}</div>
        )}
        {(ds.catalog_title || ds.catalog_id) && (
          <div style={{ fontSize: '12px', color: '#9ca3af' }}>
            {ds.catalog_url
              ? <a href={ds.catalog_url} target="_blank" rel="noreferrer">{ds.catalog_title || ds.catalog_id}</a>
              : ds.catalog_title || ds.catalog_id}
          </div>
        )}
      </div>
    ),
  },
  {
    title: 'Asset',
    key: 'asset',
    render: (_, ds) => (
      <div>
        <div style={{ fontWeight: 500, marginBottom: 2 }}>{ds.asset_title || '—'}</div>
        {ds.asset_id && (
          <code style={{ fontSize: '11px', color: '#6b7280' }}>{ds.asset_id}</code>
        )}
      </div>
    ),
  },
  {
    title: 'Format',
    key: 'format',
    width: 90,
    render: (_, ds) => <FormatTag format={ds.extra?.format} />,
  },
  {
    title: 'License',
    key: 'license',
    width: 200,
    render: (_, ds) => ds.extra?.license
      ? <Tag color="purple" style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={ds.extra.license}>{ds.extra.license}</Tag>
      : '—',
  },
  {
    title: 'Last Updated',
    key: 'updated_at',
    width: 160,
    render: (_, ds) => ds.updated_at ? new Date(ds.updated_at).toLocaleString() : '—',
    sorter: (a, b) => new Date(a.updated_at) - new Date(b.updated_at),
    defaultSortOrder: 'descend',
  },
  {
    title: '',
    key: 'actions',
    width: 80,
    render: (_, ds) => (
      <Button size="small" onClick={() => onInspect(ds)}>Inspect</Button>
    ),
  },
]

function ExpandedRow({ ds }) {
  return (
    <div style={{ padding: '8px 16px', display: 'flex', flexDirection: 'column', gap: 10 }}>
      {(ds.variable_measured || []).length > 0 && (
        <div>
          <span style={{ fontSize: '11px', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: '#9ca3af', marginRight: 10 }}>
            Variables
          </span>
          {ds.variable_measured.slice().sort().map(v => (
            <Tag key={v} color="cyan" style={{ marginBottom: 4 }}>{v}</Tag>
          ))}
        </div>
      )}
      {(ds.producing_dags || []).length > 0 && (
        <div>
          <span style={{ fontSize: '11px', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: '#9ca3af', marginRight: 10 }}>
            Produced by
          </span>
          {ds.producing_dags.map(d => (
            <Tag key={d.dag_id} color="geekblue">{d.dag_id}</Tag>
          ))}
        </div>
      )}
      {(ds.consuming_dags || []).length > 0 && (
        <div>
          <span style={{ fontSize: '11px', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: '#9ca3af', marginRight: 10 }}>
            Consumed by
          </span>
          {ds.consuming_dags.map(d => (
            <Tag key={d.dag_id} color="volcano">{d.dag_id}</Tag>
          ))}
        </div>
      )}
    </div>
  )
}

export default function DatasetList() {
  const [datasets, setDatasets] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [inspecting, setInspecting] = useState(null)
  const [search, setSearch] = useState('')

  useEffect(() => {
    getDatasets()
      .then(data => setDatasets(data.datasets || []))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  const filtered = useMemo(() => {
    const q = search.toLowerCase().trim()
    if (!q) return datasets
    return datasets.filter(ds =>
      [ds.name, ds.asset_title, ds.sns_project_name, ds.catalog_title, ds.extra?.format]
        .some(v => v?.toLowerCase().includes(q))
    )
  }, [datasets, search])

  if (error) return <ErrorMessage message={error} />

  return (
    <div>
      <h1 className="page-title">Datasets</h1>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
        <p className="page-subtitle" style={{ margin: 0 }}>
          {filtered.length}{filtered.length !== datasets.length ? ` of ${datasets.length}` : ''} dataset{datasets.length !== 1 ? 's' : ''}
        </p>
        <Input
          placeholder="Search datasets…"
          prefix={<SearchOutlined />}
          value={search}
          onChange={e => setSearch(e.target.value)}
          allowClear
          style={{ width: 260 }}
        />
      </div>

      <Table
        rowKey="id"
        columns={COLUMNS(setInspecting)}
        dataSource={filtered}
        loading={loading}
        pagination={{ pageSize: 20, showSizeChanger: true, pageSizeOptions: [10, 20, 50] }}
        size="small"
        scroll={{ x: true }}
        expandable={{
          expandedRowRender: ds => <ExpandedRow ds={ds} />,
          rowExpandable: ds =>
            (ds.variable_measured?.length > 0) ||
            (ds.producing_dags?.length > 0) ||
            (ds.consuming_dags?.length > 0),
        }}
      />

      <Modal
        open={!!inspecting}
        title={`Raw record — ${inspecting?.name || inspecting?.id}`}
        onCancel={() => setInspecting(null)}
        footer={<Button onClick={() => setInspecting(null)}>Close</Button>}
        width={780}
      >
        <pre style={{
          margin: 0, fontSize: '12px', lineHeight: '1.5',
          whiteSpace: 'pre-wrap', wordBreak: 'break-all',
          background: '#f8f9fa', padding: '12px',
          borderRadius: '4px', border: '1px solid #dee2e6',
          maxHeight: '60vh', overflowY: 'auto',
        }}>
          {JSON.stringify(inspecting?.raw, null, 2)}
        </pre>
      </Modal>
    </div>
  )
}

import { useEffect, useState } from 'react'
import { getStats } from '../api/airflow'
import { Card, Col, Row, Tag, Statistic, Spin } from 'antd'
import { CheckCircleOutlined, CloseCircleOutlined, SyncOutlined, PauseCircleOutlined } from '@ant-design/icons'
import 'antd/dist/reset.css'

const STATE_TAG = {
  success:  <Tag icon={<CheckCircleOutlined />} color="success">success</Tag>,
  failed:   <Tag icon={<CloseCircleOutlined />} color="error">failed</Tag>,
  running:  <Tag icon={<SyncOutlined spin />}   color="processing">running</Tag>,
  queued:   <Tag color="warning">queued</Tag>,
}

function fmt(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

export default function HomePage({ onNavigate }) {
  const [stats, setStats] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getStats()
      .then(setStats)
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  return (
    <div>
      <h1 className="page-title">6G-DALI DataOps</h1>
      <p className="page-subtitle">Overview of the DataOps deployment</p>

      {loading ? (
        <Spin />
      ) : stats && (
        <>
          <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
            <Col xs={12} sm={6}>
              <Card
                hoverable
                onClick={() => onNavigate('dags', {})}
                style={{ textAlign: 'center' }}
              >
                <Statistic title="Total DAGs" value={stats.dags?.total ?? 0} />
              </Card>
            </Col>
            <Col xs={12} sm={6}>
              <Card
                hoverable
                onClick={() => onNavigate('dags', {})}
                style={{ textAlign: 'center' }}
              >
                <Statistic title="Active DAGs" value={stats.dags?.active ?? 0} valueStyle={{ color: '#3f8600' }} />
              </Card>
            </Col>
            <Col xs={12} sm={6}>
              <Card style={{ textAlign: 'center' }}>
                <Statistic
                  title="Paused DAGs"
                  value={stats.dags?.paused ?? 0}
                  prefix={<PauseCircleOutlined />}
                  valueStyle={{ color: '#888' }}
                />
              </Card>
            </Col>
            <Col xs={12} sm={6}>
              <Card
                hoverable
                onClick={() => onNavigate('all-tasks', {})}
                style={{ textAlign: 'center' }}
              >
                <Statistic title="Custom Tasks" value={stats.tasks?.custom ?? 0} />
              </Card>
            </Col>
          </Row>

          {(stats.recent_runs || []).length > 0 && (
            <Card title="Recent Runs" size="small">
              <table style={{ width: '100%', fontSize: 13, borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ color: '#888', textAlign: 'left' }}>
                    <th style={{ padding: '6px 12px', fontWeight: 600 }}>DAG</th>
                    <th style={{ padding: '6px 12px', fontWeight: 600 }}>Run ID</th>
                    <th style={{ padding: '6px 12px', fontWeight: 600 }}>State</th>
                    <th style={{ padding: '6px 12px', fontWeight: 600 }}>Started</th>
                  </tr>
                </thead>
                <tbody>
                  {stats.recent_runs.map((run, i) => (
                    <tr
                      key={i}
                      style={{ borderTop: '1px solid #f0f0f0', cursor: 'pointer' }}
                      onClick={() => onNavigate('tasks', { dagId: run.dag_id, runId: run.dag_run_id })}
                    >
                      <td style={{ padding: '6px 12px', fontWeight: 500 }}>{run.dag_id}</td>
                      <td style={{ padding: '6px 12px', fontFamily: 'monospace', fontSize: 11, color: '#6b7280' }}>
                        {run.dag_run_id?.length > 40 ? run.dag_run_id.slice(0, 40) + '…' : run.dag_run_id}
                      </td>
                      <td style={{ padding: '6px 12px' }}>{STATE_TAG[run.state] ?? <Tag>{run.state}</Tag>}</td>
                      <td style={{ padding: '6px 12px', color: '#6b7280' }}>{fmt(run.start_date)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Card>
          )}
        </>
      )}
    </div>
  )
}

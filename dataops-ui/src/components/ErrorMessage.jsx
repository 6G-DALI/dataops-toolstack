export default function ErrorMessage({ message }) {
  return (
    <div style={{
      background: '#f8d7da',
      color: '#721c24',
      border: '1px solid #f5c6cb',
      borderRadius: '6px',
      padding: '12px 16px',
      margin: '16px 0',
      fontSize: '13px',
    }}>
      <strong>Error:</strong> {message}
    </div>
  )
}

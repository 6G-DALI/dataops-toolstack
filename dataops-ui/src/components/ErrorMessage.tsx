interface ErrorMessageProps {
  message: string
}

export default function ErrorMessage({ message }: ErrorMessageProps) {
  return (
    <div className="alert alert-danger my-3" role="alert">
      <strong>Error:</strong> {message}
    </div>
  )
}

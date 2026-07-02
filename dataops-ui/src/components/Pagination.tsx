interface PaginationProps {
  page: number // 1-based
  pageSize: number
  total: number
  pageSizeOptions?: number[]
  unit?: string
  onPageChange: (page: number) => void
  onPageSizeChange: (size: number) => void
}

export default function Pagination({
  page,
  pageSize,
  total,
  pageSizeOptions = [10, 25, 50, 100],
  unit = 'items',
  onPageChange,
  onPageSizeChange,
}: PaginationProps) {
  const pageCount = Math.max(1, Math.ceil(total / pageSize))
  const from = total === 0 ? 0 : (page - 1) * pageSize + 1
  const to = Math.min(page * pageSize, total)

  return (
    <div className="d-flex flex-wrap align-items-center justify-content-between gap-2 mt-2">
      <div className="text-muted small">
        {from}–{to} of {total} {unit}
      </div>
      <div className="d-flex align-items-center gap-2">
        <select
          className="form-select form-select-sm w-auto"
          value={pageSize}
          onChange={e => onPageSizeChange(Number(e.target.value))}
        >
          {pageSizeOptions.map(size => (
            <option key={size} value={size}>{size} / page</option>
          ))}
        </select>
        <nav aria-label="Pagination">
          <ul className="pagination pagination-sm mb-0">
            <li className={`page-item${page <= 1 ? ' disabled' : ''}`}>
              <button className="page-link" onClick={() => onPageChange(page - 1)} disabled={page <= 1}>
                Prev
              </button>
            </li>
            <li className="page-item disabled">
              <span className="page-link">{page} / {pageCount}</span>
            </li>
            <li className={`page-item${page >= pageCount ? ' disabled' : ''}`}>
              <button className="page-link" onClick={() => onPageChange(page + 1)} disabled={page >= pageCount}>
                Next
              </button>
            </li>
          </ul>
        </nav>
      </div>
    </div>
  )
}

import type { ReactNode } from 'react'

interface ModalProps {
  title: ReactNode
  onClose: () => void
  footer?: ReactNode
  width?: number
  children: ReactNode
}

export default function Modal({ title, onClose, footer, width, children }: ModalProps) {
  return (
    <>
      <div className="modal fade show d-block" tabIndex={-1} role="dialog" onClick={onClose}>
        <div
          className="modal-dialog modal-dialog-centered modal-dialog-scrollable"
          style={width ? { maxWidth: width } : undefined}
          onClick={e => e.stopPropagation()}
        >
          <div className="modal-content">
            <div className="modal-header">
              <h5 className="modal-title">{title}</h5>
              <button type="button" className="btn-close" aria-label="Close" onClick={onClose} />
            </div>
            <div className="modal-body">{children}</div>
            {footer && <div className="modal-footer">{footer}</div>}
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show" />
    </>
  )
}

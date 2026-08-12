import { useState } from 'react'
import { FiCheck, FiCopy } from 'react-icons/fi'

/**
 * Copyable identifier — general_gui_guidelines.md §14.4.
 *
 * Renders in JetBrains Mono, truncates visually via CSS (not by slicing the
 * string), keeps the complete value available to the copy action and to the
 * title tooltip, and confirms the copy briefly.
 */

interface CopyableIdProps {
  value: string
  /** Visual truncation width. The full value is always preserved. */
  maxWidth?: number
}

export default function CopyableId({ value, maxWidth = 260 }: CopyableIdProps) {
  const [copied, setCopied] = useState(false)

  async function copy(event: React.MouseEvent) {
    // These often sit inside clickable table rows.
    event.stopPropagation()
    try {
      await navigator.clipboard.writeText(value)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1400)
    } catch {
      // Clipboard unavailable (insecure context or denied permission) — the
      // full value stays selectable and visible in the tooltip.
    }
  }

  return (
    <span className="copyable-id">
      <code className="copyable-id-value" style={{ maxWidth }} title={value}>
        {value}
      </code>
      <button
        type="button"
        className="copyable-id-btn"
        onClick={copy}
        aria-label={copied ? 'Copied' : `Copy ${value}`}
      >
        {copied ? <FiCheck aria-hidden="true" /> : <FiCopy aria-hidden="true" />}
      </button>
      <span className="visually-hidden" role="status">
        {copied ? 'Copied to clipboard' : ''}
      </span>
    </span>
  )
}

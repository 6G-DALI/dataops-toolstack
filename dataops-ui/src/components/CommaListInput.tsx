import { useEffect, useState } from 'react'

/** Split a comma-separated field into trimmed, non-empty entries. */
export function splitList(value: string): string[] {
  return value.split(',').map(s => s.trim()).filter(Boolean)
}

interface CommaListInputProps {
  values: string[]
  onChange: (values: string[]) => void
  placeholder?: string
  className?: string
  id?: string
}

/**
 * A text input backed by a string[], holding the raw text while it is edited.
 *
 * The obvious version — `value={values.join(', ')}` with
 * `onChange={e => onChange(splitList(e.target.value))}` — is unusable: splitList
 * drops empty entries, so typing "a," parses to ["a"] and the value rendered
 * back is "a". The comma is erased on the same keystroke that produced it, so
 * the separator can never be typed at all; likewise the space after it.
 *
 * So the draft string stays the source of truth while the user types, and the
 * parsed array is pushed up on every change. The draft is re-synced from
 * `values` only when the two genuinely disagree — i.e. when the value changed
 * from outside (a form reset, an auto-detected column list) rather than by our
 * own edit, which would otherwise clobber an in-progress "a, ".
 */
export default function CommaListInput({ values, onChange, placeholder, className, id }: CommaListInputProps) {
  const [draft, setDraft] = useState(() => values.join(', '))
  // Compared as JSON so element boundaries are unambiguous — ["a b"] and
  // ["a", "b"] must never look equal.
  const valuesKey = JSON.stringify(values)

  useEffect(() => {
    if (JSON.stringify(splitList(draft)) !== valuesKey) setDraft(values.join(', '))
    // Deliberately keyed on the incoming values only: including `draft` would
    // make every keystroke a candidate for being overwritten.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [valuesKey])

  return (
    <input
      id={id}
      className={className ?? 'form-control'}
      placeholder={placeholder}
      value={draft}
      onChange={e => {
        setDraft(e.target.value)
        onChange(splitList(e.target.value))
      }}
    />
  )
}

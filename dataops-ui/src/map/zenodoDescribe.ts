// Maps the DALI Dataset Descriptor Lambda's output onto the Create-Dataset
// form's model (see types.ts). The Lambda normalises a Zenodo record into a
// DCAT/Dublin-Core-style JSON object; this turns that into the subset of the
// 6G-DALI Metadata Application Profile the form's "Metadata" step exposes.
//
// The mapping is deliberately tolerant:
//   - every field is optional, so a sparse record never throws;
//   - only non-empty values overwrite the form, so re-running "Describe" or
//     describing after hand-editing never blanks a field the user already set;
//   - it already reads a few fields the Lambda does NOT emit yet (contributors,
//     temporal coverage, spatial) so upgrading the Lambda per the notes in the
//     PR needs no further UI change — those simply start getting filled.

import type { DatasetIdentityInput, DatasetObjectInput } from '../types'

/** One creator as the Lambda emits it. */
export interface DescriptorCreator {
  name?: string
  affiliation?: string
  orcid?: string
}

/** The `metadata` object returned by the descriptor Lambda. Everything is
 *  optional: the model uses null for anything it cannot determine, and the
 *  forward-compatible fields (contributors/temporal/spatial) are absent until
 *  the Lambda is upgraded to emit them. */
export interface DescriptorMetadata {
  identifier?: string | null
  title?: string | null
  description?: string | null
  creators?: DescriptorCreator[] | null
  publisher?: string | null
  issued?: string | null
  modified?: string | null
  language?: string | null
  keywords?: string[] | null
  license?: string | null
  accessRights?: string | null
  resourceType?: string | null
  version?: string | null
  distributions?: unknown[] | null
  landingPage?: string | null
  source?: string | null
  // Forward-compatible: not emitted by the current Lambda (see PR notes).
  contributors?: string[] | null
  temporalStart?: string | null
  temporalEnd?: string | null
  spatial?: string | null
}

// Common SPDX / Zenodo license ids → the canonical URL the MAP's dct:license
// expects. Anything already a URL passes through; an unrecognised id is kept
// verbatim so the value is never lost — the user can correct it in the field.
const LICENSE_URLS: Record<string, string> = {
  'cc-by-4.0': 'https://creativecommons.org/licenses/by/4.0/',
  'cc-by-sa-4.0': 'https://creativecommons.org/licenses/by-sa/4.0/',
  'cc-by-nc-4.0': 'https://creativecommons.org/licenses/by-nc/4.0/',
  'cc-by-nc-sa-4.0': 'https://creativecommons.org/licenses/by-nc-sa/4.0/',
  'cc-by-nd-4.0': 'https://creativecommons.org/licenses/by-nd/4.0/',
  'cc0-1.0': 'https://creativecommons.org/publicdomain/zero/1.0/',
  'mit': 'https://opensource.org/license/mit',
  'apache-2.0': 'https://www.apache.org/licenses/LICENSE-2.0',
  'gpl-3.0': 'https://www.gnu.org/licenses/gpl-3.0.html',
  'gpl-3.0-only': 'https://www.gnu.org/licenses/gpl-3.0.html',
  'bsd-3-clause': 'https://opensource.org/license/bsd-3-clause',
}

function toLicenseUrl(license?: string | null): string | null {
  const value = (license ?? '').trim()
  if (!value) return null
  if (/^https?:\/\//i.test(value)) return value
  return LICENSE_URLS[value.toLowerCase()] ?? value
}

// The descriptor's DCAT-style dct:accessRights vocabulary → the MAP's
// dali:accessRights enum the form's <select> offers. `unknown` (and anything
// unrecognised) returns null, which leaves the form's existing choice untouched.
function toAccessRights(access?: string | null): DatasetObjectInput['access_rights'] | null {
  switch ((access ?? '').trim().toLowerCase()) {
    case 'open':
      return 'PUBLIC'
    case 'embargoed':
    case 'restricted':
      return 'RESTRICTED'
    case 'closed':
      return 'NON_PUBLIC'
    default:
      return null
  }
}

// `<input type="date">` needs a bare YYYY-MM-DD; a full ISO timestamp is
// truncated to its date part, and anything else is dropped rather than fed to
// the picker as an unparseable value.
function toDateInput(value?: string | null): string | null {
  const match = (value ?? '').trim().match(/^(\d{4}-\d{2}-\d{2})/)
  return match ? match[1] : null
}

function dedupe(values: string[]): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const v of values.map(s => s.trim()).filter(Boolean)) {
    const key = v.toLowerCase()
    if (!seen.has(key)) { seen.add(key); out.push(v) }
  }
  return out
}

export interface MergeResult {
  identity: DatasetIdentityInput
  object: DatasetObjectInput
  /** Human-readable list of the fields that were filled, for the status line. */
  filled: string[]
}

/**
 * Fold a descriptor record into the current form state, returning new identity
 * and object values plus the list of fields that changed. Only non-empty
 * descriptor values are applied, so this never clears a field the user already
 * filled in by hand.
 */
export function mergeDescriptor(
  meta: DescriptorMetadata,
  identity: DatasetIdentityInput,
  object: DatasetObjectInput,
): MergeResult {
  const nextIdentity: DatasetIdentityInput = { ...identity }
  const nextObject: DatasetObjectInput = { ...object }
  const filled: string[] = []

  const setId = <K extends keyof DatasetIdentityInput>(key: K, value: DatasetIdentityInput[K], label: string) => {
    nextIdentity[key] = value
    filled.push(label)
  }

  if (meta.title?.trim()) setId('title', meta.title.trim(), 'title')
  if (meta.description?.trim()) setId('description', meta.description.trim(), 'description')
  if (meta.publisher?.trim()) setId('publisher_name', meta.publisher.trim(), 'publisher')
  if (meta.version?.trim()) setId('version', meta.version.trim(), 'version')
  if (meta.language?.trim()) setId('language', meta.language.trim().toUpperCase(), 'language')

  const issued = toDateInput(meta.issued)
  if (issued) setId('issued', issued, 'first published')

  if (Array.isArray(meta.keywords) && meta.keywords.length) {
    setId('keywords', dedupe(meta.keywords), 'keywords')
  }

  if (Array.isArray(meta.creators) && meta.creators.length) {
    const creators = meta.creators
      .filter(c => c?.name?.trim())
      .map(c => ({
        kind: 'Person' as const,
        name: (c.name ?? '').trim(),
        orcid: (c.orcid ?? '').trim(),
        affiliation: (c.affiliation ?? '').trim(),
      }))
    if (creators.length) setId('creators', creators, 'creators')
  }

  // Forward-compatible fields — filled only once the Lambda emits them.
  if (Array.isArray(meta.contributors) && meta.contributors.length) {
    setId('contributors', dedupe(meta.contributors.map(String)), 'contributors')
  }
  const temporalStart = toDateInput(meta.temporalStart)
  if (temporalStart) setId('temporal_start', temporalStart, 'data start date')
  const temporalEnd = toDateInput(meta.temporalEnd)
  if (temporalEnd) setId('temporal_end', temporalEnd, 'data end date')
  if (meta.spatial?.trim()) setId('spatial', meta.spatial.trim(), 'spatial coverage')

  const license = toLicenseUrl(meta.license)
  if (license) { nextObject.license = license; filled.push('license') }
  const access = toAccessRights(meta.accessRights)
  if (access) { nextObject.access_rights = access; filled.push('access rights') }

  return { identity: nextIdentity, object: nextObject, filled }
}

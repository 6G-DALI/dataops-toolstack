/**
 * Builds a 6G-DALI MAP dataset record (DCAT-AP + GAIA-X + CMT testbed context)
 * as Turtle, client-side, for submission via POST /datasets/rdf — the RDF-first
 * counterpart to the JSON POST /datasets flow.
 *
 * This mirrors the orchestrator's `piveau_dataset_client.build_dataset_turtle`
 * field for field. The two are deliberately equivalent: POST /datasets is still
 * served by the Python builder for API clients, while the UI submits the graph
 * it generated (and showed the submitter) here. Any change to the MAP mapping
 * has to be made in BOTH places or the two entry points will drift.
 *
 * The dataset's own identifiers are not known client-side — dataset_id is minted
 * by the orchestrator and the dataspace base URI is server configuration — so
 * both appear as sentinels that the orchestrator substitutes on receipt. They
 * must stay byte-identical to the constants in piveau_dataset_client.py.
 */
import type {
  DatasetIdentityInput,
  DatasetObjectInput,
  DistributionMetricsInput,
  TestbedContextInput,
} from '../types'

/** Stands in for the dataset's subject IRI (`<{DSPACE_BASE}/set/data/{id}>`). */
export const DATASET_URI_SENTINEL = 'urn:6gdali:dataset:self'
/** Stands in for the bare dataset_id, used as dct:identifier. */
export const DATASET_ID_SENTINEL = 'urn:6gdali:dataset:id'

// Sentinels for the parts of a distribution that only exist once the file has
// been uploaded. Like the dataset sentinels, these ARE submitted — the document
// goes to POST /datasets/{id}/distributions/rdf and the orchestrator substitutes
// them (see piveau_dataset_client.resolve_distribution_sentinels), so the graph
// stored is the one the submitter was shown.
//
// The subject and the dali:assetId literal need separate tokens: one resolves to
// the distribution's full IRI, the other to the bare UUID, so a single token
// could not stand for both.
/** The distribution's subject IRI (`<{DSPACE_BASE}/set/distribution/{asset_id}>`). */
const DISTRIBUTION_URI_SENTINEL = 'urn:6gdali:distribution:self'
/** The bare asset id, used as dali:assetId. */
const ASSET_ID_SENTINEL = 'urn:6gdali:distribution:asset-id'
/** The Data Lake object URL, known only after the upload. */
const DOWNLOAD_URL_SENTINEL = 'urn:6gdali:datalake:object-url'
/** The EDC connector's negotiation entrypoint — server configuration. */
const ACCESS_URL_SENTINEL = 'urn:6gdali:edc:connector-url'

const ACCESS_RIGHTS: Record<string, string> = {
  PUBLIC: 'http://publications.europa.eu/resource/authority/access-right/PUBLIC',
  RESTRICTED: 'http://publications.europa.eu/resource/authority/access-right/RESTRICTED',
  NON_PUBLIC: 'http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC',
}

// Mirrors piveau_dataset_client.CANONICAL_MEDIA_TYPE_BY_EXTENSION — used only
// when the browser gives no content-type or a generic one.
const MEDIA_TYPE_BY_EXTENSION: Record<string, string> = {
  csv: 'text/csv',
  tsv: 'text/tab-separated-values',
  json: 'application/json',
  jsonld: 'application/ld+json',
  jsonl: 'application/jsonl',
  ndjson: 'application/x-ndjson',
  txt: 'text/plain',
  xml: 'application/xml',
  parquet: 'application/parquet',
}

// Mirrors piveau_dataset_client.FILE_TYPE_IRI_BY_EXTENSION. dct:format must be
// an IRI from the EU file-type authority, which is a different thing from
// dcat:mediaType; JSON Lines has no entry of its own, so it maps to JSON.
const FILE_TYPE_BASE = 'http://publications.europa.eu/resource/authority/file-type'
const FILE_TYPE_IRI_BY_EXTENSION: Record<string, string> = {
  csv: `${FILE_TYPE_BASE}/CSV`,
  tsv: `${FILE_TYPE_BASE}/TSV`,
  json: `${FILE_TYPE_BASE}/JSON`,
  jsonl: `${FILE_TYPE_BASE}/JSON`,
  ndjson: `${FILE_TYPE_BASE}/JSON`,
  jsonld: `${FILE_TYPE_BASE}/JSON_LD`,
  txt: `${FILE_TYPE_BASE}/TXT`,
  xml: `${FILE_TYPE_BASE}/XML`,
  parquet: `${FILE_TYPE_BASE}/PARQUET`,
  tar: `${FILE_TYPE_BASE}/TAR`,
  zip: `${FILE_TYPE_BASE}/ZIP`,
  gz: `${FILE_TYPE_BASE}/GZIP`,
}

const GENERIC_MEDIA_TYPES = new Set(['', 'application/octet-stream', 'binary/octet-stream'])

const PREFIXES = [
  '@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .',
  '@prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .',
  '@prefix dcat:   <http://www.w3.org/ns/dcat#> .',
  '@prefix dct:    <http://purl.org/dc/terms/> .',
  '@prefix adms:   <http://www.w3.org/ns/adms#> .',
  '@prefix foaf:   <http://xmlns.com/foaf/0.1/> .',
  '@prefix vcard:  <http://www.w3.org/2006/vcard/ns#> .',
  '@prefix gax:    <https://registry.lab.gaia-x.eu/v1/api/trusted-shape-registry/v1/shapes/jsonld/trustframework#> .',
  '@prefix schema: <https://schema.org/> .',
  '@prefix dali:   <https://dali-project.eu/ns#> .',
  '@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .',
]

/** The @prefix block every document here shares. Exported so the UI can hoist
 *  it above the graph and collapse it — it is boilerplate that otherwise
 *  crowds out the part a submitter is actually reading. */
export const TURTLE_PREFIXES = PREFIXES.join('\n')

/** How many declarations the collapsed prefix summary reports. */
export const TURTLE_PREFIX_COUNT = PREFIXES.length

/** Escape a value for a Turtle single-quoted string literal. Newlines and tabs
 *  are escaped too, not just quotes and backslashes — the description field is a
 *  textarea, and a raw newline inside `"..."` is a Turtle syntax error. */
function esc(value: string): string {
  return value
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n')
    .replace(/\t/g, '\\t')
}

/** A bare Turtle numeric literal, or null when the field is empty/unparseable —
 *  mirrors the backend's numericOrNull + `int(v) if float(v).is_integer()`. */
function numericLiteral(value: string, integer = false): string | null {
  const trimmed = value.trim()
  if (trimmed === '') return null
  const n = integer ? parseInt(trimmed, 10) : Number(trimmed)
  if (!Number.isFinite(n)) return null
  return Number.isInteger(n) ? String(n) : String(n)
}

function today(): string {
  return new Date().toISOString().slice(0, 10)
}

/** ORCIDs are recorded as resolvable IRIs, so a bare 0000-…-form is expanded. */
function orcidIri(orcid: string): string {
  const trimmed = orcid.trim()
  return trimmed.startsWith('http') ? trimmed : `https://orcid.org/${trimmed}`
}

function testbedContextLines(tc: TestbedContextInput): string[] {
  const scalars: [string, string][] = [
    ['dali:environment', tc.environment],
    ['dali:networkDomain', tc.network_domain],
    ['dali:ran3gppRelease', tc.ran_3gpp_release],
    ['dali:ranNewRadioType', tc.ran_new_radio_type],
    ['dali:ranSplit', tc.ran_split],
    ['dali:ranFocusedTechnology', tc.ran_focused_technology],
    ['dali:ranCoverageType', tc.ran_coverage_type],
    ['dali:ranMobilityModel', tc.ran_mobility_model],
    ['dali:coreRelease', tc.core_release],
    ['dali:coreSolution', tc.core_solution],
    ['dali:transportType', tc.transport_type],
    ['dali:computeOrchestratorType', tc.compute_orchestrator_type],
    ['dali:computeVirtualizationType', tc.compute_virtualization_type],
    ['dali:computeInfrastructureType', tc.compute_infrastructure_type],
    ['dali:trafficOrigin', tc.traffic_origin],
    ['dali:trafficPattern', tc.traffic_pattern],
    ['dali:sliceType', tc.slice_type],
    ['dali:referencePlane', tc.reference_plane],
    ['dali:relatedVertical', tc.related_vertical],
    ['dali:observationPointHorizontal', tc.observation_point_horizontal],
    ['dali:observationPointVertical', tc.observation_point_vertical],
  ]

  const lines: string[] = []
  if (tc.underlay_platform.trim()) {
    lines.push(`        dali:underlayPlatform <${tc.underlay_platform.trim()}> ;`)
  }
  for (const [predicate, value] of scalars) {
    if (!value || !value.trim()) continue
    lines.push(`        ${predicate} "${esc(value.trim())}" ;`)
  }

  const bandwidth = numericLiteral(tc.ran_bandwidth_mhz)
  if (bandwidth !== null) lines.push(`        dali:ranBandwidthMHz ${bandwidth} ;`)
  const maxDevices = numericLiteral(tc.ran_max_end_devices, true)
  if (maxDevices !== null) lines.push(`        dali:ranMaxEndDevices ${maxDevices} ;`)

  // Deliberate divergence from the Python builder, which emits the boolean
  // whenever it is not None: the form has no GPU-use control, so its value is
  // always the `false` from emptyTestbedContext. That means "unanswered", not
  // "no GPU", and must not be asserted. The backend keeps its behaviour for API
  // clients that send `false` on purpose. Fold these together if a control is
  // ever added here.
  if (tc.compute_gpu_use) lines.push('        dali:computeGpuUse true ;')

  // Repeatable properties — one triple per value, blanks skipped (an unfilled
  // "+ Add" row must not become an empty literal).
  const repeatable: [string, string[]][] = [
    ['dali:ranFrequencyBand', tc.ran_frequency_band],
    ['dali:measurementFamily', tc.measurement_family],
    ['dali:measurementTool', tc.measurement_tool],
  ]
  for (const [predicate, values] of repeatable) {
    for (const value of values) {
      if (!value || !value.trim()) continue
      lines.push(`        ${predicate} "${esc(value.trim())}" ;`)
    }
  }
  return lines
}

export interface BuildDatasetTurtleInput {
  identity: DatasetIdentityInput
  object: DatasetObjectInput
  testbedContext: TestbedContextInput
}

export interface BuildDatasetBodyOptions {
  /** Add a dcat:distribution link to the pending distribution placeholder.
   *
   *  DISPLAY ONLY — never set this for a document that will be submitted. The
   *  distribution does not exist yet at POST /datasets time, so the link would
   *  publish a dangling reference to a urn: that resolves to nothing. It exists
   *  so the panel can present the dataset and its distribution as one graph. */
  distributionRef?: boolean
}

/** The dataset's own statements, without the @prefix block. */
export function buildDatasetBody(
  { identity, object, testbedContext }: BuildDatasetTurtleInput,
  { distributionRef = false }: BuildDatasetBodyOptions = {}
): string {
  const accessRightsUri = ACCESS_RIGHTS[object.access_rights] ?? ACCESS_RIGHTS.PUBLIC
  // Same fallback as the backend builder: an unspecified publication date
  // becomes today's.
  const issued = identity.issued.trim() || today()

  const lines: string[] = [
    `<${DATASET_URI_SENTINEL}>`,
    '    rdf:type                dcat:Dataset, gax:DataResource ;',
    `    dct:title               "${esc(identity.title)}"@en ;`,
    `    dct:description         "${esc(identity.description)}"@en ;`,
    `    dct:identifier          "${DATASET_ID_SENTINEL}" ;`,
    `    dct:issued              "${issued}"^^xsd:date ;`,
    `    dct:accessRights        <${accessRightsUri}> ;`,
    `    dct:license             <${object.license.trim()}> ;`,
    `    dali:snsProjectName     "${esc(identity.sns_project_name)}" ;`,
    `    dali:gdprCompliant      ${object.gdpr_compliant} ;`,
    `    dali:fairCompliant      ${object.fair_compliant} ;`,
    `    gax:containsPII         ${object.contains_pii} ;`,
    '    dct:conformsTo          <https://www.go-fair.org/fair-principles/> ;',
  ]

  if (object.produced_by.trim()) {
    lines.push(`    gax:producedBy          <${object.produced_by.trim()}> ;`)
  }
  if (identity.publisher_name.trim()) {
    lines.push(`    dct:publisher           [ rdf:type foaf:Organization ; foaf:name "${esc(identity.publisher_name.trim())}" ] ;`)
  }
  if (identity.contact_email.trim()) {
    lines.push(`    dcat:contactPoint       [ rdf:type vcard:Organization ; vcard:hasEmail <mailto:${identity.contact_email.trim()}> ] ;`)
  }
  for (const creator of identity.creators) {
    if (!creator.name.trim()) continue
    const nodeType = creator.kind === 'Organization' ? 'foaf:Organization' : 'foaf:Person'
    const parts = [`rdf:type ${nodeType}`, `foaf:name "${esc(creator.name.trim())}"`]
    if (creator.orcid.trim()) parts.push(`schema:identifier <${orcidIri(creator.orcid)}>`)
    if (creator.affiliation.trim()) parts.push(`schema:affiliation "${esc(creator.affiliation.trim())}"`)
    lines.push(`    dct:creator             [ ${parts.join(' ; ')} ] ;`)
  }
  for (const contributor of identity.contributors) {
    if (!contributor.trim()) continue
    lines.push(`    dct:contributor         [ rdf:type foaf:Agent ; foaf:name "${esc(contributor.trim())}" ] ;`)
  }
  for (const keyword of identity.keywords) {
    if (!keyword.trim()) continue
    lines.push(`    dcat:keyword            "${esc(keyword.trim())}"@en ;`)
  }
  for (const publication of identity.related_publications) {
    if (!publication.trim()) continue
    lines.push(`    dct:relation            <${publication.trim()}> ;`)
  }
  if (identity.language.trim()) {
    lines.push(`    dct:language            <http://publications.europa.eu/resource/authority/language/${identity.language.trim()}> ;`)
  }
  if (identity.spatial.trim()) {
    lines.push(`    dct:spatial             "${esc(identity.spatial.trim())}" ;`)
  }
  // Both bounds required — dct:temporal is a dct:PeriodOfTime node, and a
  // half-open interval would be published as a period with a missing end.
  if (identity.temporal_start && identity.temporal_end) {
    lines.push(
      '    dct:temporal            [ rdf:type dct:PeriodOfTime ; ' +
      `dcat:startDate "${identity.temporal_start}"^^xsd:date ; dcat:endDate "${identity.temporal_end}"^^xsd:date ] ;`
    )
  }
  if (identity.version.trim()) {
    lines.push(`    adms:version            "${esc(identity.version.trim())}" ;`)
  }

  // Reads as one of the dataset's own properties, immediately before the
  // testbed-context block.
  if (distributionRef) {
    lines.push(`    dcat:distribution       <${DISTRIBUTION_URI_SENTINEL}> ;`)
  }

  const tcLines = testbedContextLines(testbedContext)
  if (tcLines.length > 0) {
    lines.push('    dali:testbedContext     [')
    lines.push('        rdf:type dali:TestbedContext ;')
    lines.push(...tcLines)
    lines.push('    ] ;')
  }

  // Close the dataset resource: the last predicate's ';' becomes '.'
  lines[lines.length - 1] = lines[lines.length - 1].replace(/\s*;$/, ' .')

  return lines.join('\n')
}

/** The complete MAP dataset record, exactly as submitted to POST /datasets/rdf:
 *  the @prefix block plus the dataset's statements, with sentinel identifiers
 *  for the orchestrator to substitute. Never carries a dcat:distribution link —
 *  distributions are added afterwards, one at a time, by
 *  POST /datasets/{id}/distributions. */
export function buildDatasetTurtle(input: BuildDatasetTurtleInput): string {
  return `${TURTLE_PREFIXES}\n\n${buildDatasetBody(input)}`
}

export interface BuildDistributionTurtleInput {
  file: File
  metrics: DistributionMetricsInput
  /** The dataset's license — distributions inherit it (dct:license is
   *  mandatory on dcat:Distribution at Violation severity). */
  license: string
}

/**
 * The dcat:Distribution node that adding this file will create — statements
 * only, no @prefix block and no dataset stanza, so it can sit directly below
 * the dataset body in one continuous document. The dataset's side of the link
 * comes from buildDatasetBody's `distributionRef` option.
 *
 * Unlike the dataset record, this is NOT what gets submitted. The distribution
 * is registered by POST /datasets/{id}/distributions, which uploads the file and
 * then builds this node server-side (piveau_dataset_client.add_distribution) —
 * it has to, because three of the values only exist after the upload: the asset
 * id, the Data Lake object URL, and the connector's accessURL. Those appear here
 * as `urn:6gdali:…` placeholders. Everything else is derived exactly as the
 * server derives it, so the preview is otherwise faithful.
 */
export function buildDistributionBody({ file, metrics, license }: BuildDistributionTurtleInput): string {
  const ext = (file.name.split('.').pop() ?? '').toLowerCase()
  // Same precedence as the server's resolve_media_type: trust the browser's
  // content-type unless it is missing or too generic to be worth recording.
  const browserType = (file.type || '').toLowerCase().trim()
  const mediaType = !GENERIC_MEDIA_TYPES.has(browserType)
    ? browserType
    : MEDIA_TYPE_BY_EXTENSION[ext] ?? browserType
  const formatIri = FILE_TYPE_IRI_BY_EXTENSION[ext]

  const lines: string[] = [
    `<${DISTRIBUTION_URI_SENTINEL}>`,
    '    rdf:type                dcat:Distribution ;',
    `    dct:title               "${esc(file.name)}"@en ;`,
    `    dct:license             <${license.trim()}> ;`,
  ]
  if (formatIri) lines.push(`    dct:format              <${formatIri}> ;`)
  if (mediaType) lines.push(`    dcat:mediaType          "${esc(mediaType)}" ;`)
  lines.push(`    dcat:byteSize           "${file.size}"^^xsd:nonNegativeInteger ;`)
  lines.push(`    dcat:accessURL          <${ACCESS_URL_SENTINEL}> ;`)
  lines.push(`    dcat:downloadURL        <${DOWNLOAD_URL_SENTINEL}> ;`)
  lines.push(`    dali:assetId            "${ASSET_ID_SENTINEL}" ;`)
  lines.push('    dali:connectorType      "dspaceconnector" ;')

  for (const variable of metrics.variable_measured) {
    if (!variable.trim()) continue
    lines.push(`    schema:variableMeasured "${esc(variable.trim())}" ;`)
  }
  if (metrics.measurement_technique.trim()) {
    lines.push(`    schema:measurementTechnique "${esc(metrics.measurement_technique.trim())}"@en ;`)
  }

  lines[lines.length - 1] = lines[lines.length - 1].replace(/\s*;$/, ' .')

  return lines.join('\n')
}

/** The complete distribution document, exactly as submitted to
 *  POST /datasets/{id}/distributions/rdf: the @prefix block plus the
 *  distribution's statements.
 *
 *  Submit this, and use buildDistributionBody only for composing a view that
 *  supplies its own prefixes. A body on its own is NOT a parseable Turtle
 *  document — every term in it is prefixed, so submitting one is rejected with
 *  "Prefix rdf: not bound". */
export function buildDistributionTurtle(input: BuildDistributionTurtleInput): string {
  return `${TURTLE_PREFIXES}\n\n${buildDistributionBody(input)}`
}
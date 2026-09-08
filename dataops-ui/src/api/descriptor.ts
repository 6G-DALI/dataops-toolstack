// Client for the DALI Dataset Descriptor Lambda (a separate service from the
// DataOps Orchestrator): it fetches a Zenodo record and normalises it into a
// DCAT-style metadata object via Amazon Bedrock. Called directly from the
// browser — the Lambda's Function URL sets `Access-Control-Allow-Origin: *`
// and needs no bearer token, so this does not go through api/airflow.ts.

import { config } from '../config'
import type { DescriptorMetadata } from '../map/zenodoDescribe'

export interface DescribeResponse {
  metadata: DescriptorMetadata
  /** The raw record as fetched from the source, untouched. */
  raw: unknown
}

/**
 * Describe a dataset from its Zenodo URL or DOI.
 *
 * Resolves with the normalised metadata, or rejects with the Lambda's own error
 * message (it returns `{ error }` with a 4xx/5xx for an unrecognised link, a
 * Zenodo failure, or a model error).
 */
export async function describeDataset(url: string): Promise<DescribeResponse> {
  if (!config.descriptorUrl) {
    throw new Error('Descriptor service URL is not configured (VITE_DESCRIPTOR_URL).')
  }

  let response: Response
  try {
    response = await fetch(config.descriptorUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url }),
    })
  } catch {
    // A TypeError from fetch means the request never completed — the Lambda is
    // unreachable, or a network/CORS failure — rather than an HTTP error status.
    throw new Error('Could not reach the descriptor service. Check that the Lambda is deployed and reachable.')
  }

  const data = await response.json().catch(() => ({}))
  if (!response.ok) {
    const message = (data as { error?: string }).error
    throw new Error(message || `Descriptor request failed with status ${response.status}.`)
  }
  return data as DescribeResponse
}

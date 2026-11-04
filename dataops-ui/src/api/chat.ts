import type { ChatMessage } from '../types'

/**
 * The agent chat's backend seam. `streamReply` yields the assistant's answer as it is produced, so the page renders
 * it token by token and can stop it midway (the signal).
 *
 * There is no agent behind this yet: the reply comes from a local mock that echoes the request. When an agent service
 * exists, replace the body of `streamReply` with a call to it (through the orchestrator's /api proxy, with the user's
 * token) that yields the streamed text; the page needs no other change.
 */

/** Whether replies come from the local mock instead of an agent. The page says so to the user. */
export const CHAT_IS_MOCK = true

const delay = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    const t = setTimeout(resolve, ms)
    signal.addEventListener('abort', () => {
      clearTimeout(t)
      reject(new DOMException('Aborted', 'AbortError'))
    }, { once: true })
  })

function mockAnswer(history: ChatMessage[]): string {
  const last = [...history].reverse().find(m => m.role === 'user')?.content ?? ''
  return [
    'This is a demo reply: no agent is connected to this page yet.',
    last ? `You asked: "${last}".` : '',
    'Once an agent is connected, its answer will appear here as it is written.',
  ].filter(Boolean).join('\n\n')
}

export async function* streamReply(history: ChatMessage[], signal: AbortSignal): AsyncGenerator<string> {
  await delay(400, signal)
  for (const word of mockAnswer(history).split(/(\s+)/)) {
    if (!word) continue
    await delay(35, signal)
    yield word
  }
}

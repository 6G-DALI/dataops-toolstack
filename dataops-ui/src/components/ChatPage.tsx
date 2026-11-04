import { useEffect, useRef, useState, type KeyboardEvent } from 'react'
import { FiSend, FiSquare, FiTrash2 } from 'react-icons/fi'
import { CHAT_IS_MOCK, streamReply } from '../api/chat'
import type { ChatMessage } from '../types'
import '../styles/Chat.css'

let nextId = 0
const newMessage = (role: ChatMessage['role'], content: string): ChatMessage => ({ id: `m${++nextId}`, role, content })

/** A chat session with the agent. The conversation lives in this page only: leaving it starts a new one. */
export default function ChatPage() {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abort = useRef<AbortController | null>(null)
  const end = useRef<HTMLDivElement>(null)

  useEffect(() => { end.current?.scrollIntoView({ block: 'end' }) }, [messages])
  // Leaving the page stops a reply still being written.
  useEffect(() => () => abort.current?.abort(), [])

  async function send() {
    const text = input.trim()
    if (!text || busy) return
    const user = newMessage('user', text)
    const reply = newMessage('assistant', '')
    const history = [...messages, user]
    setMessages([...history, reply])
    setInput('')
    setError(null)
    setBusy(true)
    const ctrl = new AbortController()
    abort.current = ctrl
    try {
      for await (const chunk of streamReply(history, ctrl.signal)) {
        setMessages(ms => ms.map(m => (m.id === reply.id ? { ...m, content: m.content + chunk } : m)))
      }
    } catch (e) {
      // Stopping keeps what was written so far; any other failure drops the empty reply and says why.
      if ((e as Error).name !== 'AbortError') {
        setError((e as Error).message || 'The agent could not answer.')
        setMessages(ms => ms.filter(m => m.id !== reply.id || m.content))
      }
    } finally {
      setBusy(false)
      abort.current = null
    }
  }

  function clear() {
    abort.current?.abort()
    setMessages([])
    setError(null)
  }

  function onKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    // Enter sends, Shift+Enter adds a line; Enter while composing (IME) is not a send.
    if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault()
      void send()
    }
  }

  return (
    <div className="chat-page">
      <div className="d-flex align-items-start justify-content-between mb-2">
        <div>
          <h2 className="page-title">Agent chat</h2>
          <p className="page-subtitle mb-0">Ask the agent about datasets, testbeds and DataOps runs.</p>
        </div>
        <button type="button" className="btn btn-sm btn-outline-secondary" onClick={clear} disabled={!messages.length && !busy}>
          <FiTrash2 className="me-1" aria-hidden="true" />New conversation
        </button>
      </div>

      {CHAT_IS_MOCK && (
        <div className="alert alert-info py-2 small" role="status">
          <strong>Demo.</strong> No agent is connected yet; replies are placeholders.
        </div>
      )}

      <div className="chat-log" role="log" aria-live="polite" aria-label="Conversation">
        {messages.length === 0 && <p className="chat-empty">Start the conversation by typing a message below.</p>}
        {messages.map(m => (
          <div key={m.id} className={`chat-message chat-message-${m.role}`}>
            <div className="chat-role">{m.role === 'user' ? 'You' : 'Agent'}</div>
            <div className="chat-bubble">{m.content || (busy ? <span className="chat-typing" aria-label="The agent is writing">…</span> : '')}</div>
          </div>
        ))}
        <div ref={end} />
      </div>

      {error && <div className="alert alert-danger py-2 small mt-2" role="alert">{error}</div>}

      <form className="chat-input mt-2" onSubmit={e => { e.preventDefault(); void send() }}>
        <textarea
          className="form-control"
          rows={2}
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={onKeyDown}
          placeholder="Message the agent (Enter to send, Shift+Enter for a new line)"
          aria-label="Message"
        />
        {busy ? (
          <button type="button" className="btn btn-outline-danger" onClick={() => abort.current?.abort()}>
            <FiSquare className="me-1" aria-hidden="true" />Stop
          </button>
        ) : (
          <button type="submit" className="btn btn-primary" disabled={!input.trim()}>
            <FiSend className="me-1" aria-hidden="true" />Send
          </button>
        )}
      </form>
    </div>
  )
}

import { useEffect, useRef } from 'react'
import { EditorState } from '@codemirror/state'
import { EditorView, keymap, lineNumbers, highlightActiveLine } from '@codemirror/view'
import { defaultKeymap, indentWithTab, history, historyKeymap } from '@codemirror/commands'
import { python } from '@codemirror/lang-python'
import { indentOnInput, bracketMatching, foldGutter } from '@codemirror/language'

const theme = EditorView.theme({
  '&': { fontSize: '13px', height: '100%' },
  '.cm-scroller': { fontFamily: "'Fira Mono', 'Consolas', monospace", overflow: 'auto' },
  '.cm-content': { padding: '8px 0' },
  '.cm-gutters': { background: '#f8f9fa', borderRight: '1px solid #dee2e6', color: '#999' },
  '.cm-activeLine': { background: '#f0f4ff' },
  '.cm-activeLineGutter': { background: '#e8eeff' },
})

export default function CodeEditor({ value, onChange, height = '400px' }) {
  const containerRef = useRef(null)
  const viewRef = useRef(null)

  useEffect(() => {
    const state = EditorState.create({
      doc: value,
      extensions: [
        lineNumbers(),
        foldGutter(),
        highlightActiveLine(),
        history(),
        indentOnInput(),
        bracketMatching(),
        python(),
        keymap.of([...defaultKeymap, ...historyKeymap, indentWithTab]),
        EditorView.updateListener.of(update => {
          if (update.docChanged) onChange(update.state.doc.toString())
        }),
        theme,
      ],
    })
    viewRef.current = new EditorView({ state, parent: containerRef.current })
    return () => viewRef.current?.destroy()
  }, [])

  // Sync external value changes (e.g. reset)
  useEffect(() => {
    const view = viewRef.current
    if (!view) return
    const current = view.state.doc.toString()
    if (current !== value) {
      view.dispatch({
        changes: { from: 0, to: current.length, insert: value },
      })
    }
  }, [value])

  return (
    <div
      ref={containerRef}
      style={{
        height,
        border: '1px solid #dee2e6',
        borderRadius: '4px',
        overflow: 'hidden',
      }}
    />
  )
}

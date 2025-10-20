import React, { useEffect, useState } from 'react'
import { loadFromLocal, saveToLocal } from '../utils/localStorageHelpers'
import ReactMarkdown from 'react-markdown'

export default function Notes(){
  const [notes, setNotes] = useState([])
  const [form, setForm] = useState({title:'',content:''})
  const [filter, setFilter] = useState('')

  useEffect(()=> setNotes(loadFromLocal('notesData')),[])
  useEffect(()=> saveToLocal('notesData', notes),[notes])

  function add(e){ e.preventDefault(); setNotes([{...form,id:Date.now(),date:new Date().toISOString()},...notes]); setForm({title:'',content:''}) }
  function remove(id){ setNotes(notes.filter(n=>n.id!==id)) }

  const list = notes.filter(n=> (n.title+n.content).toLowerCase().includes(filter.toLowerCase()))

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-4">Notes</h2>
      <form onSubmit={add} className="mb-4 p-4 bg-white dark:bg-gray-800 border rounded">
        <input value={form.title} onChange={e=>setForm({...form,title:e.target.value})} placeholder="Title" className="w-full px-2 py-1 border rounded mb-2" />
        <textarea value={form.content} onChange={e=>setForm({...form,content:e.target.value})} placeholder="Write... (Markdown supported)" className="w-full h-28 px-2 py-1 border rounded" />
        <div className="mt-2 flex justify-end">
          <button className="px-3 py-1 bg-indigo-600 text-white rounded">Save</button>
        </div>
      </form>

      <div className="p-4 bg-white dark:bg-gray-800 border rounded">
        <div className="flex justify-between mb-2">
          <h3 className="font-semibold">All Notes</h3>
          <input placeholder="Search" value={filter} onChange={e=>setFilter(e.target.value)} className="px-2 py-1 border rounded" />
        </div>
        <ul>
          {list.map(n=> (
            <li key={n.id} className="border-b py-3">
              <div className="flex justify-between items-center">
                <div>
                  <div className="font-semibold">{n.title}</div>
                  <div className="text-sm text-gray-500">{new Date(n.date).toLocaleString()}</div>
                </div>
                <button onClick={()=>remove(n.id)} className="text-red-500">Delete</button>
              </div>
              <div className="mt-2 prose max-w-none"><ReactMarkdown>{n.content}</ReactMarkdown></div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

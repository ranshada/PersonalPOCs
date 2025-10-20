import React, { useEffect, useState } from 'react'
import { loadFromLocal, saveToLocal } from '../utils/localStorageHelpers'

export default function Tasks(){
  const [tasks, setTasks] = useState([])
  const [form, setForm] = useState({title:'',priority:'Medium',due:''})
  const [filter, setFilter] = useState('')

  useEffect(()=> setTasks(loadFromLocal('tasksData')),[])
  useEffect(()=> saveToLocal('tasksData', tasks),[tasks])

  function add(e){ e.preventDefault(); setTasks([{...form,id:Date.now(),done:false},...tasks]); setForm({title:'',priority:'Medium',due:''}) }
  function toggle(id){ setTasks(tasks.map(t=> t.id===id? {...t,done:!t.done}:t)) }
  function remove(id){ setTasks(tasks.filter(t=>t.id!==id)) }

  const list = tasks.filter(t=> t.title.toLowerCase().includes(filter.toLowerCase()))

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-4">Tasks</h2>
      <form onSubmit={add} className="mb-4 p-4 bg-white dark:bg-gray-800 border rounded">
        <div className="flex gap-2">
          <input value={form.title} onChange={e=>setForm({...form,title:e.target.value})} placeholder="Task title" className="flex-1 px-2 py-1 border rounded" />
          <select value={form.priority} onChange={e=>setForm({...form,priority:e.target.value})} className="px-2 py-1 border rounded">
            <option>High</option>
            <option>Medium</option>
            <option>Low</option>
          </select>
          <input type="date" value={form.due} onChange={e=>setForm({...form,due:e.target.value})} className="px-2 py-1 border rounded" />
          <button className="px-3 py-1 bg-indigo-600 text-white rounded">Add</button>
        </div>
      </form>

      <div className="p-4 bg-white dark:bg-gray-800 border rounded">
        <div className="flex justify-between mb-2">
          <h3 className="font-semibold">My Tasks</h3>
          <input placeholder="Filter" value={filter} onChange={e=>setFilter(e.target.value)} className="px-2 py-1 border rounded" />
        </div>
        <ul>
          {list.map(t=> (
            <li key={t.id} className="flex justify-between items-center border-b py-2">
              <div>
                <div className={`font-semibold ${t.done? 'line-through text-gray-500':''}`}>{t.title}</div>
                <div className="text-sm text-gray-500">{t.priority} - {t.due}</div>
              </div>
              <div className="flex items-center gap-2">
                <button onClick={()=>toggle(t.id)} className="px-2 py-1 bg-green-600 text-white rounded">{t.done? 'Undo':'Done'}</button>
                <button onClick={()=>remove(t.id)} className="text-red-500">Delete</button>
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

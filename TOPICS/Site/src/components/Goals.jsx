import React, { useEffect, useState } from 'react'
import { loadFromLocal, saveToLocal } from '../utils/localStorageHelpers'

export default function Goals(){
  const [goals, setGoals] = useState([])
  const [form, setForm] = useState({title:'',target:100})

  useEffect(()=> setGoals(loadFromLocal('goalsData')),[])
  useEffect(()=> saveToLocal('goalsData', goals),[goals])

  function add(e){ e.preventDefault(); setGoals([{...form,id:Date.now(),progress:0},...goals]); setForm({title:'',target:100}) }
  function inc(id){ setGoals(goals.map(g=> g.id===id? {...g,progress: Math.min(g.target, g.progress+1)}:g)) }
  function remove(id){ setGoals(goals.filter(g=>g.id!==id)) }

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-4">Goals</h2>
      <form onSubmit={add} className="mb-4 p-4 bg-white dark:bg-gray-800 border rounded">
        <div className="flex gap-2">
          <input value={form.title} onChange={e=>setForm({...form,title:e.target.value})} placeholder="Goal title" className="flex-1 px-2 py-1 border rounded" />
          <input type="number" value={form.target} onChange={e=>setForm({...form,target: Number(e.target.value)})} className="w-28 px-2 py-1 border rounded" />
          <button className="px-3 py-1 bg-indigo-600 text-white rounded">Add</button>
        </div>
      </form>

      <div className="p-4 bg-white dark:bg-gray-800 border rounded">
        <ul>
          {goals.map(g=> (
            <li key={g.id} className="border-b py-3">
              <div className="flex justify-between items-center">
                <div>
                  <div className="font-semibold">{g.title}</div>
                  <div className="text-sm text-gray-500">{g.progress}/{g.target}</div>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-40 bg-gray-200 rounded h-3 overflow-hidden">
                    <div style={{width: `${(g.progress/g.target)*100}%`}} className="bg-indigo-500 h-full"></div>
                  </div>
                  <button onClick={()=>inc(g.id)} className="px-2 py-1 bg-green-600 text-white rounded">+1</button>
                  <button onClick={()=>remove(g.id)} className="text-red-500">Delete</button>
                </div>
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

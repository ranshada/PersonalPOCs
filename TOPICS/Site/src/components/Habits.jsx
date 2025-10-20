import React, { useEffect, useState } from 'react'
import { loadFromLocal, saveToLocal } from '../utils/localStorageHelpers'

function todayKey(){ return new Date().toISOString().slice(0,10) }

export default function Habits(){
  const [habits, setHabits] = useState([])
  const [name, setName] = useState('')

  useEffect(()=> setHabits(loadFromLocal('habitsData')),[])
  useEffect(()=> saveToLocal('habitsData', habits),[habits])

  function add(){ if(!name) return; setHabits([{id:Date.now(),name,history:{}},...habits]); setName('') }
  function toggle(h){
    const key = todayKey()
    const next = habits.map(item=>{
      if(item.id===h.id){
        const hist = {...item.history}
        hist[key] = !hist[key]
        return {...item, history: hist}
      }
      return item
    })
    setHabits(next)
  }

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-4">Habits</h2>
      <div className="mb-4 p-4 bg-white dark:bg-gray-800 border rounded">
        <div className="flex gap-2">
          <input value={name} onChange={e=>setName(e.target.value)} placeholder="New habit" className="flex-1 px-2 py-1 border rounded" />
          <button onClick={add} className="px-3 py-1 bg-indigo-600 text-white rounded">Add</button>
        </div>
      </div>

      <div className="p-4 bg-white dark:bg-gray-800 border rounded">
        <h3 className="font-semibold mb-2">Today</h3>
        <ul>
          {habits.map(h=> (
            <li key={h.id} className="flex justify-between items-center border-b py-2">
              <div>{h.name}</div>
              <div className="flex items-center gap-2">
                <button onClick={()=>toggle(h)} className={`px-3 py-1 rounded ${h.history && h.history[todayKey()] ? 'bg-green-600 text-white':'bg-gray-200'}`}>
                  {h.history && h.history[todayKey()] ? 'Done' : 'Mark'}
                </button>
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

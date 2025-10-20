import React, { useEffect, useState } from 'react'
import { loadFromLocal } from '../utils/localStorageHelpers'

export default function CalendarView(){
  const [date, setDate] = useState(new Date())
  const [tasks, setTasks] = useState([])
  const [finance, setFinance] = useState([])

  useEffect(()=>{
    setTasks(loadFromLocal('tasksData'))
    setFinance(loadFromLocal('financeData'))
  },[])

  function startOfMonth(d){ return new Date(d.getFullYear(), d.getMonth(),1) }
  function daysInMonth(d){ return new Date(d.getFullYear(), d.getMonth()+1,0).getDate() }

  const start = startOfMonth(date)
  const days = daysInMonth(date)
  const arr = Array.from({length:days},(_,i)=> new Date(date.getFullYear(), date.getMonth(), i+1))

  function entriesFor(day){
    const dayStr = day.toDateString()
    return {
      tasks: tasks.filter(t=> t.due && new Date(t.due).toDateString()===dayStr),
      finance: finance.filter(f=> new Date(f.date).toDateString()===dayStr)
    }
  }

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-4">Calendar</h2>
      <div className="grid grid-cols-7 gap-2">
        {arr.map(d=>{
          const e = entriesFor(d)
          return (
            <div key={d.toISOString()} className="p-2 border rounded bg-white dark:bg-gray-800">
              <div className="font-semibold">{d.getDate()}</div>
              <div className="text-xs text-gray-500">
                {e.tasks.length} tasks · ${e.finance.reduce((s,f)=> s+Number(f.amount||0),0)}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

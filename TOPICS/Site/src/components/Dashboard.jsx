import React, { useEffect, useState } from 'react'
import { loadFromLocal } from '../utils/localStorageHelpers'

export default function Dashboard(){
  const [finance, setFinance] = useState([])
  const [tasks, setTasks] = useState([])
  useEffect(()=>{
    setFinance(loadFromLocal('financeData'))
    setTasks(loadFromLocal('tasksData'))
  },[])

  const pendingTasks = tasks.filter(t=>!t.done).length
  const todaysExpenses = finance.filter(f => new Date(f.date).toDateString() === new Date().toDateString())
  const totalToday = todaysExpenses.reduce((s,f)=> s + Number(f.amount||0), 0)

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-4">Overview</h2>
      <div className="grid grid-cols-3 gap-4">
        <div className="p-4 rounded bg-white dark:bg-gray-800 border">
          <h3 className="text-sm text-gray-500">Pending Tasks</h3>
          <div className="text-2xl font-bold">{pendingTasks}</div>
        </div>
        <div className="p-4 rounded bg-white dark:bg-gray-800 border">
          <h3 className="text-sm text-gray-500">Today's Expenses</h3>
          <div className="text-2xl font-bold">${totalToday.toFixed(2)}</div>
        </div>
        <div className="p-4 rounded bg-white dark:bg-gray-800 border">
          <h3 className="text-sm text-gray-500">Habits</h3>
          <div className="text-2xl font-bold">-</div>
        </div>
      </div>
    </div>
  )
}

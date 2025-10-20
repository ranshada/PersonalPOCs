import React, { useEffect, useState } from 'react'
import { loadFromLocal, saveToLocal } from '../utils/localStorageHelpers'
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip } from 'recharts'

export default function Finance(){
  const [items, setItems] = useState([])
  const [form, setForm] = useState({type:'expense',category:'General',amount:'',date:'',note:''})
  const [filter, setFilter] = useState('')

  useEffect(()=>{
    setItems(loadFromLocal('financeData'))
  },[])

  useEffect(()=>{ saveToLocal('financeData', items) },[items])

  function addItem(e){
    e.preventDefault()
    const it = {...form, id:Date.now(), amount: Number(form.amount), date: form.date || new Date().toISOString()}
    setItems([it,...items])
    setForm({type:'expense',category:'General',amount:'',date:'',note:''})
  }

  function remove(id){ setItems(items.filter(i=>i.id!==id)) }

  const data = items.slice(0,20).map(i=>({date: new Date(i.date).toLocaleDateString(), value: i.amount}))

  return (
    <div>
      <h2 className="text-2xl font-semibold mb-4">Finance</h2>
      <div className="grid md:grid-cols-3 gap-4">
        <div className="md:col-span-2">
          <form onSubmit={addItem} className="p-4 bg-white dark:bg-gray-800 border rounded mb-4">
            <div className="flex gap-2">
              <select value={form.type} onChange={e=>setForm({...form,type:e.target.value})} className="px-2 py-1 border rounded">
                <option value="expense">Expense</option>
                <option value="income">Income</option>
              </select>
              <input placeholder="Category" value={form.category} onChange={e=>setForm({...form,category:e.target.value})} className="px-2 py-1 border rounded flex-1" />
              <input placeholder="Amount" value={form.amount} onChange={e=>setForm({...form,amount:e.target.value})} className="px-2 py-1 border rounded w-28" />
              <input type="date" value={form.date} onChange={e=>setForm({...form,date:e.target.value})} className="px-2 py-1 border rounded" />
              <button className="px-3 py-1 bg-indigo-600 text-white rounded">Add</button>
            </div>
            <div className="mt-2">
              <input placeholder="Note" value={form.note} onChange={e=>setForm({...form,note:e.target.value})} className="w-full px-2 py-1 border rounded" />
            </div>
          </form>

          <div className="p-4 bg-white dark:bg-gray-800 border rounded">
            <div className="flex justify-between items-center mb-2">
              <h3 className="font-semibold">Recent</h3>
              <input placeholder="Filter category" value={filter} onChange={e=>setFilter(e.target.value)} className="px-2 py-1 border rounded" />
            </div>
            <ul>
              {items.filter(i=>i.category.toLowerCase().includes(filter.toLowerCase())).map(i=> (
                <li key={i.id} className="flex justify-between items-center border-b py-2">
                  <div>
                    <div className="font-semibold">{i.category} <span className="text-sm text-gray-500">({i.type})</span></div>
                    <div className="text-sm text-gray-500">{new Date(i.date).toLocaleString()} - {i.note}</div>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className={`font-bold ${i.type==='expense'? 'text-red-500':'text-green-500'}`}>${i.amount}</div>
                    <button onClick={()=>remove(i.id)} className="text-sm text-red-500">Delete</button>
                  </div>
                </li>
              ))}
            </ul>
          </div>
        </div>

        <div className="p-4 bg-white dark:bg-gray-800 border rounded">
          <h3 className="font-semibold mb-2">Monthly</h3>
          <div style={{height:200}}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data}>
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Line dataKey="value" stroke="#8884d8" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  )
}

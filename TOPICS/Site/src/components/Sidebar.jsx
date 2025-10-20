import React from 'react'
import { FiSun, FiMoon } from 'react-icons/fi'

export default function Sidebar({views,onNavigate,current,theme,setTheme}){
  return (
    <aside className="w-64 border-r border-gray-200 dark:border-gray-800 p-4 bg-white dark:bg-gray-800">
      <div className="mb-6">
        <h1 className="text-xl font-bold">Planner</h1>
        <p className="text-sm text-gray-500">Local / Private</p>
      </div>
      <nav className="space-y-2">
        {views.map(v=> (
          <button key={v} onClick={()=>onNavigate(v)} className={`w-full text-left px-3 py-2 rounded ${current===v? 'bg-indigo-100 dark:bg-indigo-700':''}`}>
            {v}
          </button>
        ))}
      </nav>

      <div className="mt-6">
        <button onClick={()=>setTheme(theme==='light'?'dark':'light')} className="flex items-center gap-2 px-3 py-2 rounded bg-gray-100 dark:bg-gray-700">
          {theme==='light' ? <FiMoon/> : <FiSun/>}
          <span className="text-sm">Toggle Theme</span>
        </button>
      </div>
    </aside>
  )
}

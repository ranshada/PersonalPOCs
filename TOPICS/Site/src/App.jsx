import React, { useState, useEffect } from 'react'
import Sidebar from './components/Sidebar'
import Dashboard from './components/Dashboard'
import Finance from './components/Finance'
import Tasks from './components/Tasks'
import Habits from './components/Habits'
import Notes from './components/Notes'
import Goals from './components/Goals'
import CalendarView from './components/Calendar'
import { loadFromLocal, saveToLocal } from './utils/localStorageHelpers'

const VIEWS = ['Dashboard','Finance','Tasks','Habits','Notes','Goals','Calendar']

export default function App(){
  const [view, setView] = useState('Dashboard')
  const [theme, setTheme] = useState('light')

  useEffect(()=>{
    const t = localStorage.getItem('plannerTheme') || 'light'
    setTheme(t)
  },[])

  useEffect(()=>{
    document.documentElement.className = theme
    localStorage.setItem('plannerTheme', theme)
  },[theme])

  // sample demo data load if empty
  useEffect(()=>{
    if(!localStorage.getItem('financeData')){
      saveToLocal('financeData', [
        {id:1,type:'expense',category:'Food',amount:12.5,date:new Date().toISOString(),note:'Lunch'}
      ])
    }
    if(!localStorage.getItem('tasksData')){
      saveToLocal('tasksData', [
        {id:1,title:'Buy groceries',priority:'Medium',due:new Date().toISOString(),done:false}
      ])
    }
    if(!localStorage.getItem('habitsData')){
      saveToLocal('habitsData', [
        {id:1,name:'Drink Water',history:{},color:'#60A5FA'}
      ])
    }
    if(!localStorage.getItem('notesData')){
      saveToLocal('notesData', [
        {id:1,title:'Welcome',content:'This is your local personal planner. All data stored in your browser.'}]
      )
    }
    if(!localStorage.getItem('goalsData')){
      saveToLocal('goalsData', [
        {id:1,title:'Build habit tracker',target:100,progress:10}
      ])
    }
  },[])

  return (
    <div className="min-h-screen flex bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100">
      <Sidebar views={VIEWS} onNavigate={setView} current={view} theme={theme} setTheme={setTheme} />
      <main className="flex-1 p-6">
        {view==='Dashboard' && <Dashboard />}
        {view==='Finance' && <Finance />}
        {view==='Tasks' && <Tasks />}
        {view==='Habits' && <Habits />}
        {view==='Notes' && <Notes />}
        {view==='Goals' && <Goals />}
        {view==='Calendar' && <CalendarView />}
      </main>
    </div>
  )
}

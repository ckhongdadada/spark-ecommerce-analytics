import axios from 'axios'

const client = axios.create({
  timeout: 6000
})

export const fetchHealth = () => client.get('/health')
export const fetchCategoryMetrics = () => client.get('/api/v1/category-metrics')
export const fetchTopUsers = (limit = 10) => client.get(`/api/v1/user-metrics/top?limit=${limit}`)
export const fetchRealtimeMetrics = (limit = 12) => client.get(`/api/v1/realtime/metrics?limit=${limit}`)
export const fetchAlerts = (limit = 8) => client.get(`/api/v1/alerts?limit=${limit}`)
export const fetchFunnel = () => client.get('/api/v1/funnel')

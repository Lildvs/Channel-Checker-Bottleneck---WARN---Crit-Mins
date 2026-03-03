import axios from 'axios'

const API_BASE = '/api'

export interface GoCometCreditStatus {
  remaining_credits: number
  total_credits: number
  refresh_day: string
  hours_until_refresh: number
  last_query: string | null
  queries_this_week: string[]
}

export interface GoCometQueryResponse {
  success: boolean
  credits_remaining: number
  message: string
}

export async function getGoCometCredits(): Promise<GoCometCreditStatus> {
  const response = await axios.get<GoCometCreditStatus>(
    `${API_BASE}/collectors/gocomet/credits`
  )
  return response.data
}

export async function confirmGoCometQuery(): Promise<GoCometQueryResponse> {
  const response = await axios.post<GoCometQueryResponse>(
    `${API_BASE}/collectors/gocomet/confirm-query`
  )
  return response.data
}

export async function declineGoCometQuery(): Promise<GoCometQueryResponse> {
  const response = await axios.post<GoCometQueryResponse>(
    `${API_BASE}/collectors/gocomet/decline-query`
  )
  return response.data
}

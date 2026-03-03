import axios from 'axios'

const API_BASE = '/api'

export interface DataPoint {
  timestamp: string
  value: number
  quality_score?: number
  is_preliminary?: boolean
}

export interface SeriesDataResponse {
  series_id: string
  data_points: DataPoint[]
  count: number
}

export interface SeriesStats {
  series_id: string
  count: number
  mean: number
  stddev: number
  min: number
  max: number
  median: number
}

export interface CollectorStatus {
  name: string
  status: string
  next_run: string
  last_run?: string
  last_error?: string
}

export interface SeriesCatalogItem {
  id: string
  name: string
  category: string
  source: string
  frequency: string
  description?: string
}

const SERIES_CATALOG: Record<string, SeriesCatalogItem[]> = {
  'Economic Indicators': [
    { id: 'GDP', name: 'Gross Domestic Product', category: 'Economic Indicators', source: 'FRED', frequency: 'quarterly' },
    { id: 'GDPC1', name: 'Real GDP', category: 'Economic Indicators', source: 'FRED', frequency: 'quarterly' },
    { id: 'INDPRO', name: 'Industrial Production Index', category: 'Economic Indicators', source: 'FRED', frequency: 'monthly' },
    { id: 'UNRATE', name: 'Unemployment Rate', category: 'Economic Indicators', source: 'FRED', frequency: 'monthly' },
    { id: 'PAYEMS', name: 'Total Nonfarm Payrolls', category: 'Economic Indicators', source: 'FRED', frequency: 'monthly' },
  ],
  'Inflation': [
    { id: 'CPIAUCSL', name: 'Consumer Price Index', category: 'Inflation', source: 'FRED', frequency: 'monthly' },
    { id: 'PPIACO', name: 'Producer Price Index', category: 'Inflation', source: 'FRED', frequency: 'monthly' },
    { id: 'PCE', name: 'Personal Consumption Expenditures', category: 'Inflation', source: 'FRED', frequency: 'monthly' },
  ],
  'Consumer': [
    { id: 'RSXFS', name: 'Retail Sales excl Food Services', category: 'Consumer', source: 'FRED', frequency: 'monthly' },
    { id: 'UMCSENT', name: 'Consumer Sentiment (UMich)', category: 'Consumer', source: 'FRED', frequency: 'monthly' },
    { id: 'CSCICP03USM665S', name: 'Consumer Confidence (OECD)', category: 'Consumer', source: 'FRED', frequency: 'monthly' },
    { id: 'PSAVERT', name: 'Personal Savings Rate', category: 'Consumer', source: 'FRED', frequency: 'monthly' },
    { id: 'TOTALSL', name: 'Consumer Credit', category: 'Consumer', source: 'FRED', frequency: 'monthly' },
  ],
  'Energy': [
    { id: 'DCOILWTICO', name: 'WTI Crude Oil', category: 'Energy', source: 'FRED', frequency: 'daily' },
    { id: 'DHHNGSP', name: 'Henry Hub Natural Gas', category: 'Energy', source: 'FRED', frequency: 'daily' },
    { id: 'GASREGW', name: 'Regular Gas Price', category: 'Energy', source: 'FRED', frequency: 'weekly' },
    { id: 'WCSSTUS1', name: 'Crude Oil Commercial Stocks excl SPR', category: 'Energy', source: 'FRED', frequency: 'weekly' },
    { id: 'WGTSTUS1', name: 'Gasoline Stocks', category: 'Energy', source: 'FRED', frequency: 'weekly' },
    { id: 'WPULEUS3', name: 'Refinery Utilization Rate', category: 'Energy', source: 'FRED', frequency: 'weekly' },
  ],
  'Labor Market': [
    { id: 'JTSJOL', name: 'Job Openings (JOLTS)', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
    { id: 'UNEMPLOY', name: 'Unemployment Level', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
    { id: 'JTSHIR', name: 'JOLTS Hires Rate', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
    { id: 'JTSTSR', name: 'JOLTS Total Separations Rate', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
    { id: 'JTSQUR', name: 'JOLTS Quits Rate', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
    { id: 'FRBKCLMCILA', name: 'KC Fed LMCI Level of Activity', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
    { id: 'FRBKCLMCIM', name: 'KC Fed LMCI Momentum', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
    { id: 'ADPWNUSNERSA', name: 'ADP Nonfarm Private Employment', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
    { id: 'AWHMAN', name: 'Avg Weekly Hours Manufacturing', category: 'Labor Market', source: 'FRED', frequency: 'monthly' },
  ],
  'Interest Rates & Credit': [
    { id: 'DFF', name: 'Federal Funds Rate', category: 'Interest Rates & Credit', source: 'FRED', frequency: 'daily' },
    { id: 'DGS10', name: '10-Year Treasury', category: 'Interest Rates & Credit', source: 'FRED', frequency: 'daily' },
    { id: 'DGS2', name: '2-Year Treasury', category: 'Interest Rates & Credit', source: 'FRED', frequency: 'daily' },
    { id: 'T10Y2Y', name: '10Y-2Y Spread (Yield Curve)', category: 'Interest Rates & Credit', source: 'FRED', frequency: 'daily' },
    { id: 'BAMLH0A0HYM2', name: 'High Yield Corporate Bond Spread', category: 'Interest Rates & Credit', source: 'FRED', frequency: 'daily' },
    { id: 'BUSLOANS', name: 'Commercial & Industrial Loans', category: 'Interest Rates & Credit', source: 'FRED', frequency: 'monthly' },
    { id: 'NFCI', name: 'Chicago Fed Financial Conditions', category: 'Interest Rates & Credit', source: 'FRED', frequency: 'weekly' },
  ],
  'Housing': [
    { id: 'HOUST', name: 'Housing Starts', category: 'Housing', source: 'FRED', frequency: 'monthly' },
    { id: 'PERMIT', name: 'Building Permits', category: 'Housing', source: 'FRED', frequency: 'monthly' },
    { id: 'HSN1F', name: 'New Home Sales', category: 'Housing', source: 'FRED', frequency: 'monthly' },
    { id: 'CSUSHPINSA', name: 'Case-Shiller Home Price Index', category: 'Housing', source: 'FRED', frequency: 'monthly' },
    { id: 'MORTGAGE30US', name: '30-Year Mortgage Rate', category: 'Housing', source: 'FRED', frequency: 'weekly' },
  ],
  'Manufacturing & Inventories': [
    { id: 'DGORDER', name: 'Durable Goods Orders', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
    { id: 'NEWORDER', name: 'Manufacturers New Orders', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
    { id: 'BUSINV', name: 'Business Inventories', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
    { id: 'AMTMNO', name: 'Manufacturers Total Inventories', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
    { id: 'RETAILIMSA', name: 'Retail Inventories', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
    { id: 'ISRATIO', name: 'Business Inventory-to-Sales Ratio', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
    { id: 'RETAILIRSA', name: 'Retail Inventory-to-Sales Ratio', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
    { id: 'MNFCTRIRSA', name: 'Manufacturers Inventory-to-Sales Ratio', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
    { id: 'MCUMFN', name: 'Manufacturing Capacity Utilization', category: 'Manufacturing & Inventories', source: 'FRED', frequency: 'monthly' },
  ],
  'Transportation': [
    { id: 'TSIFRGHT', name: 'Transportation Services Index - Freight', category: 'Transportation', source: 'FRED', frequency: 'monthly' },
    { id: 'RAILFRTCARLOADSD11', name: 'Rail Freight Carloads', category: 'Transportation', source: 'FRED', frequency: 'monthly' },
  ],
  'International': [
    { id: 'DEXUSEU', name: 'USD/EUR Exchange Rate', category: 'International', source: 'FRED', frequency: 'daily' },
    { id: 'DTWEXBGS', name: 'Trade Weighted Dollar Index', category: 'International', source: 'FRED', frequency: 'daily' },
  ],
  'Fiscal Dominance': [
    { id: 'GFDEBTN', name: 'Total Public Debt Outstanding', category: 'Fiscal Dominance', source: 'FRED', frequency: 'quarterly' },
    { id: 'A091RC1Q027SBEA', name: 'Federal Interest Payments', category: 'Fiscal Dominance', source: 'FRED', frequency: 'quarterly' },
    { id: 'W006RC1Q027SBEA', name: 'Federal Tax Receipts', category: 'Fiscal Dominance', source: 'FRED', frequency: 'quarterly' },
    { id: 'WTREGEN', name: 'Treasury General Account Balance', category: 'Fiscal Dominance', source: 'FRED', frequency: 'weekly' },
    { id: 'RRPONTSYD', name: 'Overnight Reverse Repo', category: 'Fiscal Dominance', source: 'FRED', frequency: 'daily' },
    { id: 'WALCL', name: 'Fed Total Assets (Balance Sheet)', category: 'Fiscal Dominance', source: 'FRED', frequency: 'weekly' },
    { id: 'WRESBAL', name: 'Reserve Balances at Fed', category: 'Fiscal Dominance', source: 'FRED', frequency: 'weekly' },
  ],
  'Delinquencies': [
    { id: 'DRCCLACBS', name: 'Credit Card Delinquency Rate', category: 'Delinquencies', source: 'FRED', frequency: 'quarterly' },
    { id: 'DRCLACBS', name: 'Consumer Loan Delinquency Rate', category: 'Delinquencies', source: 'FRED', frequency: 'quarterly' },
    { id: 'DRSFRMACBS', name: 'Student Loan Delinquency Rate', category: 'Delinquencies', source: 'FRED', frequency: 'quarterly' },
    { id: 'CCLACBW027SBOG', name: 'Credit Card Loans Outstanding', category: 'Delinquencies', source: 'FRED', frequency: 'weekly' },
    { id: 'SLOAS', name: 'Student Loans Outstanding', category: 'Delinquencies', source: 'FRED', frequency: 'quarterly' },
  ],
  'Supply Disruption': [
    { id: 'DTCDFNA066MNFRBPHI', name: 'Philly Fed Delivery Time Diffusion', category: 'Supply Disruption', source: 'FRED', frequency: 'monthly' },
  ],
}

// API functions
export async function fetchSeriesData(
  seriesId: string,
  startDate?: string,
  endDate?: string,
  limit?: number
): Promise<SeriesDataResponse> {
  const params: Record<string, string | number> = {}
  if (startDate) params.start_date = startDate
  if (endDate) params.end_date = endDate
  if (limit) params.limit = limit

  const response = await axios.get(`${API_BASE}/data/series/${seriesId}`, { params })
  return response.data
}

export async function fetchSeriesStats(
  seriesId: string,
  lookbackDays = 730,
): Promise<SeriesStats> {
  const params: Record<string, number> = {}
  if (lookbackDays > 0) params.lookback_days = lookbackDays
  const response = await axios.get(`${API_BASE}/data/series/${seriesId}/statistics`, { params })
  return response.data
}

export async function fetchCollectorStatus(): Promise<CollectorStatus[]> {
  const response = await axios.get(`${API_BASE}/data/collectors/status`)
  return response.data
}

export function getSeriesCatalog(): Record<string, SeriesCatalogItem[]> {
  return SERIES_CATALOG
}

export function getAllSeries(): SeriesCatalogItem[] {
  return Object.values(SERIES_CATALOG).flat()
}

export function getSeriesByCategory(category: string): SeriesCatalogItem[] {
  return SERIES_CATALOG[category] || []
}

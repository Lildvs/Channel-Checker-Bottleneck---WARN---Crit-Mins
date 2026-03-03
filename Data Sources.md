# Government Data Sources - Comprehensive Catalog

> **Last Updated:** January 12, 2026  
> **Purpose:** Complete catalog of free, publicly available government data sources (Federal + State) for economic bottleneck detection and fundamental research.

---

## Table of Contents

1. [Federal Government - Economic & Financial](#federal-government---economic--financial)
2. [Federal Government - Industry & Sector Specific](#federal-government---industry--sector-specific)
3. [Federal Government - Trade & International](#federal-government---trade--international)
4. [Federal Government - Regulatory & Filings](#federal-government---regulatory--filings)
5. [Federal Government - Infrastructure & Transportation](#federal-government---infrastructure--transportation)
6. [Federal Government - Natural Resources & Environment](#federal-government---natural-resources--environment)
7. [Federal Government - Labor & Demographics](#federal-government---labor--demographics)
8. [Federal Government - Health & Social](#federal-government---health--social)
9. [Federal Government - Additional Agencies](#federal-government---additional-agencies)
10. [State Government Data Portals](#state-government-data-portals)
11. [International Government Sources](#international-government-sources)
12. [Data Aggregation Portals](#data-aggregation-portals)
13. [Free Alternative & Supplementary Data Sources](#free-alternative--supplementary-data-sources)

---

## Federal Government - Economic & Financial

### Federal Reserve System

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **FRED (Federal Reserve Economic Data)** | Primary economic database with 800,000+ time series | GDP, inflation, employment, interest rates, money supply, housing, international data | ✅ Yes (free key) | Varies by series |
| **Federal Reserve Board (FRB)** | Monetary policy and financial system data | H.4.1 (Fed balance sheet), H.8 (bank assets), G.19 (consumer credit), Z.1 (financial accounts) | ✅ Yes | Weekly/Quarterly |
| **Federal Reserve Banks (Regional)** | Regional economic surveys and research | Beige Book, manufacturing surveys, regional indicators | ✅ Partial | Monthly/8x per year |
| **FRASER (Federal Reserve Archival System)** | Historical economic data and publications | Historical statistics, Fed publications dating to 1800s | ❌ No (PDF/download) | Archival |

**API Endpoint:** `https://api.stlouisfed.org/fred/`  
**Registration:** https://fred.stlouisfed.org/docs/api/api_key.html

#### Regional Federal Reserve Banks - Detailed

| Fed District | Key Surveys/Data | Notable Indicators | Access |
|--------------|------------------|-------------------|--------|
| **Boston (1st)** | New England Economic Indicators | Regional employment, housing | https://www.bostonfed.org/publications/current-economic-conditions/ |
| **New York (2nd)** | Empire State Manufacturing Survey, Consumer Expectations Survey | Manufacturing index, inflation expectations | https://www.newyorkfed.org/data-and-statistics |
| **Philadelphia (3rd)** | Manufacturing Business Outlook Survey, Nonmanufacturing Survey | Philly Fed Index, future capital spending | https://www.philadelphiafed.org/surveys-and-data |
| **Cleveland (4th)** | Inflation Expectations, Community Development | Median CPI, inflation nowcasts | https://www.clevelandfed.org/indicators-and-data |
| **Richmond (5th)** | Fifth District Survey of Manufacturing, Service Sector | Regional manufacturing, services PMI | https://www.richmondfed.org/research/regional_economy |
| **Atlanta (6th)** | GDPNow, Wage Growth Tracker, Business Inflation Expectations | Real-time GDP estimate, wage trends | https://www.atlantafed.org/cqer |
| **Chicago (7th)** | Chicago Fed National Activity Index (CFNAI), Midwest Economy | National activity, Midwest manufacturing | https://www.chicagofed.org/research/data |
| **St. Louis (8th)** | FRED (hosts national data), Regional Economic Briefs | 800,000+ series (national), regional data | https://www.stlouisfed.org/data |
| **Minneapolis (9th)** | Beige Book contributions, Agricultural conditions | Farm economy, regional labor | https://www.minneapolisfed.org/economic-research |
| **Kansas City (10th)** | Manufacturing Survey, Agricultural Credit Survey | 10th District manufacturing, farm credit | https://www.kansascityfed.org/research/regional-research/ |
| **Dallas (11th)** | Texas Manufacturing Outlook, Texas Service Sector Outlook | Texas PMI, oil/gas activity | https://www.dallasfed.org/research/surveys |
| **San Francisco (12th)** | Western Economic Developments, Tech Pulse Index | Tech sector activity, Western US economy | https://www.frbsf.org/economic-research/indicators-data/ |

**Key Regional Fed Indicators for Bottleneck Detection:**
- **Atlanta Fed GDPNow**: Real-time GDP tracking (updated after each data release)
- **NY Fed Empire State Manufacturing**: Early read on manufacturing conditions
- **Philly Fed Manufacturing**: Diffusion index of factory activity
- **Dallas Fed Oil & Gas Activity**: Energy sector health
- **KC Fed Agricultural Credit**: Farm sector stress signals

---

### Bureau of Economic Analysis (BEA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **National Accounts** | GDP and national income | GDP (real, nominal), GDI, personal income, corporate profits | ✅ Yes (free key) | Quarterly |
| **Industry Accounts** | Industry-level output | GDP by industry, input-output tables, value added | ✅ Yes | Annual/Quarterly |
| **Regional Accounts** | State and metro-level data | State GDP, personal income by state/county, employment by region | ✅ Yes | Quarterly/Annual |
| **International Accounts** | Trade and investment flows | Balance of payments, international investment position, trade in services | ✅ Yes | Quarterly |

**API Endpoint:** `https://apps.bea.gov/api/data`  
**Registration:** https://apps.bea.gov/api/signup/

#### Key BEA Datasets for Bottleneck Detection:
- **Input-Output Tables** - Shows inter-industry dependencies (critical for sector mapping)
- **Supply-Use Tables** - Commodity flows between industries
- **GDP by Industry** - Real-time industry health signals

---

### U.S. Treasury Department

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Treasury Direct** | Government debt data | Outstanding debt, auction results, savings bonds | ✅ Yes | Daily |
| **Daily Treasury Statement** | Federal cash position | Daily receipts, outlays, cash balance | ✅ Yes | Daily |
| **Monthly Treasury Statement** | Federal budget | Receipts, outlays, deficit/surplus by category | ✅ Yes | Monthly |
| **Treasury International Capital (TIC)** | Foreign holdings of US assets | Foreign official holdings, cross-border flows | ✅ Yes | Monthly |
| **Yield Curve Rates** | Treasury yield data | Daily yield curve rates (all maturities) | ✅ Yes | Daily |
| **SLGS Rates** | State and local government rates | Investment rates for government entities | ✅ Yes | Daily |

**API Endpoint:** `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/`  
**Documentation:** https://fiscaldata.treasury.gov/api-documentation/

---

### Office of Management and Budget (OMB)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Federal Budget Data** | Budget proposals and actuals | Outlays, receipts, deficits by agency/function | ❌ Download | Annual |
| **Circular A-11** | Budget preparation guidance | Agency spending limits, economic assumptions | ❌ Download | Annual |
| **Historical Tables** | Long-term budget history | 50+ years of federal budget data | ❌ Download | Annual |

**Access:** https://www.whitehouse.gov/omb/budget/

---

### Congressional Budget Office (CBO)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Budget and Economic Outlook** | Fiscal projections | 10-year budget projections, economic forecasts | ❌ Download | 2x per year |
| **Long-Term Budget Outlook** | 30-year projections | Debt trajectory, entitlement spending | ❌ Download | Annual |
| **Cost Estimates** | Legislation cost analysis | Bill-by-bill fiscal impact | ❌ Download | As released |

**Access:** https://www.cbo.gov/data/budget-economic-data

---

## Federal Government - Industry & Sector Specific

### Energy Information Administration (EIA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Petroleum Data** | Oil markets | Crude production, refinery utilization, inventories, imports/exports | ✅ Yes (free key) | Weekly |
| **Natural Gas Data** | Gas markets | Production, storage, prices, consumption by sector | ✅ Yes | Weekly/Monthly |
| **Electricity Data** | Power sector | Generation by source, capacity, consumption, prices | ✅ Yes | Monthly |
| **Coal Data** | Coal industry | Production, consumption, stocks, trade | ✅ Yes | Weekly/Monthly |
| **Renewable Energy** | Clean energy | Solar, wind, hydro generation and capacity | ✅ Yes | Monthly |
| **State Energy Data System (SEDS)** | State-level energy | Consumption, prices, expenditures by state | ✅ Yes | Annual |
| **Short-Term Energy Outlook (STEO)** | Energy forecasts | Price and production forecasts | ✅ Yes | Monthly |
| **Annual Energy Outlook (AEO)** | Long-term projections | 30-year energy forecasts | ❌ Download | Annual |

**API Endpoint:** `https://api.eia.gov/v2/`  
**Registration:** https://www.eia.gov/opendata/register.php

#### Key EIA Series for Bottleneck Detection:
- `PET.WCRSTUS1.W` - Weekly crude oil stocks
- `NG.NW2_EPG0_SWO_R48_BCF.W` - Weekly natural gas storage
- `ELEC.GEN.ALL-US-99.M` - Total electricity generation

---

### U.S. Department of Agriculture (USDA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **NASS (National Agricultural Statistics Service)** | Crop and livestock data | Production, yields, prices, acreage, livestock inventory | ✅ Yes (free) | Weekly/Monthly |
| **ERS (Economic Research Service)** | Agricultural economics | Farm income, food prices, trade, rural economy | ✅ Partial | Monthly/Annual |
| **World Agricultural Supply and Demand (WASDE)** | Global crop forecasts | US and world production/consumption estimates | ❌ Download | Monthly |
| **Grain Stocks** | Inventory reports | Quarterly grain and oilseed stocks | ❌ Report | Quarterly |
| **Cattle on Feed** | Livestock inventory | Feedlot placements, marketings | ❌ Report | Monthly |
| **Cold Storage** | Food inventory | Frozen meat, poultry, dairy stocks | ❌ Report | Monthly |
| **FSIS (Food Safety)** | Meat production | Slaughter data, inspection records | ✅ Yes | Daily/Weekly |
| **FAS (Foreign Agricultural Service)** | Agricultural trade | Export sales, attaché reports | ✅ Partial | Weekly |

**API Endpoint (NASS):** `https://quickstats.nass.usda.gov/api`  
**Registration:** https://quickstats.nass.usda.gov/api

---

### Department of Commerce - Census Bureau

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Economic Census** | Comprehensive business survey | Revenue, employment, payroll by industry (NAICS) | ✅ Yes | Every 5 years |
| **Annual Business Survey** | Business characteristics | R&D, innovation, ownership demographics | ✅ Yes | Annual |
| **County Business Patterns** | Local business data | Establishments, employment, payroll by county/industry | ✅ Yes | Annual |
| **Retail Trade Survey** | Retail sales | Monthly and annual retail sales by category | ✅ Yes | Monthly |
| **Wholesale Trade Survey** | Wholesale sales | Sales, inventories by merchant type | ✅ Yes | Monthly |
| **Manufacturing Surveys** | Factory activity | Shipments, orders, inventories (M3) | ✅ Yes | Monthly |
| **Construction Spending** | Building activity | Residential, nonresidential, public construction | ✅ Yes | Monthly |
| **Housing Starts/Permits** | Residential construction | New housing units authorized and started | ✅ Yes | Monthly |
| **New Home Sales** | Housing market | New single-family home sales and prices | ✅ Yes | Monthly |
| **International Trade** | Trade flows | Imports, exports by commodity and country | ✅ Yes | Monthly |
| **Advance Economic Indicators** | Leading indicators | Durable goods, trade, inventories | ✅ Yes | Monthly |
| **Quarterly Services Survey** | Services sector | Revenue for service industries | ✅ Yes | Quarterly |

**API Endpoint:** `https://api.census.gov/data/`  
**Registration:** https://api.census.gov/data/key_signup.html

---

### Department of Commerce - Other

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Bureau of Industry and Security (BIS)** | Export controls | Export licenses, denied parties list | ❌ Download | As needed |
| **International Trade Administration (ITA)** | Trade promotion | Trade leads, market research | ✅ Partial | Ongoing |
| **NOAA Fisheries** | Commercial fishing | Landings, trade, stock assessments | ✅ Yes | Annual |
| **Patent and Trademark Office (USPTO)** | IP data | Patent grants, trademark registrations | ✅ Yes | Weekly |

---

### Department of Labor

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Bureau of Labor Statistics (BLS)** | See detailed section below | Employment, prices, productivity, compensation | ✅ Yes (free key) | Various |
| **Employment and Training Administration (ETA)** | Workforce programs | Unemployment claims, job training | ✅ Partial | Weekly |
| **OSHA** | Workplace safety | Inspection data, violations, injuries | ✅ Yes | Ongoing |
| **Wage and Hour Division** | Labor enforcement | Minimum wage violations, back wages | ❌ Download | Annual |

---

## Federal Government - Labor & Demographics

### Bureau of Labor Statistics (BLS) - Detailed

| Survey/Program | Description | Key Series | API Available | Frequency |
|----------------|-------------|------------|---------------|-----------|
| **Current Employment Statistics (CES)** | Establishment survey | Nonfarm payrolls, hours, earnings by industry | ✅ Yes | Monthly |
| **Current Population Survey (CPS)** | Household survey | Unemployment rate, labor force participation | ✅ Yes | Monthly |
| **Job Openings and Labor Turnover (JOLTS)** | Labor market dynamics | Job openings, hires, separations, quits | ✅ Yes | Monthly |
| **Consumer Price Index (CPI)** | Consumer inflation | CPI-U, CPI-W, core CPI, by category | ✅ Yes | Monthly |
| **Producer Price Index (PPI)** | Wholesale inflation | Final demand, intermediate demand, by commodity | ✅ Yes | Monthly |
| **Import/Export Price Indexes** | Trade prices | Price changes for imports and exports | ✅ Yes | Monthly |
| **Employment Cost Index (ECI)** | Compensation trends | Wages and benefits by occupation/industry | ✅ Yes | Quarterly |
| **Productivity and Costs** | Efficiency metrics | Labor productivity, unit labor costs | ✅ Yes | Quarterly |
| **Occupational Employment Statistics (OES)** | Wage data by job | Employment and wages by occupation | ✅ Yes | Annual |
| **Quarterly Census of Employment and Wages (QCEW)** | Comprehensive employment | Detailed employment by industry/county | ✅ Yes | Quarterly |
| **Consumer Expenditure Survey** | Spending patterns | Household spending by category | ✅ Yes | Annual |
| **American Time Use Survey** | Time allocation | How Americans spend their time | ✅ Yes | Annual |

**API Endpoint:** `https://api.bls.gov/publicAPI/v2/`  
**Registration:** https://data.bls.gov/registrationEngine/

#### Key BLS Series for Bottleneck Detection:
- `CES0000000001` - Total nonfarm employment
- `LNS14000000` - Unemployment rate
- `CUUR0000SA0` - CPI-U (all items)
- `WPUFD4` - PPI Final Demand
- `JTS000000000000000JOR` - Job openings rate

---

### Census Bureau - Demographics

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Decennial Census** | Complete population count | Population, housing, demographics | ✅ Yes | Every 10 years |
| **American Community Survey (ACS)** | Detailed demographics | Income, education, housing, commuting | ✅ Yes | Annual |
| **Population Estimates Program** | Intercensal estimates | Population by age, sex, race, county | ✅ Yes | Annual |
| **Building Permits Survey** | Construction permits | Residential building permits by jurisdiction | ✅ Yes | Monthly |

---

## Federal Government - Trade & International

### U.S. International Trade Commission (USITC)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **DataWeb** | Trade statistics | Imports/exports by HTS code, country, district | ✅ Yes | Monthly |
| **Tariff Database** | Tariff schedules | MFN rates, FTA rates, special programs | ✅ Yes | As updated |
| **Trade Remedy Data** | Antidumping/CVD | Active orders, investigations, sunset reviews | ❌ Download | Ongoing |

**Access:** https://dataweb.usitc.gov/

---

### Customs and Border Protection (CBP)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Trade Statistics** | Customs data | Entry summaries, duties collected | ❌ Limited | Monthly |
| **Port Data** | Port of entry info | Trade by port, processing times | ❌ Download | Monthly |
| **ACE Reports** | Automated commercial | Trade compliance data | ❌ Restricted | Ongoing |

---

### Office of the U.S. Trade Representative (USTR)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Trade Agreements** | Treaty texts | FTA provisions, tariff schedules | ❌ Download | As signed |
| **Trade Policy Reports** | Annual report | Trade barriers, enforcement actions | ❌ Download | Annual |

---

## Federal Government - Regulatory & Filings

### Securities and Exchange Commission (SEC)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **EDGAR** | Company filings | 10-K, 10-Q, 8-K, proxy statements, prospectuses | ✅ Yes (free) | Real-time |
| **Financial Statement Data Sets** | Structured financials | Balance sheets, income statements (XBRL) | ✅ Yes | Quarterly |
| **Mutual Fund Data** | Fund holdings | N-PORT, N-CEN filings | ✅ Yes | Monthly/Annual |
| **Insider Trading** | Form 3, 4, 5 | Officer/director stock transactions | ✅ Yes | As filed |
| **13F Holdings** | Institutional holdings | Quarterly portfolio disclosures | ✅ Yes | Quarterly |
| **Short Interest** | Short selling | Regulation SHO threshold securities | ❌ Via exchanges | Bi-weekly |

**API Endpoint:** `https://data.sec.gov/`  
**Documentation:** https://www.sec.gov/developer

---

### Federal Deposit Insurance Corporation (FDIC)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **BankFind Suite** | Bank information | Institution details, branch locations | ✅ Yes | Quarterly |
| **Call Reports** | Bank financials | Balance sheets, income, risk metrics | ✅ Yes | Quarterly |
| **Summary of Deposits** | Deposit data | Deposits by branch | ✅ Yes | Annual |
| **Failed Bank List** | Bank failures | Failed institutions, acquirers | ✅ Yes | As events |

**API Endpoint:** `https://banks.data.fdic.gov/api/`

---

### Federal Reserve - Regulatory

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Bank Holding Company Data** | BHC reports | FR Y-9C, Y-9LP regulatory filings | ✅ Partial | Quarterly |
| **Stress Test Results (CCAR/DFAST)** | Bank stress tests | Capital projections under scenarios | ❌ Download | Annual |
| **Senior Loan Officer Survey** | Credit conditions | Lending standards, demand | ❌ Download | Quarterly |

---

### Other Regulatory Agencies

| Agency | Data Available | API | Access |
|--------|---------------|-----|--------|
| **CFTC** | Commitments of Traders (COT), swap data | ✅ Yes | https://www.cftc.gov/MarketReports/ |
| **FHFA** | House Price Index, GSE data | ✅ Yes | https://www.fhfa.gov/DataTools |
| **CFPB** | Consumer complaint database | ✅ Yes | https://www.consumerfinance.gov/data-research/ |
| **FTC** | Antitrust, consumer protection | ❌ Limited | https://www.ftc.gov/policy/reports |
| **FCC** | Telecommunications data | ✅ Yes | https://www.fcc.gov/reports-research/data |
| **FERC** | Energy regulatory filings | ✅ Partial | https://www.ferc.gov/industries-data |
| **NRC** | Nuclear plant operations | ✅ Yes | https://www.nrc.gov/reading-rm/doc-collections/ |
| **FAA** | Aviation safety, traffic | ✅ Yes | https://www.faa.gov/data_research |

---

## Federal Government - Infrastructure & Transportation

### Department of Transportation (DOT)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Bureau of Transportation Statistics (BTS)** | Transportation data | Freight, passenger travel, on-time performance | ✅ Yes | Monthly |
| **FMCSA** | Trucking data | Carrier safety, inspections, crashes | ✅ Yes | Ongoing |
| **NHTSA** | Vehicle safety | Recalls, complaints, crash data | ✅ Yes | Ongoing |
| **FRA** | Railroad data | Rail traffic, accidents, inspections | ✅ Partial | Monthly |
| **FAA** | Aviation data | Traffic, delays, safety | ✅ Yes | Real-time/Monthly |
| **MARAD** | Maritime data | US-flag fleet, port statistics, shipbuilding | ❌ Download | Annual |
| **FHWA** | Highway data | Traffic volumes, road conditions | ✅ Partial | Annual |
| **Freight Analysis Framework** | Freight flows | Commodity flows by mode, origin-destination | ✅ Yes | Annual |

**API Access:** https://www.bts.gov/browse-statistical-products-and-data

#### Key Transportation Series for Bottleneck Detection:
- Freight Transportation Services Index (TSI)
- Border crossing data
- Container traffic at major ports
- Rail carloading data

---

### Army Corps of Engineers

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Lock Performance Monitoring** | Waterway traffic | Barge traffic, lock delays | ✅ Partial | Daily |
| **Waterborne Commerce Statistics** | Inland shipping | Tonnage by commodity, waterway | ❌ Download | Annual |
| **Navigation Data Center** | Port statistics | Vessel calls, cargo tonnage | ✅ Partial | Monthly |

---

### Federal Maritime Commission (FMC)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **OSRA Data** | Ocean shipping | Container dwell times, export volumes | ❌ Download | Quarterly |
| **Service Contracts** | Shipping rates | Filed service contract data | ❌ Restricted | Ongoing |

---

## Federal Government - Natural Resources & Environment

### U.S. Geological Survey (USGS)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Water Data** | Water resources | Streamflow, groundwater, water quality | ✅ Yes | Real-time |
| **Mineral Commodity Summaries** | Mining/minerals | Production, trade, prices for 90+ minerals | ❌ Download | Annual |
| **Earthquake Data** | Seismic activity | Real-time earthquake information | ✅ Yes | Real-time |
| **Land Cover** | Land use data | National Land Cover Database | ✅ Yes | Multi-year |

**API Endpoint:** https://waterservices.usgs.gov/

---

### Environmental Protection Agency (EPA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Air Quality System (AQS)** | Air quality | Pollutant concentrations by location | ✅ Yes | Hourly |
| **Toxics Release Inventory (TRI)** | Industrial pollution | Chemical releases by facility | ✅ Yes | Annual |
| **Enforcement & Compliance (ECHO)** | Environmental compliance | Inspections, violations, penalties | ✅ Yes | Ongoing |
| **Greenhouse Gas Reporting** | GHG emissions | Facility-level CO2 emissions | ✅ Yes | Annual |
| **Safe Drinking Water** | Water quality | Drinking water violations | ✅ Yes | Quarterly |

**API Access:** https://www.epa.gov/enviro/envirofacts-data-service-api

---

### National Oceanic and Atmospheric Administration (NOAA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Climate Data Online** | Weather/climate | Temperature, precipitation, storms | ✅ Yes | Daily |
| **National Weather Service** | Forecasts | Weather forecasts, warnings | ✅ Yes | Real-time |
| **Fisheries Statistics** | Commercial fishing | Landings, trade, stock status | ✅ Yes | Annual |
| **Coastal Economics** | Coastal economy | Tourism, recreation, blue economy | ❌ Download | Annual |
| **Storm Events Database** | Severe weather | Damage, casualties by event | ✅ Yes | Monthly |

**API Endpoint:** https://www.ncdc.noaa.gov/cdo-web/webservices/v2

---

### Department of the Interior

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Bureau of Land Management** | Federal lands | Lease sales, production on federal lands | ❌ Download | Monthly |
| **Office of Natural Resources Revenue (ONRR)** | Royalties | Oil, gas, mineral royalties from federal lands | ✅ Yes | Monthly |
| **Bureau of Reclamation** | Water projects | Reservoir levels, water deliveries | ✅ Partial | Daily/Monthly |
| **Fish and Wildlife Service** | Wildlife | Endangered species, hunting/fishing licenses | ❌ Download | Annual |

---

## Federal Government - Health & Social

### Department of Health and Human Services (HHS)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **CDC** | Disease surveillance | Disease incidence, mortality, vaccinations | ✅ Yes | Weekly |
| **CMS (Medicare/Medicaid)** | Healthcare spending | Enrollment, spending, provider data | ✅ Yes | Quarterly |
| **FDA** | Drug/device approvals | Approvals, recalls, inspections | ✅ Yes | Real-time |
| **NIH** | Research data | Clinical trials, research grants | ✅ Yes | Ongoing |
| **SAMHSA** | Substance abuse | Treatment data, surveys | ❌ Download | Annual |

---

### Social Security Administration (SSA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **OASDI (Social Security)** | Benefit data | Beneficiaries, payments, trust funds | ❌ Download | Annual |
| **SSI (Supplemental Security Income)** | SSI data | Recipients, payments | ❌ Download | Annual |
| **Wage Statistics** | Wage data | Average wages, wage distribution | ❌ Download | Annual |

---

### Department of Housing and Urban Development (HUD)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Fair Market Rents** | Rental data | FMR by metro/county | ✅ Yes | Annual |
| **Income Limits** | Housing eligibility | AMI limits by area | ✅ Yes | Annual |
| **USPS Vacancy Data** | Housing vacancy | Vacant addresses by tract | ✅ Yes | Quarterly |
| **Picture of Subsidized Households** | Assisted housing | HUD-assisted units, demographics | ❌ Download | Annual |
| **Affirmatively Furthering Fair Housing** | Fair housing | Segregation indices, opportunity maps | ✅ Yes | Annual |

---

## Federal Government - Additional Agencies

### Department of Education

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **NCES (National Center for Education Statistics)** | Education data | School enrollment, graduation rates, test scores, expenditures | ✅ Yes | Annual |
| **College Scorecard** | Higher education outcomes | Graduation rates, earnings, debt by institution | ✅ Yes | Annual |
| **IPEDS (Integrated Postsecondary Education Data System)** | College/university data | Enrollment, completions, finance, faculty | ✅ Yes | Annual |
| **NAEP (Nation's Report Card)** | Student achievement | Reading, math scores by state/demographics | ✅ Yes | Bi-annual |
| **Federal Student Aid Data** | Student loans | Loan volumes, default rates, Pell grants | ❌ Download | Annual |

**API Access:** https://nces.ed.gov/datalab/

---

### Department of Defense (DoD)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **USASpending (DoD)** | Defense contracts | Contract awards, vendors, amounts | ✅ Yes | Daily |
| **FPDS (Federal Procurement Data System)** | All federal contracts | Contract details, competition, small business | ✅ Yes | Daily |
| **DMDC (Defense Manpower Data Center)** | Military personnel | Active duty, reserves, civilians by location | ❌ Download | Quarterly |
| **Defense Logistics Agency** | Supply chain | Inventory levels, procurement | ❌ Limited | Varies |
| **DISA (Defense Information Systems)** | IT infrastructure | Cybersecurity, network data | ❌ Restricted | N/A |

**API Access (Procurement):** https://api.sam.gov/

---

### Small Business Administration (SBA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **7(a) Loan Data** | SBA guaranteed loans | Loan amounts, lenders, industries, locations | ✅ Yes | Monthly |
| **504 Loan Data** | Real estate/equipment loans | Fixed asset financing data | ✅ Yes | Monthly |
| **Disaster Loan Data** | Disaster recovery | Loan approvals by disaster, location | ✅ Yes | Ongoing |
| **PPP Loan Data** | Pandemic relief (historical) | PPP loans, forgiveness by business | ✅ Yes | Archived |
| **EIDL Data** | Economic injury loans | COVID EIDL disbursements | ✅ Yes | Archived |
| **Size Standards** | Small business definitions | Revenue/employee thresholds by NAICS | ❌ Download | Annual |

**API Access:** https://data.sba.gov/

---

### Department of Justice (DOJ)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Bureau of Justice Statistics (BJS)** | Crime/justice data | Crime rates, incarceration, law enforcement | ✅ Partial | Annual |
| **FBI Uniform Crime Reports (UCR)** | Crime statistics | Violent crime, property crime by jurisdiction | ✅ Yes | Annual |
| **National Incident-Based Reporting (NIBRS)** | Detailed crime data | Offense, victim, offender details | ✅ Yes | Annual |
| **Federal Prison Statistics** | Incarceration | Federal inmate population, demographics | ❌ Download | Monthly |
| **DEA Drug Data** | Drug enforcement | Seizures, arrests, trafficking | ❌ Download | Annual |
| **ATF Firearms Data** | Gun statistics | Traces, manufacturing, exports | ❌ Download | Annual |

**API Access:** https://crime-data-explorer.fr.cloud.gov/pages/docApi

---

### National Science Foundation (NSF)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **NCSES (National Center for Science and Engineering Statistics)** | R&D data | R&D spending by sector, industry, funder | ✅ Partial | Annual |
| **Science & Engineering Indicators** | Innovation metrics | Patents, STEM workforce, publications | ❌ Download | Bi-annual |
| **Survey of Business R&D** | Corporate R&D | R&D expenditure by industry, company size | ✅ Partial | Annual |
| **Survey of Federal Funds for R&D** | Government R&D | Federal R&D obligations by agency | ❌ Download | Annual |
| **HERD (Higher Education R&D)** | University R&D | R&D expenditure by institution, field | ✅ Partial | Annual |

**Access:** https://ncses.nsf.gov/explore-data

---

### Federal Emergency Management Agency (FEMA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Disaster Declarations** | Declared disasters | Disaster type, location, dates, programs | ✅ Yes | Real-time |
| **Individual Assistance Data** | Household assistance | IA grants, applicants by disaster | ✅ Yes | Ongoing |
| **Public Assistance Data** | Infrastructure assistance | PA projects, costs by disaster | ✅ Yes | Ongoing |
| **National Flood Insurance Program** | Flood insurance | Policies, claims, flood maps | ✅ Partial | Monthly |
| **Hazard Mitigation Grants** | Mitigation projects | Project details, funding | ✅ Yes | Ongoing |
| **NFIP Claims** | Flood claims | Historical flood insurance claims | ✅ Yes | Monthly |

**API Access:** https://www.fema.gov/about/openfema/api

---

### General Services Administration (GSA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **SAM.gov (System for Award Management)** | Federal vendors | Registered entities, exclusions | ✅ Yes | Real-time |
| **GSA Advantage** | Federal purchasing | Product/service pricing, contracts | ✅ Yes | Real-time |
| **Federal Real Property** | Government buildings | Building inventory, utilization, costs | ✅ Partial | Annual |
| **Fleet Data** | Federal vehicles | Vehicle inventory, fuel use, costs | ❌ Download | Annual |
| **Per Diem Rates** | Travel rates | Lodging/meal rates by location | ✅ Yes | Annual |

**API Access:** https://open.gsa.gov/api/

---

### Office of Personnel Management (OPM)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **FedScope** | Federal workforce | Employment, demographics, by agency/location | ✅ Yes | Quarterly |
| **Federal Employee Viewpoint Survey** | Employee satisfaction | Engagement, satisfaction by agency | ❌ Download | Annual |
| **Retirement Statistics** | Federal retirement | Retirements, annuities | ❌ Download | Annual |
| **Pay & Leave** | Compensation | Pay scales, leave policies | ❌ Download | Annual |

**API Access:** https://www.opm.gov/data/

---

### Export-Import Bank (EXIM)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Authorization Data** | Export financing | Loan guarantees, insurance by country/sector | ✅ Yes | Quarterly |
| **Transaction Data** | Individual deals | Exporters, buyers, amounts | ❌ Download | Quarterly |
| **Default/Claims Data** | Credit losses | Defaults, claims paid | ❌ Download | Annual |

**Access:** https://data.exim.gov/

---

### National Credit Union Administration (NCUA)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Call Report Data** | Credit union financials | Balance sheets, income, loans | ✅ Yes | Quarterly |
| **Credit Union Locator** | Institution info | CU details, branches | ✅ Yes | Quarterly |
| **Aggregate Statistics** | Industry data | Total assets, members, loans | ❌ Download | Quarterly |

**API Access:** https://ncua.gov/analysis/credit-union-corporate-call-report-data

---

### Pension Benefit Guaranty Corporation (PBGC)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Premium Filers Data** | Pension plans | Single/multiemployer plan statistics | ❌ Download | Annual |
| **Financial Statements** | PBGC finances | Assets, liabilities, claims | ❌ Download | Annual |
| **Multiemployer Program** | At-risk plans | Plans in critical/declining status | ❌ Download | Annual |

**Access:** https://www.pbgc.gov/prac/data-books

---

### Department of State

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Visa Statistics** | Immigration | Visa issuances by category, country | ❌ Download | Annual |
| **Passport Statistics** | Travel documents | Passports issued | ❌ Download | Annual |
| **Travel Advisories** | Safety data | Country risk levels | ❌ Web/RSS | Real-time |
| **Foreign Relations** | Diplomatic data | Treaties, agreements | ❌ Download | Ongoing |

**Access:** https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics.html

---

### Government Accountability Office (GAO)

| Source | Description | Data Types | API Available | Update Frequency |
|--------|-------------|------------|---------------|------------------|
| **Reports & Recommendations** | Audit findings | Program evaluations, cost savings | ✅ Partial | Ongoing |
| **High Risk List** | At-risk programs | Programs vulnerable to waste/fraud | ❌ Download | Bi-annual |
| **Cost Savings Database** | Financial impact | Documented savings from GAO work | ❌ Download | Annual |
| **Bid Protest Decisions** | Contract disputes | Protest outcomes | ✅ Yes | Real-time |

**Access:** https://www.gao.gov/reports-testimonies

---

### U.S. Territories

| Territory | Data Portal | Notable Data |
|-----------|-------------|--------------|
| **Puerto Rico** | https://estadisticas.pr/ | Economic indicators, demographics, trade |
| **U.S. Virgin Islands** | https://www.usviber.org/ | Tourism, economic data, labor |
| **Guam** | https://www.guamtransparency.com/ | Budget, expenditures, demographics |
| **American Samoa** | https://www.doc.as/ | Trade, demographics |
| **Northern Mariana Islands** | https://gov.mp/transparency/ | Budget, workforce |

---

## State Government Data Portals

### Major State Open Data Portals

| State | Portal URL | Notable Datasets |
|-------|------------|------------------|
| **California** | https://data.ca.gov | Employment, housing, environment, transportation |
| **Texas** | https://data.texas.gov | Energy, education, public safety, health |
| **Florida** | https://open.data.fl.gov | Tourism, labor, environment, demographics |
| **New York** | https://data.ny.gov | Finance, health, transportation, education |
| **Illinois** | https://data.illinois.gov | Labor, commerce, health, public safety |
| **Pennsylvania** | https://data.pa.gov | Labor, education, health, environment |
| **Ohio** | https://data.ohio.gov | Education, health, public safety |
| **Georgia** | https://data.georgia.gov | Labor, education, health |
| **North Carolina** | https://www.ncopendata.com | Commerce, education, health |
| **Michigan** | https://data.michigan.gov | Labor, education, environment |
| **New Jersey** | https://data.nj.gov | Labor, health, transportation |
| **Virginia** | https://data.virginia.gov | Education, health, public safety |
| **Washington** | https://data.wa.gov | Labor, education, environment |
| **Arizona** | https://opendata.az.gov | Education, public safety, health |
| **Massachusetts** | https://data.mass.gov | Health, education, transportation |
| **Tennessee** | https://www.tn.gov/transparenttn/open-data | Education, health, labor |
| **Indiana** | https://data.in.gov | Education, health, public safety |
| **Missouri** | https://data.mo.gov | Education, health, transportation |
| **Maryland** | https://data.maryland.gov | Education, health, transportation |
| **Wisconsin** | https://data.wi.gov | Education, health, labor |
| **Colorado** | https://data.colorado.gov | Education, health, environment |
| **Minnesota** | https://mn.gov/portal/data | Education, health, labor |
| **South Carolina** | https://data.sc.gov | Education, health, public safety |
| **Alabama** | https://open.alabama.gov | Education, health, public safety |
| **Louisiana** | https://data.louisiana.gov | Health, education, public safety |
| **Kentucky** | https://data.ky.gov | Education, health, public safety |
| **Oregon** | https://data.oregon.gov | Education, health, environment |
| **Oklahoma** | https://data.ok.gov | Education, health, energy |
| **Connecticut** | https://data.ct.gov | Education, health, transportation |
| **Utah** | https://opendata.utah.gov | Education, health, environment |
| **Iowa** | https://data.iowa.gov | Education, agriculture, health |
| **Nevada** | https://data.nv.gov | Gaming, tourism, education |
| **Arkansas** | https://data.arkansas.gov | Education, health, agriculture |
| **Mississippi** | https://data.ms.gov | Education, health |
| **Kansas** | https://data.kansas.gov | Education, agriculture, health |
| **New Mexico** | https://data.newmexico.gov | Energy, education, health |
| **Nebraska** | https://data.nebraska.gov | Agriculture, education, health |
| **West Virginia** | https://data.wv.gov | Education, health, energy |
| **Idaho** | https://data.idaho.gov | Agriculture, education, health |
| **Hawaii** | https://data.hawaii.gov | Tourism, education, health |
| **New Hampshire** | https://data.nh.gov | Education, health |
| **Maine** | https://data.maine.gov | Education, health, environment |
| **Montana** | https://data.mt.gov | Agriculture, education, health |
| **Rhode Island** | https://data.ri.gov | Education, health |
| **Delaware** | https://data.delaware.gov | Education, health, finance |
| **South Dakota** | https://data.sd.gov | Agriculture, education |
| **North Dakota** | https://data.nd.gov | Agriculture, energy, education |
| **Alaska** | https://data.alaska.gov | Energy, fisheries, education |
| **Vermont** | https://data.vermont.gov | Education, health, agriculture |
| **Wyoming** | https://data.wyo.gov | Energy, agriculture, education |

---

### State Labor Market Information (LMI)

All states provide labor market data through their Labor/Workforce departments:

| Data Type | Available From | Frequency |
|-----------|---------------|-----------|
| Unemployment claims | State workforce agencies | Weekly |
| Employment by industry | State LMI offices | Monthly |
| Wage data | State labor departments | Quarterly |
| Job openings | State job banks | Real-time |
| Labor force projections | State LMI offices | Annual |

---

### State Tax/Revenue & Comptroller Data - All 50 States

| State | Comptroller/Treasurer Portal | Tax/Revenue Portal | Key Data Available |
|-------|------------------------------|-------------------|-------------------|
| **Alabama** | https://comptroller.alabama.gov/ | https://revenue.alabama.gov/transparency/ | Sales tax, property tax, budget |
| **Alaska** | https://dor.alaska.gov/Treasury/ | https://tax.alaska.gov/programs/reports.aspx | Oil revenue, PFD, no income/sales tax |
| **Arizona** | https://aztreasury.gov/ | https://azdor.gov/reports-statistics | Transaction privilege tax, income tax |
| **Arkansas** | https://www.dfa.arkansas.gov/ | https://www.dfa.arkansas.gov/revenue-policy-and-legal | Sales tax, income tax |
| **California** | https://sco.ca.gov/ | https://www.ftb.ca.gov/about-ftb/open-data/ | Income tax, sales tax, budget |
| **Colorado** | https://treasury.colorado.gov/ | https://tax.colorado.gov/data | Sales tax, income tax, marijuana tax |
| **Connecticut** | https://portal.ct.gov/OTT | https://portal.ct.gov/DRS/Research/Research-Reports | Income tax, sales tax |
| **Delaware** | https://accounting.delaware.gov/ | https://finance.delaware.gov/tax-forms/report | Gross receipts, franchise tax |
| **Florida** | https://myfloridacfo.com/ | https://floridarevenue.com/opendata/ | Sales tax, documentary stamp |
| **Georgia** | https://sao.georgia.gov/ | https://dor.georgia.gov/georgia-tax-data | Income tax, sales tax |
| **Hawaii** | https://budget.hawaii.gov/ | https://tax.hawaii.gov/stats/ | General excise tax, income tax |
| **Idaho** | https://sto.idaho.gov/ | https://tax.idaho.gov/reports/ | Sales tax, income tax |
| **Illinois** | https://illinoiscomptroller.gov/ | https://tax.illinois.gov/research.html | Income tax, sales tax, property tax |
| **Indiana** | https://www.in.gov/tos/ | https://www.in.gov/dor/tax-forms/statistics-and-data/ | Sales tax, income tax |
| **Iowa** | https://treasurer.iowa.gov/ | https://tax.iowa.gov/statistics | Sales tax, income tax |
| **Kansas** | https://treasurer.ks.gov/ | https://ksrevenue.gov/research.html | Sales tax, income tax |
| **Kentucky** | https://treasury.ky.gov/ | https://revenue.ky.gov/Research/Pages/default.aspx | Sales tax, income tax |
| **Louisiana** | https://treasury.la.gov/ | https://revenue.louisiana.gov/SalesUseTax/SalesUseTaxData | Sales tax, income tax, severance |
| **Maine** | https://www.maine.gov/treasurer/ | https://www.maine.gov/revenue/research-stats | Sales tax, income tax |
| **Maryland** | https://treasurer.state.md.us/ | https://www.marylandtaxes.gov/reports/ | Income tax, sales tax |
| **Massachusetts** | https://www.masstreasury.org/ | https://www.mass.gov/tax-revenue-data | Income tax, sales tax |
| **Michigan** | https://michigan.gov/treasury/ | https://www.michigan.gov/taxes/tax-data | Income tax, sales tax |
| **Minnesota** | https://mn.gov/mmb/ | https://www.revenue.state.mn.us/statistics-data | Income tax, sales tax |
| **Mississippi** | https://treasury.ms.gov/ | https://www.dor.ms.gov/statistics | Sales tax, income tax |
| **Missouri** | https://treasurer.mo.gov/ | https://dor.mo.gov/taxation/business/tax-types/sales-use/statistics.html | Sales tax, income tax |
| **Montana** | https://mtafc.gov/ | https://mtrevenue.gov/publications/biennial-reports/ | Income tax, no sales tax |
| **Nebraska** | https://treasurer.nebraska.gov/ | https://revenue.nebraska.gov/tax-research | Sales tax, income tax |
| **Nevada** | https://www.nevadatreasurer.gov/ | https://tax.nv.gov/Publications/Reports/ | Gaming tax, sales tax, no income tax |
| **New Hampshire** | https://www.nh.gov/treasury/ | https://www.revenue.nh.gov/research-statistics | Business profits, no sales/income tax |
| **New Jersey** | https://www.nj.gov/treasury/ | https://www.state.nj.us/treasury/taxation/statistics.shtml | Income tax, sales tax |
| **New Mexico** | https://www.nm.gov/state-treasurer/ | https://www.tax.newmexico.gov/reports-statistics/ | Gross receipts, income tax |
| **New York** | https://www.osc.state.ny.us/ | https://www.tax.ny.gov/research/stats/ | Income tax, sales tax |
| **North Carolina** | https://www.nctreasurer.com/ | https://www.ncdor.gov/taxes-forms/research-and-statistics | Income tax, sales tax |
| **North Dakota** | https://www.nd.gov/treasurer/ | https://www.nd.gov/tax/user/businesses/statistics | Oil tax, sales tax |
| **Ohio** | https://www.ohiotreasurer.gov/ | https://tax.ohio.gov/government/statistics | Sales tax, income tax |
| **Oklahoma** | https://treasurer.ok.gov/ | https://oklahoma.gov/tax/resources/reports-and-statistics.html | Sales tax, income tax, oil/gas |
| **Oregon** | https://www.oregon.gov/treasury/ | https://www.oregon.gov/dor/stats/pages/default.aspx | Income tax, no sales tax |
| **Pennsylvania** | https://www.patreasury.gov/ | https://www.revenue.pa.gov/GeneralTaxInformation/News%20and%20Statistics/ | Income tax, sales tax |
| **Rhode Island** | https://treasury.ri.gov/ | https://tax.ri.gov/about-us/research-and-data | Income tax, sales tax |
| **South Carolina** | https://treasurer.sc.gov/ | https://dor.sc.gov/statistics | Income tax, sales tax |
| **South Dakota** | https://sdtreasurer.gov/ | https://dor.sd.gov/reports-and-information/ | Sales tax, no income tax |
| **Tennessee** | https://treasury.tn.gov/ | https://www.tn.gov/revenue/tax-resources/statistics.html | Sales tax, Hall tax (repealed) |
| **Texas** | https://comptroller.texas.gov/ | https://comptroller.texas.gov/transparency/ | Sales tax, franchise tax, no income tax |
| **Utah** | https://treasurer.utah.gov/ | https://tax.utah.gov/research | Income tax, sales tax |
| **Vermont** | https://www.vermonttreasurer.gov/ | https://tax.vermont.gov/reports | Income tax, sales tax |
| **Virginia** | https://www.trs.virginia.gov/ | https://www.tax.virginia.gov/annual-reports | Income tax, sales tax |
| **Washington** | https://tre.wa.gov/ | https://dor.wa.gov/taxes-rates/tax-statistics | Sales tax, B&O tax, no income tax |
| **West Virginia** | https://wvtreasury.com/ | https://tax.wv.gov/Research/Pages/default.aspx | Income tax, sales tax, severance |
| **Wisconsin** | https://statetreasurer.wi.gov/ | https://www.revenue.wi.gov/Pages/RA/home.aspx | Income tax, sales tax |
| **Wyoming** | https://statetreasurer.wyo.gov/ | https://wyor.gov/tr/Data/data.htm | Sales tax, mineral severance, no income tax |

#### Key State-Level Economic Indicators

| Data Type | Best Source | Coverage | Frequency |
|-----------|-------------|----------|-----------|
| **State GDP** | BEA (federal) | All states | Quarterly |
| **State Employment** | BLS QCEW (federal) | All states, counties | Quarterly |
| **Sales Tax Collections** | State comptrollers | All sales tax states | Monthly |
| **Property Tax** | State/county assessors | All states | Annual |
| **Income Tax Collections** | State revenue depts | 43 states with income tax | Monthly/Quarterly |
| **State Budget/Expenditures** | State comptrollers | All states | Annual |
| **Pension Fund Performance** | State treasurers | All states | Quarterly |
| **Outstanding Debt** | State treasurers | All states | Quarterly |

---

## International Government Sources

### Multilateral Organizations

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **IMF** | International financial data | GDP, inflation, trade, reserves by country | ✅ Yes | https://data.imf.org |
| **World Bank** | Development data | 1,600+ indicators for 200+ countries | ✅ Yes | https://data.worldbank.org |
| **OECD** | Economic data | GDP, employment, trade, education | ✅ Yes | https://data.oecd.org |
| **UN Data** | Demographic/economic | Population, trade, environment | ✅ Yes | https://data.un.org |
| **WTO** | Trade statistics | Trade flows, tariffs | ✅ Yes | https://data.wto.org |
| **BIS** | Banking statistics | Cross-border banking, FX, credit | ✅ Yes | https://www.bis.org/statistics/ |
| **ILO** | Labor data | Employment, wages globally | ✅ Yes | https://ilostat.ilo.org |

---

### Key Foreign Government Sources

| Country | Agency | Data Types | API | Access |
|---------|--------|------------|-----|--------|
| **Canada** | Statistics Canada | GDP, CPI, employment | ✅ Yes | https://www.statcan.gc.ca |
| **UK** | ONS | GDP, inflation, trade | ✅ Yes | https://www.ons.gov.uk |
| **Germany** | Destatis | Industrial production, trade | ✅ Yes | https://www-genesis.destatis.de |
| **Japan** | e-Stat | GDP, trade, industrial data | ✅ Yes | https://www.e-stat.go.jp |
| **China** | NBS | GDP, industrial output, trade | ❌ Limited | http://www.stats.gov.cn |
| **EU** | Eurostat | EU-wide economic data | ✅ Yes | https://ec.europa.eu/eurostat |
| **Australia** | ABS | GDP, employment, trade | ✅ Yes | https://www.abs.gov.au |
| **Mexico** | INEGI | GDP, inflation, trade | ✅ Yes | https://www.inegi.org.mx |

---

## Data Aggregation Portals

### Federal Aggregators

| Portal | Description | Scope |
|--------|-------------|-------|
| **Data.gov** | Federal open data portal | 300,000+ datasets from all agencies |
| **USASpending.gov** | Federal spending | Contracts, grants, loans |
| **Performance.gov** | Agency performance | KPIs, strategic plans |
| **HealthData.gov** | Health datasets | HHS and related health data |
| **Regulations.gov** | Regulatory dockets | Proposed rules, comments |

**Access:** https://data.gov

---

### Academic & Research

| Source | Description | Access |
|--------|-------------|--------|
| **ICPSR** | Social science data archive | https://www.icpsr.umich.edu |
| **NBER** | Economic research data | https://www.nber.org/research/data |
| **Federal Reserve Banks** | Regional economic data | Various regional Fed sites |

---

## Free Alternative & Supplementary Data Sources

> **Purpose:** These are high-quality, free (or free-tier) non-government data sources that complement official government statistics. They provide unique signals for bottleneck detection that aren't available from traditional sources.

---

### Search & Demand Trends

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **Google Trends** | Search interest over time | Search volume indices by topic, region, time | ✅ Yes (unofficial pytrends) | https://trends.google.com |

**Why It Matters for Bottleneck Detection:**
- Leading indicator of consumer demand shifts
- Real-time signal (no publication lag)
- Geographic granularity (state, metro, country)
- Can track product shortages, price sensitivity, brand sentiment

**Key Use Cases:**
- Rising searches for "car prices" → demand signal for autos
- Spike in "out of stock" + product name → supply shortage
- Geographic search patterns → regional demand variations

**API Access:** Use `pytrends` Python library (unofficial but reliable)
```
pip install pytrends
```

---

### News & Event Sentiment

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **GDELT Project (Global Database of Events, Language, and Tone)** | Real-time monitoring of world's news media | News events, sentiment, themes, people, organizations, locations | ✅ Yes (BigQuery, API) | https://www.gdeltproject.org |

**Why It Matters for Bottleneck Detection:**
- Monitors 100+ languages, translates to English
- Real-time event detection (protests, disasters, policy changes)
- Sentiment analysis at scale
- Geographic coverage of supply chain regions

**Key Datasets:**
| Dataset | Description | Update Frequency |
|---------|-------------|------------------|
| **GDELT Event Database** | Coded events (protests, conflicts, statements) | Every 15 minutes |
| **GDELT Global Knowledge Graph (GKG)** | Themes, emotions, entities from news | Every 15 minutes |
| **GDELT DOC 2.0** | Full document analysis | Every 15 minutes |

**Access Methods:**
- Google BigQuery (free tier: 1TB/month query)
- Direct file downloads
- API endpoints

**API Access:** https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/

---

### Consumer Confidence & Sentiment

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **University of Michigan Consumer Sentiment Index** | Monthly consumer confidence survey | Consumer sentiment, expectations, current conditions | ✅ Via FRED | https://fred.stlouisfed.org/series/UMCSENT |
| **Conference Board Consumer Confidence Index** | Monthly consumer confidence | Consumer confidence, present situation, expectations | ✅ Via FRED | https://fred.stlouisfed.org/series/CSCICP03USM665S |

**Why They Matter for Bottleneck Detection:**
- Leading indicators of consumer spending
- Divergence between "present" and "expectations" signals turning points
- Long historical series for backtesting

**FRED Series IDs:**
| Indicator | FRED Series | Frequency |
|-----------|-------------|-----------|
| UMich Consumer Sentiment | `UMCSENT` | Monthly |
| UMich Current Conditions | `UMCSENT1` | Monthly |
| UMich Consumer Expectations | `UMCSENT5` | Monthly |
| Conference Board Consumer Confidence | `CSCICP03USM665S` | Monthly |

---

### Consumer Credit & Spending

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **Federal Reserve G.19 Consumer Credit** | Consumer credit outstanding | Revolving credit (credit cards), non-revolving (auto, student loans) | ✅ Via FRED | https://www.federalreserve.gov/releases/g19/current/ |

**Why It Matters for Bottleneck Detection:**
- Credit card balances signal consumer financial stress
- Auto loan volumes signal vehicle demand
- Student loan trends affect housing, consumption

**FRED Series IDs:**
| Indicator | FRED Series | Frequency |
|-----------|-------------|-----------|
| Total Consumer Credit | `TOTALSL` | Monthly |
| Revolving Credit (Credit Cards) | `REVOLSL` | Monthly |
| Non-Revolving Credit (Auto, Student) | `NONREVSL` | Monthly |
| Consumer Credit % Change | `TOTALSL_PC1` | Monthly |

---

### Trade & Shipping

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **USA Trade Online (Census Bureau)** | Official U.S. trade statistics | Imports, exports by commodity, country, port, district | ✅ Yes | https://usatrade.census.gov/ |
| **UN Comtrade** | Global trade database | Bilateral trade flows for 200+ countries | ✅ Yes (free tier) | https://comtradeplus.un.org/ |
| **Baltic Dry Index (via FRED)** | Dry bulk shipping rates | Shipping rate index for raw materials | ✅ Via FRED | https://fred.stlouisfed.org/series/BDIY |

**Why They Matter for Bottleneck Detection:**
- USA Trade Online: Official customs data, detailed commodity breakdown
- UN Comtrade: Global trade flows, identify supplier country dependencies
- Baltic Dry Index: Real-time shipping demand/capacity signal

**Key Series:**
| Indicator | Source | FRED Series | Frequency |
|-----------|--------|-------------|-----------|
| Baltic Dry Index | Baltic Exchange | `BDIY` | Daily |
| Baltic Capesize Index | Baltic Exchange | `BCIY` | Daily |
| Baltic Panamax Index | Baltic Exchange | `BPIY` | Daily |

**UN Comtrade API:** https://comtradeplus.un.org/TradeFlow (free tier: 10,000 records/request, 100 requests/day)

---

### Regulatory & Compliance Signals

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **EPA Enforcement & Compliance History (ECHO)** | Environmental enforcement | Inspections, violations, penalties by facility | ✅ Yes | https://echo.epa.gov/ |
| **OSHA Inspections** | Workplace safety enforcement | Inspections, violations, penalties by employer | ✅ Yes | https://www.osha.gov/ords/imis/establishment.html |

**Why They Matter for Bottleneck Detection:**
- EPA violations can signal operational issues, potential shutdowns
- OSHA citations indicate labor/safety problems
- Penalty trends by industry reveal sector stress

**EPA ECHO API:** https://www.epa.gov/enviro/envirofacts-data-service-api

**OSHA API:** https://enforcedata.dol.gov/views/data_catalogs.php

**Key Data Points:**
| Agency | Data Available | Signal |
|--------|---------------|--------|
| EPA | Facilities in non-compliance | Operational risk |
| EPA | Significant violators list | High-risk facilities |
| OSHA | Inspection outcomes | Safety issues by industry |
| OSHA | Penalty amounts | Severity of violations |

---

### Political & Lobbying Data

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **Lobbying Disclosure Act Database** | Official lobbying filings | Lobbying registrations, activities, spending | ✅ Yes | https://lda.senate.gov/system/public/ |
| **OpenSecrets (Center for Responsive Politics)** | Money in politics | Campaign contributions, lobbying, PACs, dark money | ✅ Yes (free tier) | https://www.opensecrets.org/ |

**Why They Matter for Bottleneck Detection:**
- Lobbying spending spikes signal regulatory pressure
- Industry lobbying reveals sector priorities/concerns
- Can lead policy changes that create bottlenecks

**OpenSecrets API:** https://www.opensecrets.org/api (free API key required)

**Key Data Points:**
| Source | Data | Signal |
|--------|------|--------|
| LDA | Lobbying by industry | Policy priorities |
| OpenSecrets | Top lobbying clients | Who's spending to influence |
| OpenSecrets | Lobbying by issue | What's being fought over |

---

### Banking & Financial Institution Data

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **FDIC Call Reports** | Bank financial statements | Assets, liabilities, income, loans, deposits | ✅ Yes | https://www.fdic.gov/resources/bank-and-industry-data/call-reports |

**Why It Matters for Bottleneck Detection:**
- Loan growth/contraction by sector
- Non-performing loan trends
- Bank health indicators
- Credit availability signals

**FDIC API:** https://banks.data.fdic.gov/api/

**Key Data Points:**
| Metric | Signal |
|--------|--------|
| Commercial & Industrial Loans | Business credit demand |
| Real Estate Loans | Construction/housing activity |
| Non-Performing Loans | Credit stress |
| Loan Loss Provisions | Banks bracing for losses |

---

### Secured Transactions (UCC Filings)

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **State UCC Filings** | Uniform Commercial Code filings | Secured transactions, liens, collateral | Varies by state | State Secretary of State offices |

**Why It Matters for Bottleneck Detection:**
- UCC filings show collateralized lending activity
- Spike in filings can indicate credit stress
- Terminations can signal loan payoffs or defaults

**Access by State:**
| State | UCC Search Portal |
|-------|------------------|
| California | https://bizfileonline.sos.ca.gov/search/ucc |
| Texas | https://direct.sos.state.tx.us/UCC/default.aspx |
| New York | https://appext20.dos.ny.gov/uccs_public/uccs_search |
| Florida | https://ccfcorp.dos.state.fl.us/UCCIndex.html |
| Illinois | https://www.ilsos.gov/uccsearch/ |
| Delaware | https://icis.corp.delaware.gov/UCCInquiry |

**Note:** Most states offer free search; bulk data downloads may require fees.

---

### Analyst Commentary & Earnings Insights

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **Seeking Alpha** | Investment analysis platform | Earnings call transcripts, analyst articles, news | ⚠️ Limited (RSS, scraping) | https://seekingalpha.com |

**Why It Matters for Bottleneck Detection:**
- Earnings call transcripts reveal management commentary on supply chains
- Analyst articles synthesize industry trends
- Comments often highlight developing issues before they're news

**Free Tier Access:**
| Feature | Free Access |
|---------|-------------|
| Earnings call transcripts | ⚠️ Partial (recent available) |
| News articles | ✅ Yes |
| Analysis articles | ⚠️ Limited per month |
| Stock data | ✅ Basic |

**Alternative for Earnings Transcripts:**
- SEC EDGAR 8-K filings (many companies file transcripts)
- Company investor relations websites (usually free)

---

### Federal Spending & Procurement Data

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **USASpending.gov** | Federal spending transparency | Contracts, grants, loans, direct payments by agency/vendor/location | ✅ Yes | https://api.usaspending.gov/ |

**Why It Matters for Bottleneck Detection:**
- Track government contract awards to specific industries/vendors
- Identify spending acceleration or contraction by sector
- Monitor procurement activity as leading indicator of government priorities
- Geographic distribution of federal spending

**API Access:** https://api.usaspending.gov/ (No API key required)

**Key Data Points:**
| Metric | Signal |
|--------|--------|
| Contract awards by NAICS | Industry-level government demand |
| Vendor spending trends | Company-level revenue signals |
| Geographic spending | Regional economic stimulus |
| Agency spending velocity | Budget execution health |

---

### Veterans Healthcare Utilization Data

| Source | Description | Data Types | API Available | Access |
|--------|-------------|------------|---------------|--------|
| **VA Open Data Portal** | Veterans healthcare system | Wait times, patient volume, service mix, quality metrics | ❌ Download | https://www.data.va.gov/ |

**Why It Matters for Bottleneck Detection:**
- Healthcare system demand and capacity strain signals
- Regional healthcare access issues
- Staffing and resource allocation trends

**Note:** This is download-only data (no API). For VA-related spending on contractors, use USASpending.gov instead.

**Key Data Points:**
| Metric | Signal |
|--------|--------|
| Average wait times | Healthcare capacity strain |
| Patient volume by facility | Regional demand |
| Service utilization | Healthcare sector activity |

---

### Summary: Free Alternative Data for Bottleneck Detection

| Category | Best Free Source | Primary Use Case | Data Lag |
|----------|-----------------|------------------|----------|
| **Consumer Demand** | Google Trends | Early demand signals | Real-time |
| **Global Events/Sentiment** | GDELT | Supply chain disruption monitoring | 15 minutes |
| **Consumer Confidence** | UMich Sentiment (FRED) | Spending outlook | Monthly |
| **Consumer Credit** | Fed G.19 (FRED) | Financial stress signals | Monthly |
| **Trade Flows** | USA Trade Online | Import/export trends | Monthly |
| **Global Trade** | UN Comtrade | Supplier country dependencies | Monthly |
| **Shipping Rates** | Baltic Dry Index (FRED) | Shipping bottlenecks | Daily |
| **Environmental Risk** | EPA ECHO | Facility operational issues | Real-time |
| **Labor Safety** | OSHA Inspections | Industry labor problems | Real-time |
| **Political/Regulatory** | OpenSecrets | Policy change signals | Quarterly |
| **Bank Health** | FDIC Call Reports | Credit availability | Quarterly |
| **Corporate Liens** | State UCC Filings | Credit stress | Real-time |
| **Earnings Analysis** | Seeking Alpha | Management commentary | Real-time |
| **Federal Spending** | USASpending.gov | Government procurement signals | Daily |
| **VA Healthcare** | VA Open Data Portal | Healthcare capacity strain | Quarterly |

---

## API Key Summary

### Government APIs

| Agency | API Key Required | Registration Link | Rate Limits |
|--------|------------------|-------------------|-------------|
| FRED | ✅ Yes (free) | https://fred.stlouisfed.org/docs/api/api_key.html | 120 requests/min |
| BLS | ✅ Yes (free) | https://data.bls.gov/registrationEngine/ | 500 queries/day (v2) |
| BEA | ✅ Yes (free) | https://apps.bea.gov/api/signup/ | 100 requests/min |
| Census | ✅ Yes (free) | https://api.census.gov/data/key_signup.html | 500 requests/day |
| EIA | ✅ Yes (free) | https://www.eia.gov/opendata/register.php | 3,600 requests/hour |
| SEC EDGAR | ❌ No | N/A | 10 requests/sec (be polite) |
| Treasury | ❌ No | N/A | Reasonable use |
| USDA NASS | ✅ Yes (free) | https://quickstats.nass.usda.gov/api | 50 requests/min |
| USGS | ❌ No | N/A | Reasonable use |
| EPA | ❌ No | N/A | Reasonable use |
| NOAA | ✅ Yes (free) | https://www.ncdc.noaa.gov/cdo-web/token | 10,000 requests/day |
| FEMA | ❌ No | N/A | Reasonable use |
| FDIC | ❌ No | N/A | Reasonable use |
| SBA | ❌ No | N/A | Reasonable use |
| FBI Crime | ❌ No | N/A | Reasonable use |
| GSA | ✅ Yes (free) | https://open.gsa.gov/api/ | Varies by endpoint |
| NCES Education | ❌ No | N/A | Reasonable use |
| USASpending | ❌ No | https://api.usaspending.gov/ | Reasonable use |

### Alternative/Supplementary Data APIs

| Source | API Key Required | Registration Link | Rate Limits |
|--------|------------------|-------------------|-------------|
| Google Trends | ❌ No (via pytrends) | N/A | ~10 requests/min (unofficial) |
| GDELT | ❌ No | BigQuery requires Google account | 1TB/month free (BigQuery) |
| UN Comtrade | ✅ Yes (free) | https://comtradeplus.un.org/ | 100 requests/day, 10K records/request |
| OpenSecrets | ✅ Yes (free) | https://www.opensecrets.org/api | 200 calls/day |
| USA Trade Online | ❌ No | Free account recommended | Reasonable use |
| Seeking Alpha | ⚠️ No (scraping) | N/A | Rate limited by site |

---

## Quick Reference: Key Economic Indicators by Agency

| Indicator | Agency | Series ID (Example) | Frequency |
|-----------|--------|---------------------|-----------|
| GDP | BEA | GDPC1 | Quarterly |
| Nonfarm Payrolls | BLS | CES0000000001 | Monthly |
| Unemployment Rate | BLS | LNS14000000 | Monthly |
| CPI (Inflation) | BLS | CUUR0000SA0 | Monthly |
| PPI | BLS | WPUFD4 | Monthly |
| Industrial Production | Fed | INDPRO | Monthly |
| Retail Sales | Census | RSXFS | Monthly |
| Housing Starts | Census | HOUST | Monthly |
| Consumer Confidence | Conference Board (via FRED) | CSCICP03USM665S | Monthly |
| ISM Manufacturing | ISM (via FRED) | MANEMP | Monthly |
| Crude Oil Inventories | EIA | WCRSTUS1 | Weekly |
| Natural Gas Storage | EIA | NG.NW2_EPG0_SWO_R48_BCF.W | Weekly |
| Trade Balance | Census/BEA | BOPGSTB | Monthly |
| Federal Debt | Treasury | GFDEBTN | Daily |
| 10-Year Treasury Yield | Treasury | DGS10 | Daily |

---

## Notes on Data Quality and Timeliness

| Issue | Description | Mitigation |
|-------|-------------|------------|
| **Revisions** | Most economic data gets revised 1-3 times | Track revision history, use "final" flag |
| **Seasonal Adjustment** | SA vs NSA can differ significantly | Store both, document which is used |
| **Publication Lag** | Government data often 1-6 weeks delayed | Note release dates, build lag into models |
| **Definition Changes** | Methodologies change over time | Monitor agency announcements |
| **Geographic Granularity** | Some data only at national/state level | Note geographic limits |
| **Vintage Tracking** | Know which version of data you're using | Store collection timestamp |

---

## Legal Status

### Government Data Sources (Sections 1-12)

All U.S. government data sources listed in this document are:
- ✅ In the **public domain** (US Government works per 17 U.S.C. § 105)
- ✅ **Free to access** without subscription
- ✅ **Free to use** for any purpose (commercial or non-commercial)
- ✅ Available via **official APIs or download**

**Attribution:** While not legally required for public domain data, attribution is encouraged and considered good practice.

### Alternative/Supplementary Data Sources (Section 13)

Non-government data sources have varying terms:

| Source | License/Terms | Commercial Use |
|--------|---------------|----------------|
| **Google Trends** | Google Terms of Service | ✅ Yes (with attribution) |
| **GDELT** | Open data, freely available | ✅ Yes |
| **UN Comtrade** | UN data terms | ✅ Yes (with attribution) |
| **OpenSecrets** | Attribution required | ✅ Yes (with attribution) |
| **Seeking Alpha** | Terms of Service apply | ⚠️ Check ToS for scraping |
| **Baltic Dry Index** | Via FRED (public domain) | ✅ Yes |
| **UMich Sentiment** | Via FRED (public domain) | ✅ Yes |

**Recommendation:** Always check terms of service for non-government sources, especially for commercial use or redistribution.

---

*This catalog is maintained for the Bottom-Up Fundamental Channel Check Researcher project.*

---

## Document Statistics

| Category | Count |
|----------|-------|
| **Federal Agencies** | 50+ agencies |
| **Regional Fed Banks** | 12 districts |
| **State Portals** | 50 states + 5 territories |
| **State Comptrollers** | 50 states |
| **International Sources** | 15+ countries/orgs |
| **Alternative Data Sources** | 14 sources |
| **Total APIs Documented** | 25+ APIs |

**Last Updated:** January 12, 2026

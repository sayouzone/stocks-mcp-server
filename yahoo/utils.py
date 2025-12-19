_QUERY1_URL_ = 'https://query1.finance.yahoo.com'
_BASE_URL_ = 'https://query2.finance.yahoo.com'
_ROOT_URL_ = 'https://finance.yahoo.com'
_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary"

quote_summary_valid_modules = (
    "summaryProfile",  # contains general information about the company
    "summaryDetail",  # prices + volume + market cap + etc
    "assetProfile",  # summaryProfile + company officers
    "fundProfile",
    "price",  # current prices
    "quoteType",  # quoteType
    "esgScores",  # Environmental, social, and governance (ESG) scores, sustainability and ethical performance of companies
    "incomeStatementHistory",
    "incomeStatementHistoryQuarterly",
    "balanceSheetHistory",
    "balanceSheetHistoryQuarterly",
    "cashFlowStatementHistory",
    "cashFlowStatementHistoryQuarterly",
    "defaultKeyStatistics",  # KPIs (PE, enterprise value, EPS, EBITA, and more)
    "financialData",  # Financial KPIs (revenue, gross margins, operating cash flow, free cash flow, and more)
    "calendarEvents",  # future earnings date
    "secFilings",  # SEC filings, such as 10K and 10Q reports
    "upgradeDowngradeHistory",  # upgrades and downgrades that analysts have given a company's stock
    "institutionOwnership",  # institutional ownership, holders and shares outstanding
    "fundOwnership",  # mutual fund ownership, holders and shares outstanding
    "majorDirectHolders",
    "majorHoldersBreakdown",
    "insiderTransactions",  # insider transactions, such as the number of shares bought and sold by company executives
    "insiderHolders",  # insider holders, such as the number of shares held by company executives
    "netSharePurchaseActivity",  # net share purchase activity, such as the number of shares bought and sold by company executives
    "earnings",  # earnings history
    "earningsHistory",
    "earningsTrend",  # earnings trend
    "industryTrend",
    "indexTrend",
    "sectorTrend",
    "recommendationTrend",
    "futuresChain",
)
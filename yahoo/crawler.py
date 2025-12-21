from .client import YahooClient

from .parsers import (
    YahooInfoParser,
    YahooNewsParser,
    YahooMarketParser,
    YahooFundamentalsParser,
)

class YahooCrawler:

    def __init__(self):
        self.client = YahooClient()

        # 파서 초기화
        self._info_parser = YahooInfoParser(self.client)
        self._news_parser = YahooNewsParser(self.client)
        self._market_parser = YahooMarketParser(self.client)
        self._fundamentals_parser = YahooFundamentalsParser(self.client)

    def info(self, ticker: str):
        return self._info_parser.fetch(ticker)

    def news(self, query: str, max_articles: int = 100, period: str = "7"):
        return self._news_parser.fetch(query, max_articles, period)
        #return self._news_parser.fetch_with_yfinance(query, max_articles, period)
    
    def market(self, ticker: str, start_date: str | None = None, end_date: str | None = None):
        return self._market_parser.fetch(ticker, start_date=start_date, end_date=end_date)
        #return self._market_parser.fetch_with_yfinance(ticker, start_date=start_date, end_date=end_date)

    def dividends(self, ticker: str):
        return self._market_parser.dividends(ticker)
    
    def fundamentals(self, ticker: str):
        return self._fundamentals_parser.fetch_financials(ticker, name="income", timescale="yearly")
    
    def quarterly_fundamentals(self, ticker: str):
        return self._fundamentals_parser.fetch_financials(ticker, name="income", timescale="quarterly")

    def balance_sheet(self, ticker: str):
        return self._fundamentals_parser.fetch_financials(ticker, name="balance-sheet", timescale="yearly")

    def quarterly_balance_sheet(self, ticker: str):
        return self._fundamentals_parser.fetch_financials(ticker, name="balance-sheet", timescale="quarterly")

    def cash_flow(self, ticker: str):
        return self._fundamentals_parser.fetch_financials(ticker, name="cash-flow", timescale="yearly")

    def quarterly_cash_flow(self, ticker: str):
        return self._fundamentals_parser.fetch_financials(ticker, name="cash-flow", timescale="quarterly")

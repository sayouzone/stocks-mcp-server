from .client import NaverClient
#from .models import DartConfig

from .parsers import (
    NaverNewsParser,
    NaverMarketParser,
)

class NaverCrawler:
        
    """DART 공시 문서 크롤러.
    
    기업의 공시 문서를 DART에서 크롤링하여 GCS에 업로드합니다.
    """
    
    # 제외할 공시 유형
    EXCLUDED_REPORT_TYPES = frozenset({"기업설명회(IR)개최(안내공시)"})
    request_delay_seconds = 3

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/120.0.0.0 Safari/537.36'
    }
    
    def __init__(self):
        """크롤러를 초기화합니다.
        
        Args:
            code: 기업 코드 (기본값: 삼성전자)
        """
        self.client = NaverClient()

        # 파서 초기화
        self._news_parser = NaverNewsParser(self.client)
        self._market_parser = NaverMarketParser(self.client)
        self._corp_data : Optional[list] = None

    def main(self, stock: str):
        return self._main_parser.parse(stock)

    def market(self, stock: str, start_date: str = None, end_date: str = None):
        return self._market_parser.fetch(stock, start_date=start_date, end_date=end_date)

    def main_prices(self, stock: str):
        return self._market_parser.fetch_main_prices(stock)

    def company_metadata(self, stock: str):
        return self._market_parser.fetch_company_metadata(stock)

    def news(self, query: str, max_articles: int = 100):
        news_list = self._news_parser.fetch(query=query, max_articles=max_articles)

        return self._news_parser.parse(news_list)

    def category_news(self):
        return self._news_parser.fetch(category="카테고리")
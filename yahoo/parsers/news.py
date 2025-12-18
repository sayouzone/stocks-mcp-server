import pandas as pd
import yfinance as yf

class YahooNewsParser:
    """
    Yahoo Finance API 파싱 클래스
    
    뉴스: News, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
    """

    def __init__(self, client: YahooClient):
        self.client = client

    def fetch(
        self,
        query: str,
        max_articles: int = 100,
        period: str = "7",
    ) -> pd.DataFrame:
        """
        회사 이름(query)을 받아서 티커로 변환한 뒤,
        Yahoo Finance에서 뉴스를 수집하고, 기사 본문을 크롤링한 후
        BigQuery에 저장합니다.
        """

        ticker = "TSLA"
        news_list = yf.Ticker(ticker).news

        if not news_list:
            print(f"News not found {ticker}")
            return pd.DataFrame()
        
        if max_articles is not None:
            news_list = news_list[:max_articles]

        articles = []
        total_articles = len(news_list)
        for i, item in enumerate(news_list):
            link = item.get('content', {}).get('canonicalUrl', {}).get('url')
            item['crawled_content'] = None # Initialize to None
            if link:
                item['crawled_content'] = self._fetch_content(link)
            articles.append(item)
        
        df = pd.json_normalize(articles)

        # Clean column names for BigQuery
        df.columns = df.columns.str.replace('.', '_', regex=False)

        # Rename for compatibility
        df.rename(columns={'content_pubDate': 'providerPublishTime', 'content_canonicalUrl_url': 'link'}, inplace=True)

        # Add additional metadata
        df['search_keyword'] = query
        df['crawled_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert publish time to datetime and then to string
        if 'providerPublishTime' in df.columns:
            df['providerPublishTime'] = pd.to_datetime(df['providerPublishTime']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Reorder columns to have crawled_content at the end
        if 'crawled_content' in df.columns:
            cols = df.columns.tolist()
            cols.remove('crawled_content')
            cols.append('crawled_content')
            df = df[cols]

        period_days = int(period)
        cutoff_date = datetime.now() - timedelta(days=period_days)
        df = df[pd.to_datetime(df['providerPublishTime']) >= cutoff_date]

        return df

    def _fetch_content(self, url: str) -> str | None:
        """
        URL을 받아 웹 페이지의 본문 텍스트를 크롤링합니다.
        """

        response = self.client._get(url)

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove script and style elements
        for script_or_style in soup(['script', 'style']):
            script_or_style.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # Break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # Drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        return text

    def _parse(self, query: str, limit: int | None = None, period: str = "7") -> pd.DataFrame:
        """
        BigQuery에 캐시된 Yahoo Finance 뉴스를 조회하고 반환합니다.
        """
        table_id = f"news-yahoo-{query}" # Consistent table_id
        
        # Calculate cutoff date for filtering
        period_days = int(period)
        cutoff_date = datetime.now() - timedelta(days=period_days)
        
        # Query BigQuery for cached data
        cached_df = await asyncio.to_thread(
            self.bq_manager.query_table,
            table_id=table_id,
            start_date=cutoff_date.strftime('%Y-%m-%d %H:%M:%S'), # Filter by crawled_at or providerPublishTime
            order_by_date=True # Assuming 'providerPublishTime' or 'crawled_at' can be ordered
        )

        if cached_df is None or cached_df.empty:
            yield {"type": "result", "data": []}
            return
        
        if 'providerPublishTime' in cached_df.columns:
            cached_df['providerPublishTime'] = pd.to_datetime(cached_df['providerPublishTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        cached_df.fillna('', inplace=True)
        if limit is not None:
            cached_df = cached_df.head(limit)
        yield {"type": "result", "data": cached_df.to_dict(orient='records')}
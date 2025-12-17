class NaverMarketParser:

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Accept' : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    }

    base_url = 'https://finance.naver.com'

    def __init__(self, 
        code: Optional[str] = '005930'):
        
        self.code = code
        self.client = httpx.AsyncClient(headers=self.headers, follow_redirects=True)

    async def fetch(
        self,
        code: str | None = None, 
        start_date: str | None = None, 
        end_date: str | None = None, 
        max_page: int = 100
    ) -> pd.DataFrame:
        """
        기업 코드로 시작일과 종료일 사이 일별 시세를 조회한다.
        한 페이지의 크기는 10으로 고정되어 있음 (Gemini 답변)

        Args:
            code (str): 기업 코드, 예: 005930 (삼성전자)
            start_date (str): 시작일, 예: 2025-09-01
            end_date (str): 종료일, 예: 2025-12-31
            max_page (int): 최대 페이지 수

        Returns:
            일별 시세 DataFrame
        """
        
        if code:
            self.code = code

        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        
        market_prices = await self._fetch_historical_data(code, max_page=max_page)
        print(market_prices, type(market_prices))

        if market_prices.empty:
            print("type:", "error", ",", "message:", "Failed to crawl market data.")
            return

        # Filter by date range
        market_prices = market_prices[
            (market_prices['date'] >= pd.to_datetime(start_date)) &
            (market_prices['date'] <= pd.to_datetime(end_date))
        ]

        return market_prices
        
    async def _fetch_historical_data(
        self,
        code: str,
        start_date: str | None = None,
        max_page: int = 99999
    ) -> pd.DataFrame:
        """
        기업 코드로 시작일부터 현재까지 일별 시세를 조회한다.
        한글 키를 영어로 변환. 예: '날짜': 'date'

        Args:
            code (str): 기업 코드, 예: 005930 (삼성전자) 
            start_date (str): 시작일, 예: 2025-09-01
            max_page (int): 최대 페이지 수

        Returns:
            일별 시세 DataFrame
        """
        last_page = max_page
        
        full_df = []
        page = 1
        while True:
            try:
                url = f"{base_url}/item/sise_day.nhn?code={code}&page={page}"
                print(f"Parsing URL: {url}")
                response = await self.client.get(url)
                response.raise_for_status()
        
                if last_page == max_page:
                    _last_page = self._find_last_page(response.text)
                    last_page = min(max_page, _last_page)
                    #print(_last_page, last_page)
        
                dfs = pd.read_html(StringIO(response.text))
                df = dfs[0]
                df.dropna(how='all', inplace=True)
                
                if df.empty:
                    break
                
                full_df.append(df)
        
                if start_date:
                    # 현재 페이지에서 가장 과거 날짜 확인
                    # 네이버 금융 날짜 포맷은 'YYYY.MM.DD' 이므로 문자열 비교 가능
                    min_date = df['날짜'].min()
                    #print(f"Min Date: {min_date}")
                    
                    # 현재 페이지의 가장 옛날 날짜가 설정한 시작일보다 작거나 같으면
                    # 더 과거로 갈 필요가 없으므로 루프 종료
                    if min_date <= start_date:
                        print(f"Reached start_date limit: {min_date} <= {start_date}")
                        break
                
                print(page, last_page)
                if page >= last_page:
                    break
        
                page += 1
                await asyncio.sleep(random.uniform(0.1, 0.3))
            except Exception as exc:
                print(f"Error scraping page {page}: {exc}")
                continue
                
        crawled_df = pd.concat(full_df, ignore_index=True)
        crawled_df.rename(columns={
            '날짜': 'date',
            '종가': 'close',
            '시가': 'open',
            '고가': 'high',
            '저가': 'low',
            '거래량': 'volume'
        }, inplace=True)
        
        if '전일비' in crawled_df.columns:
            crawled_df.drop(columns=['전일비'], inplace=True)
        
        numeric_cols = ['close', 'open', 'high', 'low', 'volume']
        for col in numeric_cols:
            if col in crawled_df.columns:
                crawled_df[col] = pd.to_numeric(
                    crawled_df[col].astype(str).str.replace(',', '', regex=False),
                    errors='coerce'
                ).fillna(0)
                if col == 'volume':
                    crawled_df[col] = crawled_df[col].astype('int64')
        
        crawled_df['date'] = pd.to_datetime(crawled_df['date'], errors='coerce')
        crawled_df.dropna(subset=['date'], inplace=True)
        return crawled_df

    def _find_last_page(
        self,
        html_text: str
    ) -> int:
        """
        기업 일별 시세에 대한 최대 페이지를 확인

        Args:
            html_text (str): HTML 텍스트에서 마지막 페이지 번호 확인

        Returns:
            마지막 페이지 번호
        """
        soup = BeautifulSoup(html_text, 'html.parser')
        match = None
        pg_rr_tag = soup.select_one('.pgRR a')
        
        if pg_rr_tag:
            href_value = pg_rr_tag.get('href')
            if isinstance(href_value, str):
                match = re.search(r'page=(\d+)', href_value)
    
        if match:
            last_page = int(match.group(1))
        else:
            last_page = 1
    
        return last_page

    async def fetch_market_sum(
        self,
        code: str | None = None, 
    ):
        """
        현재 종목의 시가총액을 스크래핑하여 숫자로 반환합니다.

        Args:
            code (str): 기업 코드, 예: 005930 (삼성전자)

        Returns:
            시가 총액 정수값
        """

        if code:
            self.code = code
        
        try:
            url = f'{self.base_url}/item/sise.naver?code={self.code}'
            response = await self.client.get(url)
            response.raise_for_status()
    
            soup = BeautifulSoup(html_text, 'html.parser')

            # HTML 텍스트
            market_sum_tag = soup.select_one('#_market_sum')
    
            # Tag 존재하지 않음
            if not market_sum_tag:
                return 0
            
            market_sum_text = market_sum_tag.get_text(strip=True)
            
            market_sum = 0
            parts = market_sum_text.replace(',', '').split('조')
            if len(parts) > 1:
                market_sum += int(parts[0]) * 1_0000_0000_0000
                remaining = parts[1]
            else:
                remaining = parts[0]
            
            if '억' in remaining:
                market_sum += int(remaining.replace('억', '')) * 1_0000_0000
    
            return market_sum
            
        except Exception as e:
            print(f"Exception: {e}")
        
        return 0

    async def fetch_company_metadata(
        self,
        code: str
    ) -> Dict[str, Any]:
        """
        기업의 메타데이터를 조회합니다.
        기업명, 주식 거래소, 환율, 시가 총액 등

        Args:
            code (str): 기업 코드, 예: 005930 (삼성전자)

        Returns:
            기업 메타데이터 Dictionary
        """
        metadata: Dict[str, Any] = {}
        latest_price: float | None = None

        try:
            url = f"https://m.stock.naver.com/api/stock/{code}/basic"
            print(f"Parsing URL: {url}")
            response = await self.client.get(url)
            response.raise_for_status()

            basic_json = response.json()

            # 주식 이름
            metadata["company_name"] = basic_json.get("stockName")

            # 주식 거래 형태
            exchange_info = basic_json.get("stockExchangeType") or {}
            metadata["exchange"] = (
                exchange_info.get("name")
                or basic_json.get("stockExchangeName")
            )
            # 환율
            metadata["currency"] = (
                self._infer_currency(exchange_info.get("nationCode"))
                or "KRW"
            )

            closing_price = basic_json.get("closePrice")
            if closing_price is not None:
                try:
                    latest_price = float(str(closing_price).replace(',', ''))
                except ValueError:
                    latest_price = None
        except Exception as exc:
            metadata.setdefault("_errors", {})["basic"] = str(exc)
    
        try:
            url = f"https://api.finance.naver.com/service/itemSummary.naver?itemcode={code}"
    
            headers = self.headers
            headers['Referer'] = f'https://finance.naver.com/item/main.nhn?code={code}'
            print(f"Parsing URL: {url}")
            
            response = await self.client.get(url, headers=headers)
            response.raise_for_status()
            
            summary_json = response.json()
            market_sum = summary_json.get("marketSum")
            if isinstance(market_sum, (int, float)):
                market_cap = float(market_sum) * 1_000_000  # marketSum is in million KRW
                metadata["market_cap"] = market_cap
                if latest_price and latest_price > 0:
                    metadata["shares_outstanding"] = int(round(market_cap / latest_price))
            else:
                metadata.setdefault("_warnings", []).append("marketSum missing")

        except Exception as exc:
            metadata.setdefault("_errors", {})["summary"] = str(exc)
    
        return metadata

    def _infer_currency(self, nation_code: str | None) -> str | None:
        if not nation_code:
            return None
        nation_code = nation_code.upper()
        if nation_code in {'KOR', 'KR'}:
            return 'KRW'
        if nation_code in {'USA', 'US'}:
            return 'USD'
        if nation_code in {'JPN', 'JP'}:
            return 'JPY'
        if nation_code in {'CHN', 'CN'}:
            return 'CNY'
        return None
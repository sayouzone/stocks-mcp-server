import pandas as pd
import yfinance as yf

class YahooMarketParser:
    """
    Yahoo Finance API 파싱 클래스
    
    뉴스: News, https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
    """

    def __init__(self, client: YahooClient):
        self.client = client

    def market_collect(self, ticker: str, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
        """
        Yahoo Finance로부터 시세 정보를 가져오거나 BigQuery 캐시를 사용하여
        프론트엔드 형식에 맞게 정제하여 반환합니다.
        """
        if not company:
            raise ValueError("Ticker symbol must be provided.")

        ticker_info = yf.Ticker(ticker).info

        # 1. 날짜 설정
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')

        # 2. yfinance에서 데이터 가져오기
        hist_df = yf.Ticker(ticker).history(start=start_date, end=end_date)

        if hist_df.empty:
            return
            
        # 3. 가져온 데이터 BigQuery에 저장
        hist_df.reset_index(inplace=True)
        hist_df['code'] = company
        hist_df['source'] = 'yahoo'
        
        hist_df.rename(columns={
            'Date': 'date', 'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Volume': 'volume'
        }, inplace=True)

        hist_df['date'] = pd.to_datetime(hist_df['date']).dt.date
        required_cols = ['date', 'code', 'source', 'open', 'high', 'low', 'close', 'volume']
        hist_df = hist_df[required_cols]

        for col in ['open', 'high', 'low', 'close']:
            hist_df[col] = hist_df[col].round(4)
        hist_df['volume'] = hist_df['volume'].astype('int64')

        return hist_df

    def _format_response_from_df(self, df: pd.DataFrame, ticker_info: dict, company: str):
        """DataFrame을 받아 프론트엔드 응답 형식으로 변환하는 헬퍼 함수"""
        company_name = ticker_info.get('shortName', company)
        market_cap = ticker_info.get('marketCap', 0)

        if df is None or df.empty:
            print(f"'{company_name}'에 대한 데이터가 없어 빈 응답을 반환합니다.")
            return {
                "name": company_name,
                "source": "yahoo",
                "currentPrice": {"value": 0, "changePercent": 0},
                "volume": {"value": 0, "changePercent": 0},
                "marketCap": {"value": market_cap, "changePercent": 0},
                "priceHistory": [],
                "volumeHistory": [],
            }
        
        # BQ에서 온 데이터는 'date', 'close', 'volume' 컬럼이 존재.
        # yfinance에서 온 데이터는 'Date' 인덱스와 'Close', 'Volume' 컬럼을 가짐.
        if 'date' not in df.columns:
            df.reset_index(inplace=True)
            df.rename(columns={'Date': 'date', 'Close': 'close', 'Volume': 'volume'}, inplace=True)

        df.sort_values(by='date', ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

        latest = df.iloc[0]
        previous = df.iloc[1] if len(df) > 1 else latest

        price_change_percent = ((latest['close'] - previous['close']) / previous['close']) * 100 if previous['close'] != 0 else 0
        volume_change_percent = ((latest['volume'] - previous['volume']) / previous['volume']) * 100 if previous['volume'] != 0 else 0

        latest_close = float(latest['close']) if pd.notna(latest['close']) else 0.0
        latest_volume = int(latest['volume']) if pd.notna(latest['volume']) else 0

        result = {
            "name": company_name,
            "source": "yahoo",
            "currentPrice": {
                "value": latest_close,
                "changePercent": round(price_change_percent, 2)
            },
            "volume": {
                "value": latest_volume,
                "changePercent": round(volume_change_percent, 2)
            },
            "marketCap": {
                "value": market_cap,
                "changePercent": 0 
            },
            "priceHistory": df.rename(columns={'close': 'price'})[['date', 'price']].to_dict(orient='records'),
            "volumeHistory": df[['date', 'volume']].to_dict(orient='records')
        }

        for item in result['priceHistory']:
            if isinstance(item['date'], pd.Timestamp) or isinstance(item['date'], datetime.date):
                item['date'] = pd.to_datetime(item['date']).strftime('%Y-%m-%d')
            item['price'] = float(item['price'])

        for item in result['volumeHistory']:
            if isinstance(item['date'], pd.Timestamp) or isinstance(item['date'], datetime.date):
                item['date'] = pd.to_datetime(item['date']).strftime('%Y-%m-%d')
            item['volume'] = int(item['volume'])

        return result

class Fundamentals:
    """
    MCP 도구 기반 재무제표 수집 클래스 (캐싱 기능 포함)

    기존 복잡한 분석 로직을 제거하고, 재무제표 3종만 수집하는 단순하고 명확한 구조로 리팩토링.
    GCS 캐싱 기능은 유지하여 API 호출 비용을 최소화.
    """
    GCS_CACHE_PREFIX = "yahoofinance_fundamentals_cache"

    def __init__(self):
        self.gcs_manager = GCSManager()

    def fundamentals(
        self,
        stock: str | None = None,
        query: str | None = None,
        *,
        use_cache: bool = True,
        overwrite: bool = False,
        attribute_name_str: str | None = None
    ) -> dict[str, object]:
        """
        Yahoo Finance에서 재무제표 3종을 수집합니다.

        Args:
            stock: 종목 코드 또는 회사명
            query: stock의 별칭 (stock이 없으면 사용)
            use_cache: GCS 캐시 사용 여부
            overwrite: 캐시를 무시하고 새로 가져올지 여부
            attribute_name_str: 특정 attribute만 가져올 경우 (예: 'income_stmt', 'balance_sheet')

        Returns:
            dict: {
                "ticker": str,
                "country": str,
                "balance_sheet": str | None,  # JSON 문자열
                "income_statement": str | None,  # JSON 문자열
                "cash_flow": str | None  # JSON 문자열
            }
        """
        identifier = stock or query
        if not identifier:
            raise ValueError("Must provide either 'stock' or 'query'.")

        ticker_symbol = find.get_ticker(identifier) or identifier.upper()

        # --- 특정 attribute만 요청하는 경우 (기존 호환성 유지) ---
        if attribute_name_str:
            ticker = yf.Ticker(ticker_symbol)
            if hasattr(ticker, attribute_name_str):
                data = getattr(ticker, attribute_name_str)
                if isinstance(data, pd.DataFrame):
                    return json.loads(data.to_json(orient="records", date_format="iso"))
                if isinstance(data, pd.Series):
                    return data.to_dict()
                if isinstance(data, dict):
                    return data
                return data
            else:
                raise ValueError(f"'{attribute_name_str}' is not a valid yfinance Ticker attribute.")

        # --- 재무제표 3종 수집 (MCP 도구 버전 로직) ---
        gcs_blob_name = f"{self.GCS_CACHE_PREFIX}/{ticker_symbol}.json"

        # 캐시 확인
        if use_cache and not overwrite:
            cached_payload = self.gcs_manager.read_file(gcs_blob_name)
            if cached_payload:
                try:
                    payload = json.loads(cached_payload)
                    logging.info(f"Returning cached fundamentals for {ticker_symbol} from GCS: {gcs_blob_name}")
                    return payload
                except (json.JSONDecodeError, KeyError) as e:
                    logging.warning(f"GCS cache read failed for {ticker_symbol} (corrupted JSON): {e}. Refetching.")

        # yfinance Ticker 객체 생성
        ticker = yf.Ticker(ticker_symbol)

        # 국가 정보 추론
        country = "Unknown"
        try:
            info = ticker.info or {}
            country = info.get("country") or "Unknown"
        except Exception as e:
            logging.warning(f"Failed to fetch ticker info for {ticker_symbol}: {e}")

        # 한국 종목 코드 패턴 확인
        if ".KS" in ticker_symbol or ".KQ" in ticker_symbol:
            country = "KR"
        elif ticker_symbol.replace(".KS", "").replace(".KQ", "").isdigit() and len(ticker_symbol.replace(".KS", "").replace(".KQ", "")) == 6:
            country = "KR"

        # 재무제표 3종 수집
        result = {
            "ticker": ticker_symbol,
            "country": country,
            "balance_sheet": None,
            "income_statement": None,
            "cash_flow": None
        }

        # 1. Balance Sheet (재무상태표)
        try:
            balance_sheet = ticker.balance_sheet
            if balance_sheet is not None and not balance_sheet.empty:
                result["balance_sheet"] = balance_sheet.to_json(orient="columns", date_format="iso")
        except Exception as e:
            logging.warning(f"Failed to fetch balance_sheet for {ticker_symbol}: {e}")

        # 2. Income Statement (손익계산서)
        try:
            income_stmt = ticker.income_stmt
            if income_stmt is not None and not income_stmt.empty:
                result["income_statement"] = income_stmt.to_json(orient="columns", date_format="iso")
        except Exception as e:
            logging.warning(f"Failed to fetch income_stmt for {ticker_symbol}: {e}")

        # 3. Cash Flow (현금흐름표)
        try:
            cashflow = ticker.cashflow
            if cashflow is not None and not cashflow.empty:
                result["cash_flow"] = cashflow.to_json(orient="columns", date_format="iso")
        except Exception as e:
            logging.warning(f"Failed to fetch cashflow for {ticker_symbol}: {e}")

        # GCS에 캐시 저장
        try:
            payload_json = json.dumps(result, ensure_ascii=False, indent=2)
            self.gcs_manager.upload_file(
                source_file=payload_json,
                destination_blob_name=gcs_blob_name,
                encoding="utf-8",
                content_type="application/json; charset=utf-8",
            )
            logging.info(f"Successfully cached fundamentals for {ticker_symbol} to GCS: {gcs_blob_name}")
        except Exception as e:
            logging.error(f"GCS cache write failed for {ticker_symbol}: {e}")

        return result
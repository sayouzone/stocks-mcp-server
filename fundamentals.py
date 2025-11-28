import json
import logging
import pandas as pd

from fastmcp import FastMCP
from pathlib import Path

from utils.crawler.fnguide import FnGuideCrawler
from utils.yahoofinance import Fundamentals as YahooFundamentals
from utils.gcpmanager import GCSManager

logger = logging.getLogger(__name__)
logging.basicConfig(format="[%(levelname)s]: %(message)s", level=logging.INFO)

#mcp = FastMCP(name="StockFundamentalsServer")
mcp = FastMCP("Stocks MCP Server")

@mcp.tool(
    name="find_fnguide_data",
    description="""FnGuide에서 한국 주식 재무제표 수집 (yfinance와 동일한 스키마, 캐시 사용).
    사용 대상:
    - 6자리 숫자 티커: 005930, 000660
    - .KS/.KQ 접미사: 005930.KS, 035720.KQ
    - 한국 기업명: 삼성전자, SK하이닉스

    반환: {
        "ticker": str,
        "country": "KR",
        "balance_sheet": str | None,      # JSON 문자열
        "income_statement": str | None,   # JSON 문자열
        "cash_flow": str | None           # JSON 문자열
    }

    참고: 캐시를 우선 사용하여 빠른 응답을 제공합니다.
    크롤링은 최대 60초 이상 소요될 수 있으므로 가능한 캐시를 활용합니다.
    """,
    tags={"fnguide", "fundamentals", "korea", "standardized", "cached"}
)
async def find_fnguide_data(stock: str, use_cache: bool = True):
    """
    FnGuide에서 한국 주식 재무제표 3종을 수집합니다.

    yfinance와 동일한 스키마를 반환하여 LLM 에이전트가
    한국 주식과 해외 주식을 동일한 방식으로 처리할 수 있습니다.

    Args:
        stock: 종목 코드 (예: "005930", "삼성전자")
        use_cache: GCS 캐시 사용 여부 (기본값: True, 권장)

    Returns:
        dict: 재무제표 3종 (yfinance와 동일한 스키마)

    Note:
        - use_cache=True (기본값): GCS에서 캐시된 데이터를 먼저 확인 (빠름)
        - use_cache=False: 항상 새로 크롤링 (느림, 30초+ 소요)
    """
    logger.info(f">>> 🛠️ Tool: 'find_fnguide_data' called for '{stock}'")

    crawler = FnGuideCrawler(stock=stock)
    data = await crawler.fundamentals(use_cache=use_cache)
    return data


@mcp.tool(
    name="find_yahoofinance_data",
    description=(
        "Fetch fundamentals data for a given company from Yahoo Finance."
        "The attribute parameter should match yfinance.Ticker attribute names "
        "such as 'income_stmt', 'balance_sheet', or 'cashflow'."
    ),
    tags={"finance", "stocks", "fundamentals", "global"}
)
def find_yahoofinance_data(query: str, attribute: str):
    """
    단일 attribute를 가져오는 함수 (후방 호환성 유지)

    Args:
        query: 종목 코드 또는 회사명 (예: '005930', '삼성전자', 'AAPL', 'Apple')
        attribute:

    Returns:
        
    """
    logger.info(f">>> 🛠️ Tool: 'find_yahoofinance_data' called for '{query}'")

    data = YahooFundamentals().fundamentals(query=query, attribute_name_str=attribute)

    if isinstance(data, pd.DataFrame):
        return json.loads(data.to_json(orient="records", date_format="iso"))
    if isinstance(data, pd.Series):
        return data.to_dict()
    if isinstance(data, dict):
        return data
    
    return data


@mcp.tool(
    name="get_yahoofinance_fundamentals",
    description="""Yahoo Finance에서 해외 주식 재무제표 수집 (GCS 캐싱 지원).
    사용 대상:
    - 알파벳 티커 (1-5자): AAPL, TSLA, GOOGL
    - 해외 기업명: Apple, Tesla, Microsoft

    반환: balance_sheet, income_statement, cash_flow (연간 데이터)
    """,
    tags={"yahoo", "fundamentals", "global", "cached"}
)
def get_yahoofinance_fundamentals(query: str, use_cache: bool = True):
    """
    재무제표 3종(income_stmt, balance_sheet, cashflow)을 한 번에 가져오는 통합 함수

    리팩토링된 utils.yahoofinance.Fundamentals 클래스를 사용하여:
    - GCS 캐싱으로 반복 API 호출 방지
    - 간결한 코드 구조
    - logging 모듈을 통한 체계적인 로깅

    Args:
        query: 종목 코드 또는 회사명 (예: '005930', '삼성전자', 'AAPL', 'Apple')
        use_cache: GCS 캐시 사용 여부 (기본값: True)

    Returns:
        dict: {
            "ticker": str,  # Yahoo Finance 티커 (예: '005930.KS', 'AAPL')
            "country": str,  # 상장 국가 (예: 'KR', 'US', 'Unknown')
            "balance_sheet": str | None,  # 재무상태표 (JSON 문자열)
            "income_statement": str | None,  # 손익계산서 (JSON 문자열)
            "cash_flow": str | None  # 현금흐름표 (JSON 문자열)
        }
    """
    logger.info(f">>> 🛠️ Tool: 'get_yahoofinance_fundamentals' called for '{query}'")

    # 리팩토링된 Fundamentals 클래스 사용 (캐싱 포함)
    data = YahooFundamentals().fundamentals(query=query, use_cache=use_cache)
    return data

@mcp.tool(
    name="save_fundamentals_data_to_gcs",
    description="Saves fundamentals data to a CSV file in Google Cloud Storage.",
    tags={"gcs", "fundamentals", "storage"}
)
def save_fundamentals_data_to_gcs(
    data: dict | list, 
    gcs_path: str, 
    file_name: str) -> str:
    """
    Saves fundamentals data to a CSV file in Google Cloud Storage.

    Args:
        data: The JSON-like object returned from fetch_fnguide_data or fetch_yahoofinance_data.
        gcs_path: The destination folder path in the GCS bucket.
        file_name: The name of the CSV file.

    Returns:
        A message about saving fundamental data to GCS
    """
    logger.info(f">>> 🛠️ Tool: 'save_fundamentals_data_to_gcs' called for '{gcs_path}'")

    if not data:
        return "Fundamentals data cannot be empty."

    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        return "Data must be a dict or a list of dicts."

    csv_data = df.to_csv(index=False)

    gcs_manager = GCSManager()
    destination_blob_name = f"{gcs_path}/{file_name}"
    
    success = gcs_manager.upload_file(
        source_file=csv_data,
        destination_blob_name=destination_blob_name,
        content_type="text/csv"
    )

    message = f"Successfully saved fundamentals data to gs://{gcs_manager.bucket_name}/{destination_blob_name}"
    if not success:
        message = "Failed to upload file to GCS."
    
    return message

"""
@mcp.prompt()
def find(animal: str) -> str:
    "Find which exhibit and trail a specific animal might be located."

    return (
        f"Please find the exhibit and trail information for {animal} in the zoo. "
        f"Respond with '[animal] can be found in the [exhibit] on the [trail].'"
        f"Example: Penguins can be found in The Arctic Exhibit on the Polar Path."
    )
"""
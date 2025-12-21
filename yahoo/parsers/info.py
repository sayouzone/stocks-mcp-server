import logging
import pandas as pd
import random
import time
import yfinance as yf

from bs4 import BeautifulSoup, Tag
from datetime import datetime, timedelta

from ..client import YahooClient
from ..utils import (
    _ROOT_URL_,
    _QUERY1_URL_,
    _QUERY2_URL_,
    _QUOTE_SUMMARY_URL_,
    _ADDITIONAL_URL_,
    _CRUMB_URL_,
    quote_summary_valid_modules
)

logger = logging.getLogger(__name__)

class YahooInfoParser:
    """
    Yahoo Finance Info 파싱 클래스

    https://github.com/ranaroussi/yfinance/blob/main/yfinance/scrapers/quote.py
    """

    MODULES = [
        "financialData",
        "quoteType", 
        "defaultKeyStatistics",
        "assetProfile",
        "summaryDetail",
    ]

    def __init__(self, client: YahooClient):
        self.client = client
        self._crumb: Optional[str] = None

    def fetch(self, ticker: str):
        """
        회사 이름(query)을 받아서 티커로 변환한 뒤,
        Yahoo Finance에서 뉴스를 수집하고, 기사 본문을 크롤링합니다.
        """

        info_url = f"{_QUOTE_SUMMARY_URL_}/{ticker}"
        params = {
            "modules": ",".join(self.MODULES), 
            "corsDomain": "finance.yahoo.com", 
            "formatted": "false", 
            "symbol": ticker
        }

        if _QUERY2_URL_ in info_url:
            params["crumb"] = self._fetch_crumb()

        response = self.client._get(info_url, params=params)
        info = response.json()

        #print(info)

        params = {"symbols": ticker, "formatted": "false"}
        params["crumb"] = self._fetch_crumb()

        response = self.client._get(_ADDITIONAL_URL_, params=params)
        additional_info = response.json()
        #print(additional_info)

        if additional_info is not None and info is not None:
            info.update(additional_info)
        else:
            info = additional_info

        query_info = {}
        for quote in ['quoteSummary', 'quoteResponse']:
            if quote in info and len(info[quote]['result']) > 0:
                info[quote]['result'][0]["symbol"] = ticker
                query = next(
                    (item for item in info.get(quote, {}).get('result', []) 
                    if item.get('symbol') == ticker), 
                    None
                )
                
                if query:
                    query_info.update(query)

        # Normalize and flatten nested dictionaries while converting maxAge from days (1) to seconds (86400).
        # This handles Yahoo Finance API inconsistency where maxAge is sometimes expressed in days instead of seconds.
        processed_info = {}
        for k, v in query_info.items():
            if isinstance(v, dict):
                for k1, v1 in v.items():
                    if v1 is not None:
                        processed_info[k1] = 86400 if k1 == 'maxAge' and v1 == 1 else v1
            elif v is not None:
                processed_info[k] = v

        query_info = processed_info

        # recursively format but only because of 'companyOfficers'

        def _format(k, v):
            if isinstance(v, dict) and "raw" in v and "fmt" in v:
                v2 = v["fmt"] if k in {"regularMarketTime", "postMarketTime"} else v["raw"]
            elif isinstance(v, list):
                v2 = [_format(None, x) for x in v]
            elif isinstance(v, dict):
                v2 = {k: _format(k, x) for k, x in v.items()}
            elif isinstance(v, str):
                v2 = v.replace("\xa0", " ")
            else:
                v2 = v
            return v2

        return {k: _format(k, v) for k, v in query_info.items()}

    def _fetch_crumb(self):
        """Yahoo Finance 크럼(인증 토큰) 획득"""
        if self._crumb:
            return self._crumb

        self.client._get(_ROOT_URL_)

        response = self.client._get(_CRUMB_URL_)
        self._crumb = response.content.decode("utf-8").strip()
        return self._crumb
        
# Copyright (c) 2025, Sayouzone
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import pandas as pd

from io import StringIO
from lxml import html
from typing import Optional

from ..client import KisratingClient
from ..models import Statistics, XPathConfig, FileType, DownloadFile
from ..utils import (
    _STATISTICS_SPREAD_URL_,
    _STATISTICS_SPREAD_EXCEL_URL_,
    get_filename,
)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StatisticsParser:
    DEFAULT_LOCAL_PATH = './statistics'

    def __init__(
        self,
        client: KisratingClient,
        config: Optional[XPathConfig] = None,
        local_path: Optional[str] = None,
    ):
        self.client = client

        self._config = config or XPathConfig()
        self._local_path = local_path or self.DEFAULT_LOCAL_PATH
    
    def fetch(self, start_date: str) -> Statistics:
        # POST 요청에 담을 데이터 (payload)
        payload = {
            'startDt': start_date
        }

        response = self.client._post(_STATISTICS_SPREAD_URL_, data=payload)

        yield_df = self._extract_table(response.text, self._config.yield_xpath)
        spread_df = self._extract_table(response.text, self._config.spread_xpath)

        return Statistics(yield_df=yield_df, spread_df=spread_df)
    
    def fetch_excel(self, start_date: str) -> Statistics:
        # POST 요청에 담을 데이터 (payload)
        payload = {
            'startDt': start_date
        }

        response = self.client._post(_STATISTICS_SPREAD_EXCEL_URL_, data=payload)
        headers = response.headers
        #print(headers)

        filename = get_filename(headers)
        #print(filename)
        file_type = self._detect_file_type(filename)

        file_path = self._save_file(filename, response.content)

        return DownloadFile(
            filename=filename, 
            filepath=file_path, 
            content=response.content, 
            file_type=file_type
        )

    def _extract_table(self, html_string: str, xpath_expr: str) -> Optional[pd.DataFrame]:
        """XPath로 지정된 영역에서 테이블을 추출합니다."""
        tree = html.fromstring(html_string)
        elements = tree.xpath(xpath_expr)
        
        if not elements:
            return None
        
        # 해당 요소의 HTML만 추출하여 테이블 파싱
        element_html = html.tostring(elements[0], encoding='unicode')
        #print(element_html)
        tables = pd.read_html(StringIO(element_html))
        
        return tables[0] if tables else None

    def _extract_df_by_xpath(self, html_string, xpath_expr):
        # HTML 문자열을 파싱하여 Element 객체 생성
        tree = html.fromstring(html_string)

        elements = tree.xpath(xpath_expr)

        # 결과 확인 및 텍스트 추출
        if elements:
            # 첫 번째 일치하는 요소의 텍스트를 가져옵니다.
            # .text_content()는 태그 내부의 모든 텍스트를 합쳐서 보여줍니다.
            extracted_text = elements[0].text_content()
            print(f"XPath 경로: {xpath_expr}")
            #print(f"추출된 텍스트: {extracted_text}")
        else:
            print("해당 XPath 경로와 일치하는 요소를 찾을 수 없습니다.")

        # pandas의 read_html을 사용하여 응답 텍스트에서 HTML 테이블을 추출합니다.
        # read_html은 페이지의 모든 테이블을 리스트 형태로 반환합니다.
        tables = pd.read_html(StringIO(html_string))

        # 테이블이 성공적으로 추출되었는지 확인합니다.
        if tables:
            # 일반적으로 원하는 데이터는 첫 번째 테이블에 있습니다.
            df = tables[0]
            print("성공적으로 테이블을 추출했습니다.")
            print(df)
        
            ## 결과를 CSV 파일로 저장합니다.
            #df.to_csv("kisrating_spread.csv", index=False, encoding='utf-8-sig')
            #print("\n'kisrating_spread.csv' 파일로 저장되었습니다.")
            return df
        
        else:
            print("페이지에서 테이블 데이터를 찾을 수 없었습니다.")
            return None
    
    def _detect_file_type(self, filename: str) -> FileType:
        """
        파일명으로 파일 유형 감지
        
        Args:
            filename: 파일명
            
        Returns:
            FileType: 파일 유형
        """
        lower_name = filename.lower()
        if lower_name.endswith((".xlsx", ".xls")):
            return FileType.EXCEL
        elif lower_name.endswith(".zip"):
            return FileType.ZIP
        elif lower_name.endswith(".csv"):
            return FileType.CSV
        return FileType.UNKNOWN

    
    def _save_file(self, filename: str, content: bytes) -> str:
        """
        파일을 로컬에 저장
        
        Args:
            filename: 저장할 파일명
            content: 파일 내용
        Returns:
            저장된 파일 경로
        """
        os.makedirs(self._local_path, exist_ok=True)
        path = os.path.join(self._local_path, filename)
        
        with open(path, "wb") as file:
            file.write(content)
        
        logger.info(f"파일 저장 완료: {path}")
        return path
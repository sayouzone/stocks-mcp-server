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

from .client import KoreainvestmentClient

from .parsers import (
    DomesticParser,
)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KoreainvestmentCrawler:
        
    """Koreainvestment Crawler"""
    
    def __init__(self, app_key: str = None, app_secret: str = None):
        """Initialize Koreainvestment Crawler"""
        self.client = KoreainvestmentClient(app_key, app_secret)

        # Parser initialization
        self._domestic_parser = DomesticParser(self.client)

    def domestic(self, start_date: str = None):
        return self._domestic_parser.fetch(start_date)

    def inquire_balance(self):
        return self._domestic_parser.inquire_balance()

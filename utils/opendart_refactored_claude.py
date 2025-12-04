"""OpenDart 공시 문서 크롤러 모듈.

DART(전자공시시스템)에서 기업 공시 문서를 크롤링하여
GCS(Google Cloud Storage)에 업로드하는 기능을 제공합니다.
"""

import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import requests
from bs4 import BeautifulSoup, Tag
from OpenDartReader import OpenDartReader

from utils import gcpmanager
from utils.companydict import companydict


# ============================================================================
# Constants
# ============================================================================

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/138.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {"User-Agent": USER_AGENT}
DART_BASE_URL = "https://dart.fss.or.kr"
REQUEST_DELAY_SECONDS = 3


# ============================================================================
# Configuration
# ============================================================================

@dataclass(frozen=True)
class DartConfig:
    """DART 크롤러 설정."""
    
    bucket_name: str
    api_key: str
    
    @classmethod
    def from_env(cls) -> "DartConfig":
        """환경 변수에서 설정을 로드합니다."""
        bucket_name = "sayouzone-ai-stocks"
        api_key = os.environ.get("DART_API_KEY")
        
        if not api_key:
            raise ValueError("DART_API_KEY 환경 변수가 설정되지 않았습니다.")
        
        return cls(bucket_name=bucket_name, api_key=api_key)


# ============================================================================
# Utilities
# ============================================================================

@contextmanager
def temporary_chdir(destination: str) -> Iterator[None]:
    """임시로 작업 디렉토리를 변경하는 컨텍스트 매니저."""
    original_dir = os.getcwd()
    try:
        os.chdir(destination)
        yield
    finally:
        os.chdir(original_dir)


def is_cloud_run_environment() -> bool:
    """Cloud Run 환경인지 확인합니다."""
    return bool(os.environ.get("K_SERVICE"))


# ============================================================================
# DART Attachment Parser
# ============================================================================

class DartAttachmentParser:
    """DART 첨부파일 파싱을 담당하는 클래스."""
    
    PDF_DOWNLOAD_PATTERN = re.compile(r"openPdfDownload\('(\d+)',\s*'(\d+)'\)")
    
    @classmethod
    def fetch_attachments(cls, rcept_no: str) -> dict[str, str]:
        """공시 번호로부터 첨부파일 목록을 가져옵니다.
        
        Args:
            rcept_no: 접수번호
            
        Returns:
            파일명과 다운로드 URL의 딕셔너리
        """
        main_page_url = f"{DART_BASE_URL}/dsaf001/main.do?rcpNo={rcept_no}"
        
        response = requests.get(main_page_url, headers=DEFAULT_HEADERS, timeout=30)
        response.raise_for_status()
        
        match = cls.PDF_DOWNLOAD_PATTERN.findall(response.text)
        if not match:
            return {}
        
        rcp_no, dcm_no = match[0]
        return cls._parse_download_page(rcp_no, dcm_no)
    
    @classmethod
    def _parse_download_page(cls, rcp_no: str, dcm_no: str) -> dict[str, str]:
        """다운로드 페이지에서 첨부파일 목록을 파싱합니다."""
        download_url = f"http://dart.fss.or.kr/pdf/download/main.do?rcpNo={rcp_no}&dcm_no={dcm_no}"
        
        response = requests.get(download_url, headers=DEFAULT_HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table")
        
        if not isinstance(table, Tag):
            return {}
        
        return cls._extract_files_from_table(table)
    
    @classmethod
    def _extract_files_from_table(cls, table: Tag) -> dict[str, str]:
        """테이블에서 파일 정보를 추출합니다."""
        files: dict[str, str] = {}
        
        tbody = table.find("tbody")
        if not isinstance(tbody, Tag):
            return files
        
        for row in tbody.find_all("tr"):
            if not isinstance(row, Tag):
                continue
            
            file_info = cls._extract_file_from_row(row)
            if file_info:
                filename, url = file_info
                files[filename] = url
        
        return files
    
    @classmethod
    def _extract_file_from_row(cls, row: Tag) -> tuple[str, str] | None:
        """테이블 행에서 파일 정보를 추출합니다."""
        cells = row.find_all("td")
        if len(cells) < 2:
            return None
        
        filename_cell, link_cell = cells[0], cells[1]
        if not (isinstance(filename_cell, Tag) and isinstance(link_cell, Tag)):
            return None
        
        anchor = link_cell.find("a")
        if not isinstance(anchor, Tag):
            return None
        
        href = anchor.get("href")
        if href is None:
            return None
        
        filename = filename_cell.get_text(strip=True)
        url = f"http://dart.fss.or.kr{href}"
        
        return filename, url


# ============================================================================
# Main Crawler
# ============================================================================

class OpenDartCrawler:
    """DART 공시 문서 크롤러.
    
    기업의 공시 문서를 DART에서 크롤링하여 GCS에 업로드합니다.
    """
    
    # 제외할 공시 유형
    EXCLUDED_REPORT_TYPES = frozenset({"기업설명회(IR)개최(안내공시)"})
    
    def __init__(self, company: str = "005930"):
        """크롤러를 초기화합니다.
        
        Args:
            company: 기업 코드 (기본값: 삼성전자)
        """
        self.config = DartConfig.from_env()
        self.company = company
        self.dart = self._initialize_dart_reader()
        self.gcs_manager = gcpmanager.GCSManager()
    
    def _initialize_dart_reader(self) -> OpenDartReader:
        """OpenDartReader를 초기화합니다."""
        if is_cloud_run_environment():
            with temporary_chdir("/tmp"):
                return OpenDartReader(self.config.api_key)  # pyright: ignore[reportCallIssue]
        return OpenDartReader(self.config.api_key)  # pyright: ignore[reportCallIssue]
    
    def crawl_fundamentals(
        self,
        company: str | None = None,
        start_date: str = "2025-01-01",
        end_date: str = "2025-08-19",
        count: int = 5,
    ) -> None:
        """기업의 기본 공시 문서를 크롤링합니다.
        
        Args:
            company: 기업 코드 (None이면 초기화 시 설정된 값 사용)
            start_date: 검색 시작일 (YYYY-MM-DD)
            end_date: 검색 종료일 (YYYY-MM-DD)
            count: 크롤링할 최대 문서 수
        """
        if company:
            self.company = company
        
        company_name = companydict.get_company(self.company)
        folder_path = f"OpenDart/{company_name}/"
        
        reports_to_process = self._get_reports_to_process(
            start_date, end_date, folder_path, count
        )
        
        for rcept_no, flr_nm in reports_to_process:
            self._process_single_report(rcept_no, flr_nm, folder_path)
            time.sleep(REQUEST_DELAY_SECONDS)
    
    def _get_reports_to_process(
        self,
        start_date: str,
        end_date: str,
        folder_path: str,
        count: int,
    ) -> list[tuple[str, str]]:
        """처리할 공시 목록을 가져옵니다."""
        df = self.dart.list(self.company, start=start_date, end=end_date)
        df["report_nm"] = df["report_nm"].str.strip()
        
        # 이미 다운로드된 파일 확인
        existing_files = self.gcs_manager.list_files(folder_name=folder_path)
        existing_rcept_nos = {
            f.split("_")[-1].replace(".pdf", "") for f in existing_files
        }
        
        # 필터링 조건 적용
        is_new = ~df["rcept_no"].isin(existing_rcept_nos)
        is_not_excluded = ~df["report_nm"].isin(self.EXCLUDED_REPORT_TYPES)
        
        filtered_df = df[is_new & is_not_excluded].head(count)
        
        return list(zip(filtered_df["rcept_no"], filtered_df["flr_nm"]))
    
    def _process_single_report(
        self,
        rcept_no: str,
        flr_nm: str,
        folder_path: str,
    ) -> None:
        """단일 공시를 처리합니다."""
        files = self._get_attachment_files(rcept_no)
        
        if not files:
            print(f"첨부파일을 찾지 못했습니다 (접수번호: {rcept_no})")
            return
        
        for filename, url in files.items():
            self._download_and_upload_file(filename, url, rcept_no, folder_path)
    
    def _get_attachment_files(self, rcept_no: str) -> dict[str, str]:
        """첨부파일 목록을 가져옵니다."""
        # OpenDartReader의 내장 메서드 우선 시도
        files = self.dart.attach_files(rcept_no)
        
        if files:
            return files
        
        # 실패 시 직접 파싱 시도
        try:
            return DartAttachmentParser.fetch_attachments(rcept_no)
        except requests.RequestException as e:
            print(f"네트워크 오류 (접수번호: {rcept_no}): {e}")
        except Exception as e:
            print(f"파싱 오류 (접수번호: {rcept_no}): {e}")
        
        return {}
    
    def _download_and_upload_file(
        self,
        filename: str,
        url: str,
        rcept_no: str,
        folder_path: str,
    ) -> None:
        """파일을 다운로드하고 GCS에 업로드합니다."""
        # 파일명 정규화
        if not filename:
            filename = url.split("/")[-1]
        
        # HTML 파일 제외
        if filename.endswith(".html"):
            return
        
        # 접수번호를 파일명에 추가
        destination_name = filename.replace(".pdf", f"_{rcept_no}.pdf")
        
        try:
            response = requests.get(url, stream=True, headers=DEFAULT_HEADERS, timeout=60)
            response.raise_for_status()
            
            self.gcs_manager.upload_file(
                source_file=response.content,
                destination_blob_name=folder_path + destination_name,
            )
        except requests.RequestException as e:
            print(f"파일 다운로드/업로드 실패 ({filename}): {e}")


# ============================================================================
# Backwards Compatibility
# ============================================================================

# 기존 코드와의 호환성을 위한 별칭
OpenDartCrawler.fundamentals = OpenDartCrawler.crawl_fundamentals  # type: ignore[attr-defined]
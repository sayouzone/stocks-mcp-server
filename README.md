# Stocks MCP Server

Stocks Crawling and Caching MCP Server<br/>

주식 정보를 `FnGuide`, `OpenDart`, `Naver Finance`, `Yahoo Finance`, `SEC/EDGAR` 등에서 <br/>
종목 기본 정보 (펀더멘탈, fundamental)를 가져온다.

모델 컨텍스트 프로토콜 (Model Context Protocol, MCP) 서버를 빌도하고 배포<br/>
MCP 서버는 LLM에 외부 도구 및 서비스에 대한 액세스 권한을 제공<br/>
FastMCP를 사용, MCP 서버와 클라이언트를 빌드하는 빠르고 Pythonic한 방법을 제공<br/>

Playwright는 Cloud Run에서 동작할 수 없음으로 Requests 방식을 추가<br/>

- Gemini 2.5 Pro
- Gemini 2.5 Flash
- Gemini 2.5 Flash Lite

#### 참조 문서
- [Cloud Run에 보안 MCP 서버를 배포하는 방법](https://codelabs.developers.google.com/codelabs/cloud-run/how-to-deploy-a-secure-mcp-server-on-cloud-run?hl=ko)
- [Gemini CLI: Custom slash commands](https://cloud.google.com/blog/topics/developers-practitioners/gemini-cli-custom-slash-commands?e=48754805)

## 패키지 구조

- SEC EDGAR 
- FnGuide
- Naver Finance
- OpenDart
- Yahoo Finance

```
├── edgar/
│   ├── __init__.py          # 공개 API 정의
│   ├── client.py            # SEC EDGAR HTTP 클라이언트
│   ├── models.py            # 데이터 클래스 (DTO)
│   ├── utils.py             # 유틸리티 함수 & 상수
│   ├── crawler.py           # 통합 인터페이스 (Facade)
│   ├── examples.py          # 사용 예시
│   └── parsers/
│       ├── __init__.py
│       ├── form_10k.py      # 10-K/10-Q 파서
│       ├── form_8k.py       # 8-K 파서
│       ├── form_13f.py      # 13F 파서
│       └── def14a.py        # DEF 14A 파서
├── examples/
├── fnguide/
│   ├── __init__.py          # 공개 API 정의
│   ├── client.py            # OpenDART HTTP 클라이언트
│   ├── models.py            # 데이터 클래스 (DTO)
│   ├── utils.py             # 유틸리티 함수 & 상수
│   ├── crawler.py           # 통합 인터페이스 (Facade)
│   ├── examples.py          # 사용 예시
│   └── parsers/
│       ├── __init__.py
│       ├── company.py            # FnGuide 기업개요 파서
│       ├── comparison.py         # FnGuide 경쟁사비교 파서
│       ├── consensus.py          # FnGuide 컨센서스 파서
│       ├── dart.py               # FnGuide 금감원공시 파서
│       ├── disclosure.py         # FnGuide 거래소공시 파서
│       ├── finance_ratio.py      # FnGuide 재무비율 파서
│       ├── finance.py            # FnGuide 재무제표 파서
│       ├── industry_analysis.py  # FnGuide 업종분석 파서
│       ├── invest.py             # FnGuide 투자지표 파서
│       ├── main.py               # FnGuide 메인(Snapshot) 파서
│       └── share_analysis.py     # FnGuide 지분분석 파서
├── kisrating/
│   ├── __init__.py          # 공개 API 정의
│   ├── client.py            # OpenDART HTTP 클라이언트
│   ├── models.py            # 데이터 클래스 (DTO)
│   ├── utils.py             # 유틸리티 함수 & 상수
│   ├── crawler.py           # 통합 인터페이스 (Facade)
│   ├── examples.py          # 사용 예시
│   └── parsers/
│   │   ├── __init__.py
│   │   ├── statistics.py    # KisRating 통계 파서
│   │   └── html_extractor.py # KisRating HTML 추출 파서
│   └── utils/
│       ├── storage.py       # KisRating 데이터 저장
│       └── utils.py         # KisRating 유틸리티 함수
├── koreainvestment/
│   ├── __init__.py          # 공개 API 정의
│   ├── client.py            # OpenDART HTTP 클라이언트
│   └── models/              # 데이터 클래스 (DTO)
│   │   ├── __init__.py
│   │   ├── base_model.py    # KisRating 통계 파서
│   │   ├── domestic.py      # 국내주식
│   │   ├── domestic_finance.py      # 국내주식 재무정보
│   │   ├── domestic_ksdinfo.py      # 국내주식 기업정보
│   │   ├── overseas.py      # 해외주식
│   │   └── overseas_finance.py      # 해외주식 재무정보
│   ├── crawler.py           # 통합 인터페이스 (Facade)
│   ├── examples.py          # 사용 예시
│   └── parsers/
│   │   ├── __init__.py
│   │   ├── domestic.py      # 국내주식
│   │   ├── domestic_finance.py      # 국내주식 재무정보
│   │   ├── domestic_ksdinfo.py      # 국내주식 기업정보
│   │   ├── overseas.py      # 해외주식
│   │   └── overseas_finance.py      # 해외주식 재무정보
│   └── utils/
│       ├── storage.py       # KisRating 데이터 저장
│       └── utils.py         # KisRating 유틸리티 함수
├── naver/
│   ├── __init__.py          # 공개 API 정의
│   ├── client.py            # OpenDART HTTP 클라이언트
│   ├── models.py            # 데이터 클래스 (DTO)
│   ├── utils.py             # 유틸리티 함수 & 상수
│   ├── crawler.py           # 통합 인터페이스 (Facade)
│   ├── examples.py          # 사용 예시
│   └── parsers/
│       ├── __init__.py
│       ├── news.py          # Naver News 크롤링 파서
│       └── market.py        # Naver Market API/크롤링 파서
├── opendart/
│   ├── __init__.py          # 공개 API 정의
│   ├── client.py            # OpenDART HTTP 클라이언트
│   ├── models.py            # 데이터 클래스 (DTO)
│   ├── utils.py             # 유틸리티 함수 & 상수
│   ├── crawler.py           # 통합 인터페이스 (Facade)
│   ├── examples.py          # 사용 예시
│   └── parsers/
│       ├── __init__.py
│       ├── document.py        # 문서 API 파서
│       ├── document_viewer.py # 문서 뷰어 API 파서
│       ├── disclosure.py      # 공시정보 API 파서
│       ├── finance.py         # 정기보고서 재무정보 API 파서
│       ├── material_facts.py  # 주요사항보고서 주요정보 API 파서
│       ├── ownership.py       # 지분공시 종합정보 API 파서
│       ├── registration.py    # 증권신고서 주요정보 API 파서
│       └── reports.py         # 정기보고서 주요정보 API 파서
├── tests/
│   ├── test_edgar.py          # Edgar 테스트 (로컬 소스)
│   ├── test_edgar_.py         # Edgar 테스트 (sayou-stock)
│   ├── test_fnguide.py        # FnGuide 테스트 (로컬 소스)
│   ├── test_fnguide_.py       # FnGuide 테스트 (sayou-stock)
│   ├── test_naver.py          # Naver 테스트 (로컬 소스)
│   ├── test_naver_.py         # Naver 테스트 (sayou-stock)
│   ├── test_opendart.py       # OpenDART 테스트 (로컬 소스)
│   ├── test_opendart_.py      # OpenDART 테스트 (sayou-stock)
│   ├── test_yahoo.py          # Yahoo 테스트 (로컬 소스)
│   └── test_yahoo_.py         # Yahoo 테스트 (sayou-stock)
├── yahoo/
│   ├── __init__.py          # 공개 API 정의
│   ├── client.py            # OpenDART HTTP 클라이언트
│   ├── models.py            # 데이터 클래스 (DTO)
│   ├── utils.py             # 유틸리티 함수 & 상수
│   ├── crawler.py           # 통합 인터페이스 (Facade)
│   ├── examples.py          # 사용 예시
│   └── parsers/
│       ├── __init__.py
│       ├── analysis.py      # 분석 API 파서
│       ├── chart.py         # 시세정보 API 파서
│       ├── fundamentals.py  # 재무정보 API 파서
│       ├── holders.py       # 소유주 API 파서
│       ├── market.py        # 시장정보 API 파서
│       ├── news.py          # 뉴스 API 파서
│       ├── options.py       # 옵션 API 파서
│       ├── profile.py       # 프로필 API 파서
│       ├── quote.py         # 기업정보 API 파서
│       ├── statistics.py    # 통계 API 파서
│       └── summary.py       # 요약 API 파서
├── __init__.py
├── .gitignore
├── Dockerfile
├── LICENSE
├── pyproject.toml
├── README.md
├── requirements.txt
└── server.py
```

## 배포 (Cloud Run)

```bash
MCP_SERVER_NAME=stocks-mcp-server
export GOOGLE_CLOUD_PROJECT=sayouzone-ai
```

#### GCP 설정 (1회만)

서비스 활성화

```bash
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com
```

서비스 계정 생성

```bash
gcloud iam service-accounts create mcp-server-sa --display-name="MCP Server Service Account"
```

```bash
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member=user:$(gcloud config get-value account) \
    --role='roles/run.invoker'
```

```bash
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member=serviceAccount:mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --role="roles/secretmanager.secretAccessor"
```

#### 배포

**패키지 소스로 테스트**

```bash
gcloud run deploy $MCP_SERVER_NAME \
    --service-account=mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --no-allow-unauthenticated \
    --region=us-central1 \
    --source=. \
    --labels=dev-tutorial=stocks-mcp
```

**패키지 소스 및 접근권한 테스트**

```bash
gcloud run deploy $MCP_SERVER_NAME \
    --region=us-central1 \
    --source=. \
    --labels=dev-tutorial=stocks-mcp
```

**sayou-stock 설치 및 테스트**

```bash
gcloud run deploy $MCP_SERVER_NAME \
    --service-account=mcp-server-sa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com \
    --no-allow-unauthenticated \
    --region=us-central1 \
    --source=. \
    --set-env-vars="ENV_TYPE=pip" \
    --labels=dev-tutorial=stocks-mcp
```

**sayou-stock 설치 및 접근권한 테스트**

```bash
gcloud run deploy $MCP_SERVER_NAME \
    --region=us-central1 \
    --source=. \
    --set-env-vars="ENV_TYPE=pip" \
    --labels=dev-tutorial=stocks-mcp
```

## Tests

#### Gemini 테스트

```bash
export PROJECT_NUMBER=$(gcloud projects describe $GOOGLE_CLOUD_PROJECT --format="value(projectNumber)")
export ID_TOKEN=$(gcloud auth print-identity-token)
```

settings.json

```json
{
    "ide": {
        "hasSeenNudge": true
    },
    "mcpServers": {
        "stocks-remote": {
            "httpUrl": "https://stocks-mcp-server-$PROJECT_NUMBER.us-central1.run.app/mcp",
            "headers": {
                "Authorization": "Bearer $ID_TOKEN"
            }
        },
        "zoo-remote": {
            "httpUrl": "https://zoo-mcp-server-$PROJECT_NUMBER.us-central1.run.app/mcp",
            "headers": {
                "Authorization": "Bearer $ID_TOKEN"
            }
        }
    },
    "security": {
        "auth": {
            "selectedType": "gemini-api-key"
        }
    }
}
```

Copy settings.json file to ~/.gemini/ directory.

```bash
cp settings.json ~/.gemini/
```

```bash
gemini
```

- 삼성전자
- OpenDart으로 삼성전자 재무제표 보여줘
- OpenDart으로 삼성전자 재무 상태를 보여줘
- OpenDart으로 삼성전자 재무제표 보여줘
- OpenDart으로 삼성전자 배당 정보를 보여줘
- OpenDart으로 삼성전자 배당에 대해 알려줘
- OpenDart으로 삼성전자가 지급하는 보상에 대해 알려줘
- OpenDart으로 2024년 삼성전자 재무제표 보여줘
- OpenDart으로 2025년 3분기 삼성전자 재무제표 보여줘
- FnGuide로 삼성전자 재무제표 보여줘


```bash
Loaded cached credentials.

 ███            █████████  ██████████ ██████   ██████ █████ ██████   █████ █████
░░░███         ███░░░░░███░░███░░░░░█░░██████ ██████ ░░███ ░░██████ ░░███ ░░███
  ░░░███      ███     ░░░  ░███  █ ░  ░███░█████░███  ░███  ░███░███ ░███  ░███
    ░░░███   ░███          ░██████    ░███░░███ ░███  ░███  ░███░░███░███  ░███
     ███░    ░███    █████ ░███░░█    ░███ ░░░  ░███  ░███  ░███ ░░██████  ░███
   ███░      ░░███  ░░███  ░███ ░   █ ░███      ░███  ░███  ░███  ░░█████  ░███
 ███░         ░░█████████  ██████████ █████     █████ █████ █████  ░░█████ █████
░░░            ░░░░░░░░░  ░░░░░░░░░░ ░░░░░     ░░░░░ ░░░░░ ░░░░░    ░░░░░ ░░░░░

Tips for getting started:
1. Ask questions, edit files, or run commands.
2. Be specific for the best results.
3. Create GEMINI.md files to customize your interactions with Gemini.
4. /help for more information.

╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ Gemini CLI update available! 0.14.0 → 0.15.0                                                                            │
│ Installed via Homebrew. Please update with "brew upgrade".                                                              │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯


⠋ Connecting to MCP servers... (1/2)

 Using: 2 MCP servers
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ >   Type your message or @path/to/file                                                                                  │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
 ~/.../src/sayou/mcp/stocks_mcp (main*)                                      no sandbox (see /docs)                                       auto
```

## Deploy sayou-stock

```bash
git push origin main
git tag sayou-stock-v0.1.1
git push origin sayou-stock-v0.1.1 
```

## Errors

```bash
> /mcp

Configured MCP servers:

🟢 stocks-remote - Ready (4 tools)
  Tools:
  - find_fnguide_data
  - find_yahoofinance_data
  - get_yahoofinance_fundamentals
  - save_fundamentals_data_to_gcs

🟢 zoo-remote - Ready (2 tools, 1 prompt)
  Tools:
  - get_animal_details
  - get_animals_by_species
  Prompts:
  - find


ℹ Gemini CLI update available! 0.14.0 → 0.15.0
  Installed via Homebrew. Please update with "brew upgrade".

 Using: 2 MCP servers
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ >   Type your message or @path/to/file                                                                                  │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
 ~/.../src/sayou/mcp/stocks_mcp (main*)                     no sandbox (see /docs)                                     auto
```

```bash
> 삼성전자

ℹ Gemini CLI update available! 0.14.0 → 0.15.0
  Installed via Homebrew. Please update with "brew upgrade".
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ x  find_fnguide_data (stocks-remote MCP Server) {"stock":"삼성전자"}                                                      │
│                                                                                                                         │
│    MCP tool 'find_fnguide_data' reported tool error for function call: {"name":"find_fnguide_data","args":{"stock":"삼성전자"}} with  │
│    response: [{"functionResponse":{"name":"find_fnguide_data","response":{"error":{"content":[{"type":"text","text":"Error calling tool │
│    'find_fnguide_data': BrowserType.launch: Executable doesn't exist at                                                                     │
│    /root/.cache/ms-playwright/chromium_headless_shell-1194/chrome-linux/headless_shell\n╔══════════════════════════════════════════════════ │
│    ══════════╗\n║ Looks like Playwright was just installed or updated.       ║\n║ Please run the following command to download new          │
│    browsers: ║\n║                                                            ║\n║     playwright install                                    │
│    ║\n║                                                            ║\n║ <3 Playwright Team                                                  │
│    ║\n╚════════════════════════════════════════════════════════════╝"}],"isError":true}}}}]                                                 │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ -  Shell playwright install [current working directory /Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/mcp/stocks_mcp] … │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

#### PROJECT_NUMBER가 설정되지 않아서 발생한 오류

```bash
(.venv) (base) seongjungkim@MacBook-Pro-4 stocks_mcp % gemini
Loaded cached credentials.

 ███            █████████  ██████████ ██████   ██████ █████ ██████   █████ █████
░░░███         ███░░░░░███░░███░░░░░█░░██████ ██████ ░░███ ░░██████ ░░███ ░░███
  ░░░███      ███     ░░░  ░███  █ ░  ░███░█████░███  ░███  ░███░███ ░███  ░███
    ░░░███   ░███          ░██████    ░███░░███ ░███  ░███  ░███░░███░███  ░███
     ███░    ░███    █████ ░███░░█    ░███ ░░░  ░███  ░███  ░███ ░░██████  ░███
   ███░      ░░███  ░░███  ░███ ░   █ ░███      ░███  ░███  ░███  ░░█████  ░███
 ███░         ░░█████████  ██████████ █████     █████ █████ █████  ░░█████ █████
░░░            ░░░░░░░░░  ░░░░░░░░░░ ░░░░░     ░░░░░ ░░░░░ ░░░░░    ░░░░░ ░░░░░

Tips for getting started:
1. Ask questions, edit files, or run commands.
2. Be specific for the best results.
3. /help for more information.

✕ Error during discovery for server 'stocks-remote': Connection failed for 'stocks-remote': Error POSTing to endpoint (HTTP
  400): <!DOCTYPE html><html lang=en> <meta charset=utf-8> <meta name=viewport content="initial-scale=1, minimum-scale=1,
  width=device-width"> <title>Error 400 (Bad Request)!!1</title> <style> *{margin:0;padding:0}html,code{font:15px/22px
  arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto
  0;max-width:390px;min-height:180px;padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100%
  5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a
  img{border:0}@media screen and
  (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/imag
  es/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;margin-left:-5px}@media only screen and
  (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png)
  no-repeat 0% 0%/100% 100%;-moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png)
  0}}@media only screen and
  (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54
  dp.png) no-repeat;-webkit-background-size:100% 100%}}#logo{display:inline-block;height:54px;width:150px} </style> <a
  href=//www.google.com/><span id=logo aria-label=Google></span></a> <p><b>400.</b> <ins>That’s an error.</ins> <p>Your client
  has issued a malformed or illegal request. <ins>That’s all we know.</ins>

✕ Error during discovery for server 'zoo-remote': Connection failed for 'zoo-remote': Error POSTing to endpoint (HTTP 400):
  <!DOCTYPE html><html lang=en> <meta charset=utf-8> <meta name=viewport content="initial-scale=1, minimum-scale=1,
  width=device-width"> <title>Error 400 (Bad Request)!!1</title> <style> *{margin:0;padding:0}html,code{font:15px/22px
  arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto
  0;max-width:390px;min-height:180px;padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100%
  5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a
  img{border:0}@media screen and
  (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/imag
  es/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;margin-left:-5px}@media only screen and
  (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png)
  no-repeat 0% 0%/100% 100%;-moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png)
  0}}@media only screen and
  (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54
  dp.png) no-repeat;-webkit-background-size:100% 100%}}#logo{display:inline-block;height:54px;width:150px} </style> <a
  href=//www.google.com/><span id=logo aria-label=Google></span></a> <p><b>400.</b> <ins>That’s an error.</ins> <p>Your client
  has issued a malformed or illegal request. <ins>That’s all we know.</ins>


ℹ Gemini CLI update available! 0.14.0 → 0.18.4
  Installed via Homebrew. Please update with "brew upgrade".
```

```bash
> 삼성전자

╭──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ ✓  find_fnguide_data (stocks-remote MCP Server) {"stock":"삼성전자"}                                                                     │
│                                                                                                                                          │
│ ... first 456 lines hidden ...                                                                                                           │
│ \"영업활동으로인한자산부채변동(운전자본변동) / 영업활동으로인한자산부채변동(운전자본변동)계산에 참여한 계정 펼치기\": \"-15,676\",       │
│ \"영업활동으로인한자산부채변동(운전자본변동) / 자산의감소(증가)\": \"-9,666\", \"영업활동으로인한자산부채변동(운전자본변동) /            │
│ 부채의증가(감소)\": \"-4,329\", \"영업활동으로인한자산부채변동(운전자본변동) / 정부보조금등의변동\": \"\",                               │
│ \"영업활동으로인한자산부채변동(운전자본변동) / 기타운전자본의변동\": \"-1,680\", \"영업활동으로인한자산부채변동(운전자본변동) /          │
│ *영업에서창출된현금흐름\": \"758,309\", \"기타영업활동으로인한현금흐름 / 기타영업활동으로인한현금흐름계산에 참여한 계정 펼치기\":        │
│ \"-28,483\", \"기타영업활동으로인한현금흐름 / 이자수입\": \"40,084\", \"기타영업활동으로인한현금흐름 / 이자지급(-)\": \"-6,750\",        │
│ \"기타영업활동으로인한현금흐름 / 배당금수입\": \"2,685\", \"기타영업활동으로인한현금흐름 / 배당금지급(-)\": \"\",                        │
│ \"기타영업활동으로인한현금흐름 / 법인세환입\": \"\", \"기타영업활동으로인한현금흐름 / 법인세납부(-)\": \"-64,500\",                      │
│ \"기타영업활동으로인한현금흐름 / 중단영업관련현금흐름\": \"\", \"기타영업활동으로인한현금흐름 / 투자활동으로인한현금흐름\":              │
│ \"-853,817\", \"투자활동으로인한현금유입액 / 투자활동으로인한현금유입액계산에 참여한 계정 펼치기\": \"57,444\",                          │
│ \"투자활동으로인한현금유입액 / 유동금융자산의감소\": \"13,205\", \"투자활동으로인한현금유입액 / 장기금융상품의감소\": \"41,000\",        │
│ \"투자활동으로인한현금유입액 / 매도가능금융자산의감소\": \"\", \"투자활동으로인한현금유입액 / 만기보유금융자산의감소\": \"\",            │
│ \"투자활동으로인한현금유입액 / 장기대여금의감소\": \"\", \"투자활동으로인한현금유입액 / 파생상품의변동\": \"\",                          │
│ \"투자활동으로인한현금유입액 / 기타비유동금융자산의감소\": \"\", \"투자활동으로인한현금유입액 / 관계기업등지분관련투자자산의감소\":      │
│ \"332\", \"투자활동으로인한현금유입액 / 유형자산의감소\": \"1,562\", \"투자활동으로인한현금유입액 / 무형자산의감소\": \"159\",           │
│ \"투자활동으로인한현금유입액 / 생물자산의감소\": \"\", \"투자활동으로인한현금유입액 / 투자부동산의감소\": \"\",                          │
│ \"투자활동으로인한현금유입액 / 기타투자활동으로인한현금유입액\": \"1,187\", \"(투자활동으로인한현금유출액) /                             │
│ (투자활동으로인한현금유출액)계산에 참여한 계정 펼치기\": \"911,261\", \"(투자활동으로인한현금유출액) / 유동금융자산의증가\":             │
│ \"332,433\", \"(투자활동으로인한현금유출액) / 장기금융상품의증가\": \"39,873\", \"(투자활동으로인한현금유출액) /                         │
│ 매도가능금융자산의증가\": \"\", \"(투자활동으로인한현금유출액) / 만기보유금융자산의증가\": \"\", \"(투자활동으로인한현금유출액) /        │
│ 장기대여금의증가\": \"\", \"(투자활동으로인한현금유출액) / 파생상품의변동\": \"\", \"(투자활동으로인한현금유출액) /                      │
│ 기타비유동금융자산의증가\": \"\", \"(투자활동으로인한현금유출액) / 관계기업등지분관련투자자산의증가\": \"117\",                          │
│ \"(투자활동으로인한현금유출액) / 유형자산의증가\": \"514,064\", \"(투자활동으로인한현금유출액) / 무형자산의증가\": \"23,353\",           │
│ \"(투자활동으로인한현금유출액) / 생물자산의증가\": \"\", \"(투자활동으로인한현금유출액) / 투자부동산의증가\": \"\",                      │
│ \"(투자활동으로인한현금유출액) / 기타투자활동으로인한현금유출액\": \"1,422\", \"기타투자활동으로인한현금흐름 /                           │
│ 기타투자활동으로인한현금흐름계산에 참여한 계정 펼치기\": \"\", \"기타투자활동으로인한현금흐름 / 이자수입\": \"\",                        │
│ \"기타투자활동으로인한현금흐름 / 이자지급(-)\": \"\", \"기타투자활동으로인한현금흐름 / 배당금수입\": \"\",                               │
│ \"기타투자활동으로인한현금흐름 / 배당금지급(-)\": \"\", \"기타투자활동으로인한현금흐름 / 법인세환입\": \"\",                             │
│ \"기타투자활동으로인한현금흐름 / 법인세납부(-)\": \"\", \"기타투자활동으로인한현금흐름 / 중단영업관련현금흐름\": \"\",                   │
│ \"기타투자활동으로인한현금흐름 / 재무활동으로인한현금흐름\": \"-77,972\", \"재무활동으로인한현금유입액 /                                 │
│ 재무활동으로인한현금유입액계산에 참여한 계정 펼치기\": \"62,763\", \"재무활동으로인한현금유입액 / 사채의증가\": \"\",                    │
│ \"재무활동으로인한현금유입액 / 차입금의증가\": \"62,763\", \"재무활동으로인한현금유입액 / 미지급금의증가\": \"\",                        │
│ \"재무활동으로인한현금유입액 / 유동성장기부채의증가\": \"\", \"재무활동으로인한현금유입액 / 기타금융부채의증가\": \"\",                  │
│ \"재무활동으로인한현금유입액 / 기타부채의증가\": \"\", \"재무활동으로인한현금유입액 / 유상증자\": \"\", \"재무활동으로인한현금유입액 /   │
│ 자기주식의처분\": \"\", \"재무활동으로인한현금유입액 / 주식매입선택권의행사\": \"\", \"재무활동으로인한현금유입액 /                      │
│ 자본구성항목의증가\": \"\", \"재무활동으로인한현금유입액 / 기타재무활동으로인한현금유입액\": \"\", \"(재무활동으로인한현금유출액) /      │
│ (재무활동으로인한현금유출액)계산에 참여한 계정 펼치기\": \"31,848\", \"(재무활동으로인한현금유출액) / 사채의감소\": \"13,645\",          │
│ \"(재무활동으로인한현금유출액) / 차입금의감소\": \"\", \"(재무활동으로인한현금유출액) / 미지급금의감소\": \"\",                          │
│ \"(재무활동으로인한현금유출액) / 유동성장기부채의감소\": \"\", \"(재무활동으로인한현금유출액) / 기타금융부채의감소\": \"\",              │
│ \"(재무활동으로인한현금유출액) / 기타부채의감소\": \"\", \"(재무활동으로인한현금유출액) / 유상감자\": \"\",                              │
│ \"(재무활동으로인한현금유출액) / 자기주식의취득\": \"18,118\", \"(재무활동으로인한현금유출액) / 자본구성항목의감소\": \"85\",            │
│ \"(재무활동으로인한현금유출액) / 기타재무활동으로인한현금유출액\": \"\", \"기타재무활동으로인한현금흐름 /                                │
│ 기타재무활동으로인한현금흐름계산에 참여한 계정 펼치기\": \"-108,887\", \"기타재무활동으로인한현금흐름 / 이자수입\": \"\",                │
│ \"기타재무활동으로인한현금흐름 / 이자지급(-)\": \"\", \"기타재무활동으로인한현금흐름 / 배당금수입\": \"\",                               │
│ \"기타재무활동으로인한현금흐름 / 배당금지급(-)\": \"-108,887\", \"기타재무활동으로인한현금흐름 / 법인세환입\": \"\",                     │
│ \"기타재무활동으로인한현금흐름 / 법인세납부(-)\": \"\", \"기타재무활동으로인한현금흐름 / 중단영업관련현금흐름\": \"\",                   │
│ \"기타재무활동으로인한현금흐름 / 영업투자재무활동기타현금흐름\": \"\", \"기타재무활동으로인한현금흐름 /                                  │
│ 연결범위변동으로인한현금의증가\": \"\", \"기타재무활동으로인한현금흐름 / 환율변동효과\": \"48,210\", \"기타재무활동으로인한현금흐름 /    │
│ 현금및현금성자산의증가\": \"-153,753\", \"기타재무활동으로인한현금흐름 / 기초현금및현금성자산\": \"690,809\",                            │
│ \"기타재무활동으로인한현금흐름 / 기말현금및현금성자산\": \"537,056\"}, {\"period\": \"2025/09\", \"영업활동으로인한현금흐름\":           │
│ \"565,155\", \"당기순손익\": \"255,651\", \"법인세비용차감전계속사업이익\": \"\", \"현금유출이없는비용등가산 /                           │
│ 현금유출이없는비용등가산계산에 참여한 계정 펼치기\": \"440,320\", \"현금유출이없는비용등가산 / 퇴직급여\": \"12,028\",                   │
│ \"현금유출이없는비용등가산 / 종업원급여\": \"\", \"현금유출이없는비용등가산 / 주식보상비\": \"\", \"현금유출이없는비용등가산 /           │
│ 대손상각비\": \"432\", \"현금유출이없는비용등가산 / 감가상각비\": \"322,489\", \"현금유출이없는비용등가산 / 무형자산상각비\":            │
│ \"24,457\", \"현금유출이없는비용등가산 / 계약부채전입액\": \"\", \"현금유출이없는비용등가산 / 반품(환불)부채전입액\": \"\",              │
│ \"현금유출이없는비용등가산 / 배출부채전입액\": \"\", \"현금유출이없는비용등가산 / 충당부채전입액\": \"\", \"현금유출이없는비용등가산 /   │
│ 외환손실\": \"\", \"현금유출이없는비용등가산 / 기타의대손상각비\": \"\", \"현금유출이없는비용등가산 / 금융원가\": \"20,968\",            │
│ \"현금유출이없는비용등가산 / 이자비용\": \"\", \"현금유출이없는비용등가산 / 배당금지급\": \"\", \"현금유출이없는비용등가산 /             │
│ 매출채권처분손실\": \"\", \"현금유출이없는비용등가산 / 당기손익-공정가치측정 금융자산관련손실\": \"\", \"현금유출이없는비용등가산 /      │
│ 금융자산처분손실\": \"\", \"현금유출이없는비용등가산 / 금융자산평가손실\": \"\", \"현금유출이없는비용등가산 / 금융자산손상차손\": \"\",  │
│ \"현금유출이없는비용등가산 / 파생상품손실\": \"\", \"현금유출이없는비용등가산 / 금융부채관련손실\": \"\", \"현금유출이없는비용등가산 /   │
│ 사채상환손실\": \"\", \"현금유출이없는비용등가산 / 재고자산감모손실\": \"\", \"현금유출이없는비용등가산 / 재고자산폐기(처분)손실\":      │
│ \"\", \"현금유출이없는비용등가산 / 자산처분(폐기)손실\": \"392\", \"현금유출이없는비용등가산 / 투자자산평가손실\": \"\",                 │
│ \"현금유출이없는비용등가산 / 자산재평가손실\": \"\", \"현금유출이없는비용등가산 / 자산손상차손\": \"29,341\", \"현금유출이없는비용등가산 │
│ / 지분법관련손실\": \"\", \"현금유출이없는비용등가산 / 종속기업관련손실\": \"\", \"현금유출이없는비용등가산 / 관계기업관련손익\": \"\",  │
│ \"현금유출이없는비용등가산 / 법인세비용\": \"28,882\", \"현금유출이없는비용등가산 / 기타비용\": \"1,330\", \"(현금유입이없는수익등차감)  │
│ / (현금유입이없는수익등차감)계산에 참여한 계정 펼치기\": \"61,101\", \"(현금유입이없는수익등차감) / 외환이익\": \"\",                    │
│ \"(현금유입이없는수익등차감) / 대손충당금환입액\": \"\", \"(현금유입이없는수익등차감) / 금융수익\": \"53,728\",                          │
│ \"(현금유입이없는수익등차감) / 이자수익\": \"\", \"(현금유입이없는수익등차감) / 배당금수익\": \"936\", \"(현금유입이없는수익등차감) /    │
│ 매출채권처분이익\": \"\", \"(현금유입이없는수익등차감) / 당기손익-공정가치측정 금융자산관련이익\": \"\", \"(현금유입이없는수익등차감) /  │
│ 금융자산처분이익\": \"\", \"(현금유입이없는수익등차감) / 금융자산평가이익\": \"\", \"(현금유입이없는수익등차감) /                        │
│ 금융자산손상차손환입\": \"\", \"(현금유입이없는수익등차감) / 파생상품이익\": \"\", \"(현금유입이없는수익등차감) / 금융부채관련이익\":    │
│ \"\", \"(현금유입이없는수익등차감) / 사채상환이익\": \"\", \"(현금유입이없는수익등차감) / 퇴직급여충당부채환입액\": \"\",                │
│ \"(현금유입이없는수익등차감) / 계약부채환입액\": \"\", \"(현금유입이없는수익등차감) / 반품(환불)부채환입액\": \"\",                      │
│ \"(현금유입이없는수익등차감) / 배출부채환입액\": \"\", \"(현금유입이없는수익등차감) / 충당부채환입액\": \"\",                            │
│ \"(현금유입이없는수익등차감) / 주식보상비환입\": \"\", \"(현금유입이없는수익등차감) / 재고자산감모손실환입\": \"\",                      │
│ \"(현금유입이없는수익등차감) / 재고자산폐기(처분)이익\": \"\", \"(현금유입이없는수익등차감) / 자산처분(폐기)이익\": \"854\",             │
│ \"(현금유입이없는수익등차감) / 투자자산평가이익\": \"\", \"(현금유입이없는수익등차감) / 자산재평가이익\": \"\",                          │
│ \"(현금유입이없는수익등차감) / 자산손상차손환입\": \"\", \"(현금유입이없는수익등차감) / 지분법관련이익\": \"5,584\",                     │
│ \"(현금유입이없는수익등차감) / 종속회사관련이익\": \"\", \"(현금유입이없는수익등차감) / 관계기업관련손익\": \"\",                        │
│ \"(현금유입이없는수익등차감) / 법인세수익\": \"\", \"(현금유입이없는수익등차감) / 기타수익\": \"\",                                      │
│ \"영업활동으로인한자산부채변동(운전자본변동) / 영업활동으로인한자산부채변동(운전자본변동)계산에 참여한 계정 펼치기\": \"-54,665\",       │
│ \"영업활동으로인한자산부채변동(운전자본변동) / 자산의감소(증가)\": \"-65,037\", \"영업활동으로인한자산부채변동(운전자본변동) /           │
│ 부채의증가(감소)\": \"-11,576\", \"영업활동으로인한자산부채변동(운전자본변동) / 정부보조금등의변동\": \"\",                              │
│ \"영업활동으로인한자산부채변동(운전자본변동) / 기타운전자본의변동\": \"21,948\", \"영업활동으로인한자산부채변동(운전자본변동) /          │
│ *영업에서창출된현금흐름\": \"580,204\", \"기타영업활동으로인한현금흐름 / 기타영업활동으로인한현금흐름계산에 참여한 계정 펼치기\":        │
│ \"-15,049\", \"기타영업활동으로인한현금흐름 / 이자수입\": \"35,803\", \"기타영업활동으로인한현금흐름 / 이자지급(-)\": \"-3,441\",        │
│ \"기타영업활동으로인한현금흐름 / 배당금수입\": \"2,239\", \"기타영업활동으로인한현금흐름 / 배당금지급(-)\": \"\",                        │
│ \"기타영업활동으로인한현금흐름 / 법인세환입\": \"\", \"기타영업활동으로인한현금흐름 / 법인세납부(-)\": \"-49,651\",                      │
│ \"기타영업활동으로인한현금흐름 / 중단영업관련현금흐름\": \"\", \"기타영업활동으로인한현금흐름 / 투자활동으로인한현금흐름\":              │
│ \"-375,212\", \"투자활동으로인한현금유입액 / 투자활동으로인한현금유입액계산에 참여한 계정 펼치기\": \"81,041\",                          │
│ \"투자활동으로인한현금유입액 / 유동금융자산의감소\": \"72,175\", \"투자활동으로인한현금유입액 / 장기금융상품의감소\": \"6,987\",         │
│ \"투자활동으로인한현금유입액 / 매도가능금융자산의감소\": \"\", \"투자활동으로인한현금유입액 / 만기보유금융자산의감소\": \"\",            │
│ \"투자활동으로인한현금유입액 / 장기대여금의감소\": \"\", \"투자활동으로인한현금유입액 / 파생상품의변동\": \"\",                          │
│ \"투자활동으로인한현금유입액 / 기타비유동금융자산의감소\": \"\", \"투자활동으로인한현금유입액 / 관계기업등지분관련투자자산의감소\":      │
│ \"41\", \"투자활동으로인한현금유입액 / 유형자산의감소\": \"1,702\", \"투자활동으로인한현금유입액 / 무형자산의감소\": \"136\",            │
│ \"투자활동으로인한현금유입액 / 생물자산의감소\": \"\", \"투자활동으로인한현금유입액 / 투자부동산의감소\": \"\",                          │
│ \"투자활동으로인한현금유입액 / 기타투자활동으로인한현금유입액\": \"\", \"(투자활동으로인한현금유출액) /                                  │
│ (투자활동으로인한현금유출액)계산에 참여한 계정 펼치기\": \"456,253\", \"(투자활동으로인한현금유출액) / 유동금융자산의증가\": \"2,369\",  │
│ \"(투자활동으로인한현금유출액) / 장기금융상품의증가\": \"47,732\", \"(투자활동으로인한현금유출액) / 매도가능금융자산의증가\": \"\",      │
│ \"(투자활동으로인한현금유출액) / 만기보유금융자산의증가\": \"\", \"(투자활동으로인한현금유출액) / 장기대여금의증가\": \"\",              │
│ \"(투자활동으로인한현금유출액) / 파생상품의변동\": \"\", \"(투자활동으로인한현금유출액) / 기타비유동금융자산의증가\": \"\",              │
│ \"(투자활동으로인한현금유출액) / 관계기업등지분관련투자자산의증가\": \"3,251\", \"(투자활동으로인한현금유출액) / 유형자산의증가\":       │
│ \"359,729\", \"(투자활동으로인한현금유출액) / 무형자산의증가\": \"34,208\", \"(투자활동으로인한현금유출액) / 생물자산의증가\": \"\",     │
│ \"(투자활동으로인한현금유출액) / 투자부동산의증가\": \"\", \"(투자활동으로인한현금유출액) / 기타투자활동으로인한현금유출액\": \"8,963\", │
│ \"기타투자활동으로인한현금흐름 / 기타투자활동으로인한현금흐름계산에 참여한 계정 펼치기\": \"\", \"기타투자활동으로인한현금흐름 /         │
│ 이자수입\": \"\", \"기타투자활동으로인한현금흐름 / 이자지급(-)\": \"\", \"기타투자활동으로인한현금흐름 / 배당금수입\": \"\",             │
│ \"기타투자활동으로인한현금흐름 / 배당금지급(-)\": \"\", \"기타투자활동으로인한현금흐름 / 법인세환입\": \"\",                             │
│ \"기타투자활동으로인한현금흐름 / 법인세납부(-)\": \"\", \"기타투자활동으로인한현금흐름 / 중단영업관련현금흐름\": \"\",                   │
│ \"기타투자활동으로인한현금흐름 / 재무활동으로인한현금흐름\": \"-189,361\", \"재무활동으로인한현금유입액 /                                │
│ 재무활동으로인한현금유입액계산에 참여한 계정 펼치기\": \"20,037\", \"재무활동으로인한현금유입액 / 사채의증가\": \"\",                    │
│ \"재무활동으로인한현금유입액 / 차입금의증가\": \"20,037\", \"재무활동으로인한현금유입액 / 미지급금의증가\": \"\",                        │
│ \"재무활동으로인한현금유입액 / 유동성장기부채의증가\": \"\", \"재무활동으로인한현금유입액 / 기타금융부채의증가\": \"\",                  │
│ \"재무활동으로인한현금유입액 / 기타부채의증가\": \"\", \"재무활동으로인한현금유입액 / 유상증자\": \"\", \"재무활동으로인한현금유입액 /   │
│ 자기주식의처분\": \"\", \"재무활동으로인한현금유입액 / 주식매입선택권의행사\": \"\", \"재무활동으로인한현금유입액 /                      │
│ 자본구성항목의증가\": \"\", \"재무활동으로인한현금유입액 / 기타재무활동으로인한현금유입액\": \"\", \"(재무활동으로인한현금유출액) /      │
│ (재무활동으로인한현금유출액)계산에 참여한 계정 펼치기\": \"134,961\", \"(재무활동으로인한현금유출액) / 사채의감소\": \"\",               │
│ \"(재무활동으로인한현금유출액) / 차입금의감소\": \"53,444\", \"(재무활동으로인한현금유출액) / 미지급금의감소\": \"\",                    │
│ \"(재무활동으로인한현금유출액) / 유동성장기부채의감소\": \"\", \"(재무활동으로인한현금유출액) / 기타금융부채의감소\": \"\",              │
│ \"(재무활동으로인한현금유출액) / 기타부채의감소\": \"\", \"(재무활동으로인한현금유출액) / 유상감자\": \"\",                              │
│ \"(재무활동으로인한현금유출액) / 자기주식의취득\": \"81,493\", \"(재무활동으로인한현금유출액) / 자본구성항목의감소\": \"24\",            │
│ \"(재무활동으로인한현금유출액) / 기타재무활동으로인한현금유출액\": \"\", \"기타재무활동으로인한현금흐름 /                                │
│ 기타재무활동으로인한현금흐름계산에 참여한 계정 펼치기\": \"-74,438\", \"기타재무활동으로인한현금흐름 / 이자수입\": \"\",                 │
│ \"기타재무활동으로인한현금흐름 / 이자지급(-)\": \"\", \"기타재무활동으로인한현금흐름 / 배당금수입\": \"\",                               │
│ \"기타재무활동으로인한현금흐름 / 배당금지급(-)\": \"-74,438\", \"기타재무활동으로인한현금흐름 / 법인세환입\": \"\",                      │
│ \"기타재무활동으로인한현금흐름 / 법인세납부(-)\": \"\", \"기타재무활동으로인한현금흐름 / 중단영업관련현금흐름\": \"\",                   │
│ \"기타재무활동으로인한현금흐름 / 영업투자재무활동기타현금흐름\": \"\", \"기타재무활동으로인한현금흐름 /                                  │
│ 연결범위변동으로인한현금의증가\": \"\", \"기타재무활동으로인한현금흐름 / 환율변동효과\": \"-3,643\", \"기타재무활동으로인한현금흐름 /    │
│ 현금및현금성자산의증가\": \"-3,061\", \"기타재무활동으로인한현금흐름 / 기초현금및현금성자산\": \"537,056\",                              │
│ \"기타재무활동으로인한현금흐름 / 기말현금및현금성자산\": \"533,995\"}]"}                                                                 │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
✦ Here is the financial data for Samsung Electronics:

  Balance Sheet (2024/12)
   * 자산 (Assets): 5,145,319 (in 억 KRW)
   * 부채 (Liabilities): 1,123,399 (in 억 KRW)
   * 자본 (Equity): 4,021,921 (in 억 KRW)

  Income Statement (2024/12)
   * 매출액 (Revenue): 3,008,709 (in 억 KRW)
   * 영업이익 (Operating Income): 327,260 (in 억 KRW)
   * 당기순이익 (Net Income): 344,514 (in 억 KRW)

  Cash Flow Statement (2024/12)
   * 영업활동으로인한현금흐름 (Cash Flow from Operating Activities): 729,826 (in 억 KRW)
   * 투자활동으로인한현금흐름 (Cash Flow from Investing Activities): -853,817 (in 억 KRW)
   * 재무활동으로인한현금흐름 (Cash Flow from Financing Activities): -77,972 (in 억 KRW)

  Would you like to see more detailed information for a specific period or category?
```

```bash
> Apple

╭──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ ✓  get_yahoofinance_fundamentals (stocks-remote MCP Server) {"query":"Apple","use_cache":true}                                           │
│                                                                                                                                          │
│ ... first 133 lines hidden ...                                                                                                           │
│ Interest Income Expense\":null,\"Interest Expense Non Operating\":null,\"Interest Income Non Operating\":null,\"Operating                │
│ Income\":123216000000.0,\"Operating Expense\":57467000000.0,\"Research And Development\":31370000000.0,\"Selling General And             │
│ Administration\":26097000000.0,\"Gross Profit\":180683000000.0,\"Cost Of Revenue\":210352000000.0,\"Total                                │
│ Revenue\":391035000000.0,\"Operating Revenue\":391035000000.0},\"2023-09-30T00:00:00.000\":{\"Tax Effect Of Unusual Items\":0.0,\"Tax    │
│ Rate For Calcs\":0.147,\"Normalized EBITDA\":125820000000.0,\"Net Income From Continuing Operation Net Minority                          │
│ Interest\":96995000000.0,\"Reconciled Depreciation\":11519000000.0,\"Reconciled Cost Of                                                  │
│ Revenue\":214137000000.0,\"EBITDA\":125820000000.0,\"EBIT\":114301000000.0,\"Net Interest Income\":-183000000.0,\"Interest               │
│ Expense\":3933000000.0,\"Interest Income\":3750000000.0,\"Normalized Income\":96995000000.0,\"Net Income From Continuing And             │
│ Discontinued Operation\":96995000000.0,\"Total Expenses\":268984000000.0,\"Total Operating Income As Reported\":114301000000.0,\"Diluted │
│ Average Shares\":15812547000.0,\"Basic Average Shares\":15744231000.0,\"Diluted EPS\":6.13,\"Basic EPS\":6.16,\"Diluted NI Availto Com   │
│ Stockholders\":96995000000.0,\"Net Income Common Stockholders\":96995000000.0,\"Net Income\":96995000000.0,\"Net Income Including        │
│ Noncontrolling Interests\":96995000000.0,\"Net Income Continuous Operations\":96995000000.0,\"Tax Provision\":16741000000.0,\"Pretax     │
│ Income\":113736000000.0,\"Other Income Expense\":-565000000.0,\"Other Non Operating Income Expenses\":-565000000.0,\"Net Non Operating   │
│ Interest Income Expense\":-183000000.0,\"Interest Expense Non Operating\":3933000000.0,\"Interest Income Non                             │
│ Operating\":3750000000.0,\"Operating Income\":114301000000.0,\"Operating Expense\":54847000000.0,\"Research And                          │
│ Development\":29915000000.0,\"Selling General And Administration\":24932000000.0,\"Gross Profit\":169148000000.0,\"Cost Of               │
│ Revenue\":214137000000.0,\"Total Revenue\":383285000000.0,\"Operating Revenue\":383285000000.0},\"2022-09-30T00:00:00.000\":{\"Tax       │
│ Effect Of Unusual Items\":0.0,\"Tax Rate For Calcs\":0.162,\"Normalized EBITDA\":130541000000.0,\"Net Income From Continuing Operation   │
│ Net Minority Interest\":99803000000.0,\"Reconciled Depreciation\":11104000000.0,\"Reconciled Cost Of                                     │
│ Revenue\":223546000000.0,\"EBITDA\":130541000000.0,\"EBIT\":119437000000.0,\"Net Interest Income\":-106000000.0,\"Interest               │
│ Expense\":2931000000.0,\"Interest Income\":2825000000.0,\"Normalized Income\":99803000000.0,\"Net Income From Continuing And             │
│ Discontinued Operation\":99803000000.0,\"Total Expenses\":274891000000.0,\"Total Operating Income As Reported\":119437000000.0,\"Diluted │
│ Average Shares\":16325819000.0,\"Basic Average Shares\":16215963000.0,\"Diluted EPS\":6.11,\"Basic EPS\":6.15,\"Diluted NI Availto Com   │
│ Stockholders\":99803000000.0,\"Net Income Common Stockholders\":99803000000.0,\"Net Income\":99803000000.0,\"Net Income Including        │
│ Noncontrolling Interests\":99803000000.0,\"Net Income Continuous Operations\":99803000000.0,\"Tax Provision\":19300000000.0,\"Pretax     │
│ Income\":119103000000.0,\"Other Income Expense\":-334000000.0,\"Other Non Operating Income Expenses\":-334000000.0,\"Net Non Operating   │
│ Interest Income Expense\":-106000000.0,\"Interest Expense Non Operating\":2931000000.0,\"Interest Income Non                             │
│ Operating\":2825000000.0,\"Operating Income\":119437000000.0,\"Operating Expense\":51345000000.0,\"Research And                          │
│ Development\":26251000000.0,\"Selling General And Administration\":25094000000.0,\"Gross Profit\":170782000000.0,\"Cost Of               │
│ Revenue\":223546000000.0,\"Total Revenue\":394328000000.0,\"Operating Revenue\":394328000000.0},\"2021-09-30T00:00:00.000\":{\"Tax       │
│ Effect Of Unusual Items\":null,\"Tax Rate For Calcs\":null,\"Normalized EBITDA\":null,\"Net Income From Continuing Operation Net         │
│ Minority Interest\":null,\"Reconciled Depreciation\":null,\"Reconciled Cost Of Revenue\":null,\"EBITDA\":null,\"EBIT\":null,\"Net        │
│ Interest Income\":198000000.0,\"Interest Expense\":2645000000.0,\"Interest Income\":2843000000.0,\"Normalized Income\":null,\"Net Income │
│ From Continuing And Discontinued Operation\":null,\"Total Expenses\":null,\"Total Operating Income As Reported\":null,\"Diluted Average  │
│ Shares\":null,\"Basic Average Shares\":null,\"Diluted EPS\":null,\"Basic EPS\":null,\"Diluted NI Availto Com Stockholders\":null,\"Net   │
│ Income Common Stockholders\":null,\"Net Income\":null,\"Net Income Including Noncontrolling Interests\":null,\"Net Income Continuous     │
│ Operations\":null,\"Tax Provision\":null,\"Pretax Income\":null,\"Other Income Expense\":null,\"Other Non Operating Income               │
│ Expenses\":null,\"Net Non Operating Interest Income Expense\":198000000.0,\"Interest Expense Non Operating\":2645000000.0,\"Interest     │
│ Income Non Operating\":2843000000.0,\"Operating Income\":null,\"Operating Expense\":null,\"Research And Development\":null,\"Selling     │
│ General And Administration\":null,\"Gross Profit\":null,\"Cost Of Revenue\":null,\"Total Revenue\":null,\"Operating                      │
│ Revenue\":null}}","cash_flow":"{\"2025-09-30T00:00:00.000\":{\"Free Cash Flow\":98767000000.0,\"Repurchase Of Capital                    │
│ Stock\":-90711000000.0,\"Repayment Of Debt\":-10932000000.0,\"Issuance Of Debt\":4481000000.0,\"Issuance Of Capital                      │
│ Stock\":null,\"Capital Expenditure\":-12715000000.0,\"Interest Paid Supplemental Data\":null,\"Income Tax Paid Supplemental              │
│ Data\":43369000000.0,\"End Cash Position\":35934000000.0,\"Beginning Cash Position\":29943000000.0,\"Changes In                          │
│ Cash\":5991000000.0,\"Financing Cash Flow\":-120686000000.0,\"Cash Flow From Continuing Financing Activities\":-120686000000.0,\"Net     │
│ Other Financing Charges\":-6071000000.0,\"Cash Dividends Paid\":-15421000000.0,\"Common Stock Dividend Paid\":-15421000000.0,\"Net       │
│ Common Stock Issuance\":-90711000000.0,\"Common Stock Payments\":-90711000000.0,\"Common Stock Issuance\":null,\"Net Issuance Payments   │
│ Of Debt\":-8483000000.0,\"Net Short Term Debt Issuance\":-2032000000.0,\"Net Long Term Debt Issuance\":-6451000000.0,\"Long Term Debt    │
│ Payments\":-10932000000.0,\"Long Term Debt Issuance\":4481000000.0,\"Investing Cash Flow\":15195000000.0,\"Cash Flow From Continuing     │
│ Investing Activities\":15195000000.0,\"Net Other Investing Changes\":-1480000000.0,\"Net Investment Purchase And                         │
│ Sale\":29390000000.0,\"Sale Of Investment\":53797000000.0,\"Purchase Of Investment\":-24407000000.0,\"Net Business Purchase And          │
│ Sale\":null,\"Purchase Of Business\":null,\"Net PPE Purchase And Sale\":-12715000000.0,\"Purchase Of PPE\":-12715000000.0,\"Operating    │
│ Cash Flow\":111482000000.0,\"Cash Flow From Continuing Operating Activities\":111482000000.0,\"Change In Working                         │
│ Capital\":-25000000000.0,\"Change In Other Working Capital\":null,\"Change In Other Current Liabilities\":-11076000000.0,\"Change In     │
│ Other Current Assets\":-9197000000.0,\"Change In Payables And Accrued Expense\":902000000.0,\"Change In Payable\":902000000.0,\"Change   │
│ In Account Payable\":902000000.0,\"Change In Inventory\":1400000000.0,\"Change In Receivables\":-7029000000.0,\"Changes In Account       │
│ Receivables\":-6682000000.0,\"Other Non Cash Items\":-89000000.0,\"Stock Based Compensation\":12863000000.0,\"Deferred                   │
│ Tax\":null,\"Deferred Income Tax\":null,\"Depreciation Amortization Depletion\":11698000000.0,\"Depreciation And                         │
│ Amortization\":11698000000.0,\"Net Income From Continuing Operations\":112010000000.0},\"2024-09-30T00:00:00.000\":{\"Free Cash          │
│ Flow\":108807000000.0,\"Repurchase Of Capital Stock\":-94949000000.0,\"Repayment Of Debt\":-9958000000.0,\"Issuance Of                   │
│ Debt\":0.0,\"Issuance Of Capital Stock\":null,\"Capital Expenditure\":-9447000000.0,\"Interest Paid Supplemental Data\":null,\"Income    │
│ Tax Paid Supplemental Data\":26102000000.0,\"End Cash Position\":29943000000.0,\"Beginning Cash Position\":30737000000.0,\"Changes In    │
│ Cash\":-794000000.0,\"Financing Cash Flow\":-121983000000.0,\"Cash Flow From Continuing Financing Activities\":-121983000000.0,\"Net     │
│ Other Financing Charges\":-5802000000.0,\"Cash Dividends Paid\":-15234000000.0,\"Common Stock Dividend Paid\":-15234000000.0,\"Net       │
│ Common Stock Issuance\":-94949000000.0,\"Common Stock Payments\":-94949000000.0,\"Common Stock Issuance\":null,\"Net Issuance Payments   │
│ Of Debt\":-5998000000.0,\"Net Short Term Debt Issuance\":3960000000.0,\"Net Long Term Debt Issuance\":-9958000000.0,\"Long Term Debt     │
│ Payments\":-9958000000.0,\"Long Term Debt Issuance\":0.0,\"Investing Cash Flow\":2935000000.0,\"Cash Flow From Continuing Investing      │
│ Activities\":2935000000.0,\"Net Other Investing Changes\":-1308000000.0,\"Net Investment Purchase And Sale\":13690000000.0,\"Sale Of     │
│ Investment\":62346000000.0,\"Purchase Of Investment\":-48656000000.0,\"Net Business Purchase And Sale\":null,\"Purchase Of               │
│ Business\":null,\"Net PPE Purchase And Sale\":-9447000000.0,\"Purchase Of PPE\":-9447000000.0,\"Operating Cash                           │
│ Flow\":118254000000.0,\"Cash Flow From Continuing Operating Activities\":118254000000.0,\"Change In Working                              │
│ Capital\":3651000000.0,\"Change In Other Working Capital\":null,\"Change In Other Current Liabilities\":15552000000.0,\"Change In Other  │
│ Current Assets\":-11731000000.0,\"Change In Payables And Accrued Expense\":6020000000.0,\"Change In Payable\":6020000000.0,\"Change In   │
│ Account Payable\":6020000000.0,\"Change In Inventory\":-1046000000.0,\"Change In Receivables\":-5144000000.0,\"Changes In Account        │
│ Receivables\":-3788000000.0,\"Other Non Cash Items\":-2266000000.0,\"Stock Based Compensation\":11688000000.0,\"Deferred                 │
│ Tax\":null,\"Deferred Income Tax\":null,\"Depreciation Amortization Depletion\":11445000000.0,\"Depreciation And                         │
│ Amortization\":11445000000.0,\"Net Income From Continuing Operations\":93736000000.0},\"2023-09-30T00:00:00.000\":{\"Free Cash           │
│ Flow\":99584000000.0,\"Repurchase Of Capital Stock\":-77550000000.0,\"Repayment Of Debt\":-11151000000.0,\"Issuance Of                   │
│ Debt\":5228000000.0,\"Issuance Of Capital Stock\":null,\"Capital Expenditure\":-10959000000.0,\"Interest Paid Supplemental               │
│ Data\":3803000000.0,\"Income Tax Paid Supplemental Data\":18679000000.0,\"End Cash Position\":30737000000.0,\"Beginning Cash             │
│ Position\":24977000000.0,\"Changes In Cash\":5760000000.0,\"Financing Cash Flow\":-108488000000.0,\"Cash Flow From Continuing Financing  │
│ Activities\":-108488000000.0,\"Net Other Financing Charges\":-6012000000.0,\"Cash Dividends Paid\":-15025000000.0,\"Common Stock         │
│ Dividend Paid\":-15025000000.0,\"Net Common Stock Issuance\":-77550000000.0,\"Common Stock Payments\":-77550000000.0,\"Common Stock      │
│ Issuance\":null,\"Net Issuance Payments Of Debt\":-9901000000.0,\"Net Short Term Debt Issuance\":-3978000000.0,\"Net Long Term Debt      │
│ Issuance\":-5923000000.0,\"Long Term Debt Payments\":-11151000000.0,\"Long Term Debt Issuance\":5228000000.0,\"Investing Cash            │
│ Flow\":3705000000.0,\"Cash Flow From Continuing Investing Activities\":3705000000.0,\"Net Other Investing Changes\":-1337000000.0,\"Net  │
│ Investment Purchase And Sale\":16001000000.0,\"Sale Of Investment\":45514000000.0,\"Purchase Of Investment\":-29513000000.0,\"Net        │
│ Business Purchase And Sale\":null,\"Purchase Of Business\":null,\"Net PPE Purchase And Sale\":-10959000000.0,\"Purchase Of               │
│ PPE\":-10959000000.0,\"Operating Cash Flow\":110543000000.0,\"Cash Flow From Continuing Operating Activities\":110543000000.0,\"Change   │
│ In Working Capital\":-6577000000.0,\"Change In Other Working Capital\":null,\"Change In Other Current                                    │
│ Liabilities\":3031000000.0,\"Change In Other Current Assets\":-5684000000.0,\"Change In Payables And Accrued                             │
│ Expense\":-1889000000.0,\"Change In Payable\":-1889000000.0,\"Change In Account Payable\":-1889000000.0,\"Change In                      │
│ Inventory\":-1618000000.0,\"Change In Receivables\":-417000000.0,\"Changes In Account Receivables\":-1688000000.0,\"Other Non Cash       │
│ Items\":-2227000000.0,\"Stock Based Compensation\":10833000000.0,\"Deferred Tax\":null,\"Deferred Income Tax\":null,\"Depreciation       │
│ Amortization Depletion\":11519000000.0,\"Depreciation And Amortization\":11519000000.0,\"Net Income From Continuing                      │
│ Operations\":96995000000.0},\"2022-09-30T00:00:00.000\":{\"Free Cash Flow\":111443000000.0,\"Repurchase Of Capital                       │
│ Stock\":-89402000000.0,\"Repayment Of Debt\":-9543000000.0,\"Issuance Of Debt\":5465000000.0,\"Issuance Of Capital                       │
│ Stock\":null,\"Capital Expenditure\":-10708000000.0,\"Interest Paid Supplemental Data\":2865000000.0,\"Income Tax Paid Supplemental      │
│ Data\":19573000000.0,\"End Cash Position\":24977000000.0,\"Beginning Cash Position\":35929000000.0,\"Changes In                          │
│ Cash\":-10952000000.0,\"Financing Cash Flow\":-110749000000.0,\"Cash Flow From Continuing Financing Activities\":-110749000000.0,\"Net   │
│ Other Financing Charges\":-6383000000.0,\"Cash Dividends Paid\":-14841000000.0,\"Common Stock Dividend Paid\":-14841000000.0,\"Net       │
│ Common Stock Issuance\":-89402000000.0,\"Common Stock Payments\":-89402000000.0,\"Common Stock Issuance\":null,\"Net Issuance Payments   │
│ Of Debt\":-123000000.0,\"Net Short Term Debt Issuance\":3955000000.0,\"Net Long Term Debt Issuance\":-4078000000.0,\"Long Term Debt      │
│ Payments\":-9543000000.0,\"Long Term Debt Issuance\":5465000000.0,\"Investing Cash Flow\":-22354000000.0,\"Cash Flow From Continuing     │
│ Investing Activities\":-22354000000.0,\"Net Other Investing Changes\":-2086000000.0,\"Net Investment Purchase And                        │
│ Sale\":-9560000000.0,\"Sale Of Investment\":67363000000.0,\"Purchase Of Investment\":-76923000000.0,\"Net Business Purchase And          │
│ Sale\":-306000000.0,\"Purchase Of Business\":-306000000.0,\"Net PPE Purchase And Sale\":-10708000000.0,\"Purchase Of                     │
│ PPE\":-10708000000.0,\"Operating Cash Flow\":122151000000.0,\"Cash Flow From Continuing Operating Activities\":122151000000.0,\"Change   │
│ In Working Capital\":1200000000.0,\"Change In Other Working Capital\":478000000.0,\"Change In Other Current                              │
│ Liabilities\":6110000000.0,\"Change In Other Current Assets\":-6499000000.0,\"Change In Payables And Accrued                             │
│ Expense\":9448000000.0,\"Change In Payable\":9448000000.0,\"Change In Account Payable\":9448000000.0,\"Change In                         │
│ Inventory\":1484000000.0,\"Change In Receivables\":-9343000000.0,\"Changes In Account Receivables\":-1823000000.0,\"Other Non Cash       │
│ Items\":1006000000.0,\"Stock Based Compensation\":9038000000.0,\"Deferred Tax\":895000000.0,\"Deferred Income                            │
│ Tax\":895000000.0,\"Depreciation Amortization Depletion\":11104000000.0,\"Depreciation And Amortization\":11104000000.0,\"Net Income     │
│ From Continuing Operations\":99803000000.0},\"2021-09-30T00:00:00.000\":{\"Free Cash Flow\":null,\"Repurchase Of Capital                 │
│ Stock\":null,\"Repayment Of Debt\":null,\"Issuance Of Debt\":null,\"Issuance Of Capital Stock\":1105000000.0,\"Capital                   │
│ Expenditure\":null,\"Interest Paid Supplemental Data\":2687000000.0,\"Income Tax Paid Supplemental Data\":null,\"End Cash                │
│ Position\":null,\"Beginning Cash Position\":null,\"Changes In Cash\":null,\"Financing Cash Flow\":null,\"Cash Flow From Continuing       │
│ Financing Activities\":null,\"Net Other Financing Charges\":null,\"Cash Dividends Paid\":null,\"Common Stock Dividend Paid\":null,\"Net  │
│ Common Stock Issuance\":null,\"Common Stock Payments\":null,\"Common Stock Issuance\":1105000000.0,\"Net Issuance Payments Of            │
│ Debt\":null,\"Net Short Term Debt Issuance\":null,\"Net Long Term Debt Issuance\":null,\"Long Term Debt Payments\":null,\"Long Term Debt │
│ Issuance\":null,\"Investing Cash Flow\":null,\"Cash Flow From Continuing Investing Activities\":null,\"Net Other Investing               │
│ Changes\":null,\"Net Investment Purchase And Sale\":null,\"Sale Of Investment\":null,\"Purchase Of Investment\":null,\"Net Business      │
│ Purchase And Sale\":-33000000.0,\"Purchase Of Business\":-33000000.0,\"Net PPE Purchase And Sale\":null,\"Purchase Of                    │
│ PPE\":null,\"Operating Cash Flow\":null,\"Cash Flow From Continuing Operating Activities\":null,\"Change In Working                      │
│ Capital\":null,\"Change In Other Working Capital\":1676000000.0,\"Change In Other Current Liabilities\":null,\"Change In Other Current   │
│ Assets\":null,\"Change In Payables And Accrued Expense\":null,\"Change In Payable\":null,\"Change In Account Payable\":null,\"Change In  │
│ Inventory\":null,\"Change In Receivables\":null,\"Changes In Account Receivables\":null,\"Other Non Cash Items\":null,\"Stock Based      │
│ Compensation\":null,\"Deferred Tax\":-4774000000.0,\"Deferred Income Tax\":-4774000000.0,\"Depreciation Amortization                     │
│ Depletion\":null,\"Depreciation And Amortization\":null,\"Net Income From Continuing Operations\":null}}"}                               │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
✦ Here is the financial data for Apple (AAPL) as of 2025년 9월 30일:

  Balance Sheet
   * Total Assets: 359,241,000,000 USD
   * Total Liabilities: 285,508,000,000 USD
   * Stockholders Equity: 73,733,000,000 USD

  Income Statement
   * Total Revenue: 416,161,000,000 USD
   * Operating Income: 133,050,000,000 USD
   * Net Income: 112,010,000,000 USD

  Cash Flow Statement
   * Operating Cash Flow: 111,482,000,000 USD
   * Investing Cash Flow: 15,195,000,000 USD
   * Financing Cash Flow: -120,686,000,000 USD

  Would you like to see more detailed information for a specific period or category?
```

```bash
> OpenDart으로 삼성전자 재무제표 보여줘

╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ ✓  find_opendart_data (stocks-remote MCP Server) {"stock":"삼성전자"}                                                     │
│                                                                                                                         │
│ Error calling tool 'find_opendart_data': 'DartDisclosureParser' object has no attribute '_DartDisclosureParser__save_zip_path' │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ ✓  ReadFolder opendart                                                                                                  │
│                                                                                                                         │
│ Listed 9 item(s).                                                                                                       │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## References

- [Cloud Run에서 MCP 서버를 사용하는 ADK 에이전트 빌드 및 배포](https://codelabs.developers.google.com/codelabs/cloud-run/use-mcp-server-on-cloud-run-with-an-adk-agent?hl=ko)
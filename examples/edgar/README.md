# 

1. Downloads the indices.
  download_indices
  https://www.sec.gov/Archives/edgar/full-index//2025/QTR1/master.zip

2. Gets specific indices according to the provided filing types and CIKs/tickers.
  get_specific_indices
  https://www.sec.gov/files/company_tickers.json

3. Compares the new indices with the old ones to download only the new filings.
  pd.read_csv

4. Crawls through each index to download (.tsv files) and save the filing.
  1. crawl
  https://www.sec.gov/Archives/edgar/data/320193/0000320193-25-000008-index.html
  https://www.sec.gov/cgi-bin/browse-edgar?CIK=320193
  2. download
  https://www.sec.gov/Archives/edgar/data/320193/000032019325000077/aapl-20251030.htm

##

```bash
Saving log to /Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/logs

Downloading index files from SEC...
Downloading 2025_QTR1.tsv from https://www.sec.gov/Archives/edgar/full-index//2025/QTR1/master.zip
Writing /Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/datasets/INDICES/2025_QTR1.tsv
2025_QTR1.tsv downloaded
Downloading 2025_QTR2.tsv from https://www.sec.gov/Archives/edgar/full-index//2025/QTR2/master.zip
Writing /Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/datasets/INDICES/2025_QTR2.tsv
2025_QTR2.tsv downloaded
Downloading 2025_QTR3.tsv from https://www.sec.gov/Archives/edgar/full-index//2025/QTR3/master.zip
Writing /Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/datasets/INDICES/2025_QTR3.tsv
2025_QTR3.tsv downloaded
Downloading 2025_QTR4.tsv from https://www.sec.gov/Archives/edgar/full-index//2025/QTR4/master.zip
Writing /Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/datasets/INDICES/2025_QTR4.tsv
2025_QTR4.tsv downloaded

Reading filings metadata...

100%|█████████████████████████████████████████████████████████████| 69/69 [00:00<00:00, 5724.71it/s]

There are no more filings to download for the given years, quarters and companies
```

Extract master.zip files from SEC and save master.idx files in temporary folder


```bash
  0%|                                                                        | 0/13 [00:00<?, ?it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000023-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000023-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
  8%|████▉                                                           | 1/13 [00:00<00:07,  1.62it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000007-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000007-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 15%|█████████▊                                                      | 2/13 [00:01<00:05,  1.94it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000021-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000021-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 23%|██████████████▊                                                 | 3/13 [00:01<00:04,  2.05it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000039-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000039-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 31%|███████████████████▋                                            | 4/13 [00:01<00:04,  2.13it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000116-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000116-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 38%|████████████████████████▌                                       | 5/13 [00:02<00:03,  2.47it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000082-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000082-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 46%|█████████████████████████████▌                                  | 6/13 [00:02<00:02,  2.39it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000115-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000115-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 54%|██████████████████████████████████▍                             | 7/13 [00:03<00:02,  2.33it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000209-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000209-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 62%|███████████████████████████████████████▍                        | 8/13 [00:03<00:01,  2.61it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000179-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000179-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 69%|████████████████████████████████████████████▎                   | 9/13 [00:03<00:01,  2.46it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000197-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000197-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 77%|████████████████████████████████████████████████▍              | 10/13 [00:04<00:01,  2.71it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000207-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000207-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 85%|█████████████████████████████████████████████████████▎         | 11/13 [00:04<00:00,  2.96it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000230-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000230-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
 92%|██████████████████████████████████████████████████████████▏    | 12/13 [00:04<00:00,  3.06it/s]
Downloading html file from https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000228-index.html
https://www.sec.gov/Archives/edgar/data/1045810/0001045810-25-000228-index.html에서 html file 다운로드
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:542: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  series["Filing Date"] = form.nextSibling.nextSibling.text
/Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/download_filings.py:545: DeprecationWarning: Access to deprecated property nextSibling. (Replaced by next_sibling) -- Deprecated since version 4.0.0.
  period_of_report = form.nextSibling.nextSibling.text
100%|███████████████████████████████████████████████████████████████| 13/13 [00:04<00:00,  2.69it/s]

Filings metadata exported to /Users/seongjungkim/Development/sayouzone/base-framework/src/sayou/crawlers/edgar/datasets/FILINGS_METADATA.csv
```
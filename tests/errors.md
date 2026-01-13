```bash
Traceback (most recent call last):
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/connectionpool.py", line 787, in urlopen
    response = self._make_request(
               ^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/connectionpool.py", line 534, in _make_request
    response = conn.getresponse()
               ^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/connection.py", line 565, in getresponse
    httplib_response = super().getresponse()
                       ^^^^^^^^^^^^^^^^^^^^^
  File "/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/http/client.py", line 1374, in getresponse
    response.begin()
  File "/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/http/client.py", line 318, in begin
    version, status, reason = self._read_status()
                              ^^^^^^^^^^^^^^^^^^^
  File "/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/http/client.py", line 287, in _read_status
    raise RemoteDisconnected("Remote end closed connection without"
http.client.RemoteDisconnected: Remote end closed connection without response

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/requests/adapters.py", line 644, in send
    resp = conn.urlopen(
           ^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/connectionpool.py", line 841, in urlopen
    retries = retries.increment(
              ^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/util/retry.py", line 474, in increment
    raise reraise(type(error), error, _stacktrace)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/util/util.py", line 38, in reraise
    raise value.with_traceback(tb)
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/connectionpool.py", line 787, in urlopen
    response = self._make_request(
               ^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/connectionpool.py", line 534, in _make_request
    response = conn.getresponse()
               ^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/urllib3/connection.py", line 565, in getresponse
    httplib_response = super().getresponse()
                       ^^^^^^^^^^^^^^^^^^^^^
  File "/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/http/client.py", line 1374, in getresponse
    response.begin()
  File "/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/http/client.py", line 318, in begin
    version, status, reason = self._read_status()
                              ^^^^^^^^^^^^^^^^^^^
  File "/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/http/client.py", line 287, in _read_status
    raise RemoteDisconnected("Remote end closed connection without"
urllib3.exceptions.ProtocolError: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/seongjungkim/Development/sayouzone/mcp-stocks-server/tests/tests_koreainvestment_trading.py", line 122, in <module>
    main()
  File "/Users/seongjungkim/Development/sayouzone/mcp-stocks-server/tests/tests_koreainvestment_trading.py", line 114, in main
    demo_overseas_trading(crawler)
  File "/Users/seongjungkim/Development/sayouzone/mcp-stocks-server/tests/tests_koreainvestment_trading.py", line 76, in demo_overseas_trading
    data = crawler.sell_stock_overseas(info[0], info[1], info[2], info[3])
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/mcp-stocks-server/koreainvestment/crawler.py", line 94, in sell_stock_overseas
    return self._overseas_trading_parser.sell(stock_code, order_quantity, order_price, exchange_type)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/mcp-stocks-server/koreainvestment/parsers/overseas_trading.py", line 101, in sell
    return self._order(
           ^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/mcp-stocks-server/koreainvestment/parsers/overseas_trading.py", line 189, in _order
    response = self._client._post(
               ^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/mcp-stocks-server/koreainvestment/client.py", line 86, in _post
    response = self.session.post(url, json=json, headers=headers, timeout=timeout)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/requests/sessions.py", line 637, in post
    return self.request("POST", url, data=data, json=json, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/requests/sessions.py", line 589, in request
    resp = self.send(prep, **send_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/requests/sessions.py", line 703, in send
    r = adapter.send(request, **kwargs)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/seongjungkim/Development/sayouzone/.venv/lib/python3.11/site-packages/requests/adapters.py", line 659, in send
    raise ConnectionError(err, request=request)
requests.exceptions.ConnectionError: ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))
```
"""
bmex.py

A script to download and store historical data (bars + quotes + trades) from BitMEX.

Copyright (c) 2019, Diogo Flores.
License: MIT
"""

import argparse
import csv
from datetime import datetime as dt
from datetime import timedelta
from dateutil.parser import parse
import gzip
import os
import requests
import sys
import time


# https://www.bitmex.com/api/explorer/
bars_endpoint = "https://www.bitmex.com/api/v1/trade/bucketed?binSize={}&partial=false&symbol={}&count=500&start=0&reverse=false&startTime={}&endTime={}"

# https://public.bitmex.com/?prefix=data/trade/
quotes_trades_endpoint = (
    "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/{}/{}.csv.gz"
)

bars_header = [
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "trades",
    "volume",
    "vwap",
    "lastSize",
    "turnover",
    "homeNotional",
    "foreignNotional",
]

quotes_header = ["timestamp", "symbol", "bidSize", "bidPrice", "askPrice", "askSize"]

trades_header = [
    "timestamp",
    "symbol",
    "side",
    "size",
    "price",
    "tickDirection",
    "trdMatchID",
    "grossValue",
    "homeNotional",
    "foreignNotional",
]


def _validate_dates(start: dt, end: dt):
    """
    Validates start and end dates prior to polling data from BitMEX servers.
    """

    # Earliest date of data available.
    min_date = dt(2014, 11, 22)
    today = dt.today()

    if start < min_date:
        sys.exit(f"\nError: Start-date can't be earlier than {min_date.date()}\n")

    if end < start:
        sys.exit("\nError: End-date can't be earlier than start-date.\n")

    if end > today:
        end = today

    return start, end


def _validate_symbols(symbols: set):
    """
    Validates that each symbol/index exists/existed on BitMEX.
    """

    r = requests.get(
        "https://www.bitmex.com/api/v1/instrument?count=500&reverse=false"
    ).json()

    valid = [x["symbol"] for x in r]
    not_valid = [symb for symb in symbols if symb not in valid]

    if not_valid:
        sys.exit(f"\nError: Not valid symbol(s): {not_valid}.\n")

    return symbols


def _make_dirs(symbols: set, save_to: str = None):
    """
    Creates a base directory and one sub-directory for each symbol, to be
    populated with historical data.
    """

    base = "BITMEX"
    path = save_to

    if path:
        if not os.path.exists(path):
            sys.exit("\nError: The path you provided does not exist.\n")
    else:
        path = os.getcwd()

    if not os.path.isdir(f"{path}/{base}"):
        try:
            os.mkdir(f"{path}/{base}")
        except PermissionError:
            sys.exit(f"\nError: You don't have permissions to write on {path}\n")

    for sym in symbols:
        if not os.path.isdir(f"{path}/{base}/{sym}"):
            os.mkdir(f"{path}/{base}/{sym}")

    return base, path


def _unzip_quotes_trades(temp: str, r):
    """
    Unzip downloaded .tar.gz file and parse the data inside.
    """
    with open(temp, "wb") as fp:
        fp.write(r.content)

    with gzip.open(temp, "rb") as fp:
        data = fp.read()

    with open(temp, "wb") as fp:
        fp.write(data)


def _store_quotes_trades(start: str, symbols: set, channel: str, path: str, base: str):
    """
    Stores the data as .csv files on a pre-defined (see README.md) directory structure.
    """
    temp = start.strftime("%Y%m%d")  # Saves passing 'temp' as an argument.
    new = True
    header = {symbol: True for symbol in symbols}

    with open(temp, newline="") as inp:
        reader = csv.reader(inp)
        for row in reader:
            # Pandas couldn't parse the dates - The next line fixes that.
            row[0] = row[0].replace("D", "T", 1)
            if row[1] in symbols:
                symbol = row[1]
                location = (
                    f"{path}/{base}/{symbol}/{channel}s/{start.year}/{start.month}"
                )

                if not os.path.isdir(location):
                    os.makedirs(location)

                _file = f"{location}/{temp[:4]}-{temp[4:6]}-{temp[6:]}.csv"

                if new:
                    # If the file already exists, remove it before creating a new one
                    # and start appending to it.
                    # This is a safety measure to ensure data integrity, in case the
                    # program is run with the same start and end dates multiple times.
                    if os.path.exists(_file):
                        os.remove(_file)
                    new = False

                with open(_file, "a", newline="") as out:
                    write = csv.writer(out)
                    if header[symbol]:
                        h = trades_header if channel == "trade" else quotes_header
                        write.writerow(h)
                        header[symbol] = False
                    write.writerow(row)
    os.remove(temp)


def poll_quotes_trades(
    start: dt, end: dt, symbols: set, channel: str, base: str, path: str
):
    """
    Polls data in daily blocks from BitMEX servers.
    """

    print("-" * 80)
    print(f"Start processing {channel}s:\n")
    while start <= end:
        # BitMEX names each zipped file by its date in the format below.
        # We must download it as such and then unzip it and extract the data,
        # hence the "temp" variable name.
        temp = start.strftime("%Y%m%d")
        count = 0
        while True:
            r = requests.get(quotes_trades_endpoint.format(channel, temp))
            if r.status_code == 200:
                break
            else:
                count += 1
                if count == 10:
                    if r.status_code == 404:
                        # Data for today or yesterday might yet not be available.
                        today = dt.today()
                        yesterday = today - timedelta(1)
                        if (
                            start.date() == today.date()
                            or start.date() == yesterday.date()
                        ):
                            return f"Failed to download: {start.date()} - data not (yet) available."
                    r.raise_for_status()
                print(f"{r.status_code} error processing: {start.date()} - retrying.")
                time.sleep(10)

        _unzip_quotes_trades(temp, r)
        _store_quotes_trades(start, symbols, channel, path, base)

        print(f"Processed {channel}s: {str(start)[:10]}")
        start += timedelta(days=1)

    return "Success - all data downloaded and stored."


def _store_bars(
    data: list,
    end: dt,
    path: str,
    base: str,
    symbol: str,
    channel: str,
    bar: str,
    header: dt,
):
    """
    Stores the data as .csv files on a pre-defined (see README.md) directory structure.

    Note: On the first time this function is called, 'header' will be of type None,
    subsequently it will be of type datetime.
    """
    for row in data:
        date = parse(row["timestamp"]).date()
        if date > end.date():
            return

        location = f"{path}/{base}/{symbol}/{channel}/{bar}/{date.year}/{date.month}"
        if not os.path.isdir(location):
            os.makedirs(location)

        d = date.strftime("%Y%m%d")
        _file = f"{location}/{d[:4]}-{d[4:6]}-{d[6:]}.csv"

        new = True if date != header else False
        if new:
            # If the file already exists, remove it before creating a new one
            # and start appending to it.
            # This is a safety measure to ensure data integrity, in case the
            # program is run with the same start and end dates multiple times.
            if os.path.exists(_file):
                os.remove(_file)

        with open(_file, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=bars_header)
            # "header" starts as None and after the 2nd iteration, keeps memory
            # of the last "header" (date) used - if different then it writes
            # a bars_header to the new csv file.
            if header != date:
                writer.writeheader()
                header = date
            writer.writerow(row)
    return header


def poll_bars(
    start: dt, end: dt, symbols: set, channel: str, bars: list, base: str, path: str
):
    """
    Polls bars (time buckets of trades) from BitMEX servers.
    Options are: [1m, 5m, 1h, 1d]
    """

    req = 0
    header = None
    error = False

    print("-" * 80)
    print(f"Start processing {channel}:\n")
    for symbol in symbols:
        for bar in bars:
            # Can't modify start/end directly since they will be used multiple times.
            st = start
            nd = end

            while st < (nd + timedelta(days=1)):
                if error:
                    print("Success: continuing.")
                error = False
                req += 1

                # 30 is the maximum number of requests allowed per minute.
                if req % 30 == 0:
                    print(f"Sleeping for 60 seconds.")
                    time.sleep(60)
                    req = 0

                r = requests.get(
                    bars_endpoint.format(
                        bar, symbol, str(st), str(nd + timedelta(days=1))
                    )
                )

                # BitMEX API often throws a "429 - too many requests" error, even
                # if we are respecting its limits.
                if r.status_code == 429:
                    error = True
                    print(
                        f"Error processing: {st.date()} - Sleeping for 60 seconds and retrying."
                    )
                    time.sleep(60)
                    continue

                data = r.json()

                if not data:
                    print(f"Data does not exist {symbol}-{bar}: {st.date()}")
                    st += timedelta(days=1)
                    time.sleep(1)
                    continue

                stored = _store_bars(data, nd, path, base, symbol, channel, bar, header)
                if stored:
                    header = stored

                print(f"Processed {symbol} {bar}-bars: {st.date()}")

                # 500 is the maximum number of results per request.
                if bar == "1m":
                    st += timedelta(minutes=500)
                elif bar == "5m":
                    st += timedelta(minutes=500 * 5)
                elif bar == "1h":
                    st += timedelta(hours=500)
                else:
                    st += timedelta(days=500)

    return "Success - all data downloaded and stored."


def _transform_validate(args):
    """
    Transforms and/or validates the arguments passed to the main function.
    """

    start = dt.strptime(args.start, "%Y-%m-%d")
    end = dt.strptime(args.end, "%Y-%m-%d")
    bars = args.bars
    save_to = args.save_to

    # Remove possible duplicates.
    symbols = set(args.symbols)
    channels = set(args.channels)

    if "bars" in channels and not bars:
        sys.exit(
            """\nIf the channel 'bars' is enabled, you must provide at least one time frame.
                Options: [1m, 5m, 1h, 1d]\n"""
        )
    if bars and "bars" not in channels:
        sys.exit("\nTime frames provided but channel 'bars' is not enabled.\n")

    # Validate symbols and dates.
    start, end = _validate_dates(start, end)
    symbols = _validate_symbols(symbols)

    return bars, channels, end, save_to, start, symbols


def main(args):

    bars, channels, end, save_to, start, symbols = _transform_validate(args)
    base, path = _make_dirs(symbols, save_to)

    report = {}
    if "trades" in channels:
        report["trades"] = poll_quotes_trades(start, end, symbols, "trade", base, path)
    if "quotes" in channels:
        report["quotes"] = poll_quotes_trades(start, end, symbols, "quote", base, path)
    if "bars" in channels:
        report["bars"] = poll_bars(start, end, symbols, "bars", bars, base, path)

    print("-" * 80)
    print("Finished.\n")
    print("Report")
    print("------")
    for k, v in report.items():
        print(f"{k}: {v}")
    print()


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Download and store BitMEX historical data."
    )
    parser.add_argument(
        "--symbols", nargs="+", required=True, help="Symbols/indices to download."
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        required=True,
        choices=["bars", "quotes", "trades"],
        help="Choose between 'bars', 'quotes' or 'trades' channel. Both are allowed.",
    )
    parser.add_argument(
        "--bars",
        nargs="+",
        choices=["1m", "5m", "1h", "1d"],
        help="Time bars to download.",
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="From when to retrieve data. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="Until when to retrieve data. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--save_to",
        type=str,
        help="Provide a full path for where to store the retrieved data. (optional)",
    )

    arguments = parser.parse_args()
    return arguments


if __name__ == "__main__":
    arguments = parse_arguments()
    main(arguments)

"""
bmex.py

A script to download and store historical data (bars, quotes and trades) from BitMEX.

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
_headers = {
    "bars": [
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
    ],
    "quote": ["timestamp", "symbol", "bidSize", "bidPrice", "askPrice", "askSize"],
    "trade": [
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
    ],
}


def _validate_dates(start: dt, end: dt):
    """
    Validates start and end dates prior to polling data from BitMEX servers.
    """

    # Earliest date of data available.
    min_date = dt(2014, 11, 22)
    today = dt.today()

    if start < min_date:
        sys.exit(f"\nERROR: Start-date can't be earlier than {min_date.date()}\n")

    if end < start:
        sys.exit("\nERROR: End-date can't be earlier than start-date.\n")

    if end > today:
        end = today

    return start, end


def _validate_symbols(symbols: set):
    """
    Validates that each symbol/index exists/existed on BitMEX.
    """

    response = requests.get(
        "https://www.bitmex.com/api/v1/instrument?count=500&reverse=false"
    ).json()

    valid = [field["symbol"] for field in response]
    not_valid = [symbol for symbol in symbols if symbol not in valid]

    if not_valid:
        sys.exit(f"\nERROR: These symbols are not valid: {not_valid}.\n")

    return symbols


def _validate_path(save_to: str = None):
    """
    Creates a directory ('BITMEX') if the path is valid, otherwise it exists the program
    with an error message.
    """

    base = "BITMEX"
    path = save_to

    if path:
        if not os.path.exists(path):
            sys.exit("\nERROR: The path you provided does not exist.\n")
    else:
        path = os.getcwd()

    path = f"{path}/{base}"
    if not os.path.isdir(f"{path}"):
        try:
            os.mkdir(f"{path}")
        except PermissionError:
            sys.exit(f"\nERROR: You don't have permissions to write on {path}\n")

    return path


def _unzip_quotes_trades(temp: str, response: dict):
    """
    Unzip downloaded .tar.gz file and parse the data inside.
    """

    with open(temp, "wb") as fp:
        fp.write(response.content)

    with gzip.open(temp, "rb") as fp:
        data = fp.read()

    with open(temp, "wb") as fp:
        fp.write(data)


def _delete_old(_file: str):
    """
    If the '_file' already exists, remove it and create a new one to which to append the
    data to.
    This is a safety measure to ensure data integrity, in case the program is run with
    the same start and end dates multiple times.
    """

    if os.path.exists(_file):
        os.remove(_file)


def _store_quotes_trades(start: str, symbols: set, channel: str, path: str):
    """
    Stores the data as .csv files on a pre-defined (see README.md) directory structure.
    """

    new = True
    header = {symbol: True for symbol in symbols}
    temp = start.strftime("%Y%m%d")  # Points to the file.

    with open(temp, newline="") as inp:
        reader = csv.reader(inp)
        for row in reader:
            if row[1] in symbols:
                symbol = row[1]

                location = f"{path}/{symbol}/{channel}s/{start.year}/{start.month}"
                if not os.path.isdir(location):
                    os.makedirs(location)

                _file = f"{location}/{temp[:4]}-{temp[4:6]}-{temp[6:]}.csv"

                if new:
                    _delete_old(_file)
                    new = False

                # Pandas couldn't parse the dates - The next line fixes that.
                row[0] = row[0].replace("D", "T", 1)

                with open(_file, "a", newline="") as f:
                    writer = csv.writer(f)
                    if header[symbol]:
                        writer.writerow(_headers[channel])
                        header[symbol] = False
                    writer.writerow(row)

    os.remove(temp)


def poll_quotes_trades(start: dt, end: dt, symbols: set, channel: str, path: str):
    """
    Polls data in daily blocks from BitMEX servers.
    """

    while start <= end:
        # BitMEX names each zipped file by its date in the format below.
        # We must download, unzip it and extract the data.
        temp = start.strftime("%Y%m%d")
        count = 0
        while True:
            response = requests.get(quotes_trades_endpoint.format(channel, temp))
            if response.status_code == 200:
                break
            else:
                count += 1
                if count == 10:
                    if response.status_code == 404:
                        # Data for today or yesterday might yet not be available.
                        today = dt.today()
                        yesterday = today - timedelta(1)
                        if (
                            start.date() == today.date()
                            or start.date() == yesterday.date()
                        ):
                            return f"Failed to download: {start.date()} - data not (yet) available."
                    response.raise_for_status()
                print(
                    f"ERROR: {response.status_code} while processing {start.date()} - retrying."
                )
                time.sleep(10)

        _unzip_quotes_trades(temp, response)
        _store_quotes_trades(start, symbols, channel, path)

        print(f"Processed {channel}s: {str(start)[:10]}")
        start += timedelta(days=1)

    return "Success - all data downloaded and stored."


def _store_bars(
    data: list, end: dt, path: str, symbol: str, channel: str, bar: str, header: dt
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

        location = f"{path}/{symbol}/{channel}/{bar}/{date.year}/{date.month}"
        if not os.path.isdir(location):
            os.makedirs(location)

        d = date.strftime("%Y%m%d")
        _file = f"{location}/{d[:4]}-{d[4:6]}-{d[6:]}.csv"

        new = True if date != header else False
        if new:
            _delete_old(_file)

        with open(_file, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_headers[channel])

            # "header" starts as None but from the 2nd iteration on it equals the last
            # date. If the actual date (not datetime) is different than the previous one,
            # a new header is written (on a new file).
            if header != date:
                writer.writeheader()
                header = date
            writer.writerow(row)
    return header


def poll_bars(start: dt, end: dt, symbols: set, channel: str, bars: list, path: str):
    """
    Polls bars (time buckets of trades) from BitMEX servers.
    Options are: [1m, 5m, 1h, 1d]
    """

    # Number of requests made to the server.
    req = 0

    header = None
    error = False

    for symbol in symbols:
        for bar in bars:
            # Can't modify 'start' and 'end' directly.
            _start = start
            _end = end + timedelta(days=1)

            while _start < _end:
                if error:
                    print("Success: continuing.")
                    error = False

                req += 1
                # 30 is the maximum number of requests allowed per minute.
                if req % 30 == 0:
                    print(f"Respecting API limits - Sleeping for 60 seconds.")
                    time.sleep(60)
                    req = 0

                response = requests.get(
                    bars_endpoint.format(bar, symbol, str(_start), str(_end))
                )

                # BitMEX API often throws a "429 - too many requests" error, even
                # if we are respecting its limits.
                if response.status_code == 429:
                    error = True
                    print(
                        f"Failed to process: {_start.date()} - Sleeping for 60 seconds and retrying."
                    )
                    time.sleep(60)
                    continue

                data = response.json()
                if not data:
                    print(f"Data does not exist for {symbol}-{bar}: {_start.date()}")
                    _start += timedelta(days=1)
                    time.sleep(1)
                    continue

                stored = _store_bars(data, _end, path, symbol, channel, bar, header)
                if stored:
                    header = stored

                print(f"Processed {symbol} {bar}-bars: {_start.date()}")

                # 500 is the maximum number of results per request.
                if bar == "1m":
                    _start += timedelta(minutes=500)
                elif bar == "5m":
                    _start += timedelta(minutes=500 * 5)
                elif bar == "1h":
                    _start += timedelta(hours=500)
                else:
                    _start += timedelta(days=500)

    return "Success - all data downloaded and stored."


def _separator(channel=None):
    """
    Prints a separator for each of the different parts of the program.
    """
    print("-" * 80)
    if channel:
        print(f"Start processing {channel}:\n")
    else:
        print("Finished.\n")
        print("Report")
        print("------")


def _transform_validate(args):
    """
    Transforms and validates the arguments passed to the main function.
    """

    # Strip dates.
    start = dt.strptime(args.start, "%Y-%m-%d")
    end = dt.strptime(args.end, "%Y-%m-%d")

    # Remove possible duplicates.
    symbols = set(args.symbols)
    channels = set(args.channels)

    # Validate all values.
    if args.bars:
        bars = set(args.bars)
        if "bars" in channels and not bars:
            sys.exit("\nChannel 'bars' enabled, but no timeframe was provided.")
        if bars and "bars" not in channels:
            sys.exit("\nTimeframe(s) provided but channel 'bars' is not enabled.\n")
    else:
        bars = None

    symbols = _validate_symbols(symbols)
    start, end = _validate_dates(start, end)
    path = _validate_path(args.save_to)

    return symbols, channels, start, end, bars, path


def main(args):

    symbols, channels, start, end, bars, path = _transform_validate(args)

    report = {}
    for channel in channels:
        _separator(channel)
        if channel == "bars":
            report[channel] = poll_bars(start, end, symbols, channel, bars, path)
        else:
            report[channel] = poll_quotes_trades(
                start, end, symbols, channel[:-1], path
            )

    _separator()
    for channel, status in report.items():
        print(f"{channel}: {status}")
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

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/21f103c475e44fa4b30936f06bb5088f)](https://www.codacy.com/manual/dxflores/bmex?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=dxflores/bmex&amp;utm_campaign=Badge_Grade)
# bmex

Lets you download and store (by creating the directory structure shown below) historical data ([bars](https://www.bitmex.com/api/explorer/#!/Trade/Trade_getBucketed), [quotes](https://www.bitmex.com/api/explorer/#!/Quote/Quote_get) and [trades](https://www.bitmex.com/api/explorer/#!/Trade/Trade_get)) for all symbols and indices (including delisted ones) from BitMEX.

Example: `python bmex.py --symbols XBTUSD ETHUSD --channels bars quotes trades --start 2018-12-31 --end 2019-01-01 --bars 1m 5m 1h 1d`

```
current_directory   # Where you run the code.
    └──BITMEX/
        ├── ETHUSD
        │   ├── bars
        │   │   ├── 1d
        │   │   │   ├── 2018
        │   │   │   │   └── 12
        │   │   │   │       └── 2018-12-31.csv
        │   │   │   └── 2019
        │   │   │       └── 1
        │   │   │           └── 2019-01-01.csv
        │   │   ├── 1h
        │   │   │   ├── 2018
        │   │   │   │   └── 12
        │   │   │   │       └── 2018-12-31.csv
        │   │   │   └── 2019
        │   │   │       └── 1
        │   │   │           └── 2019-01-01.csv
        │   │   ├── 1m
        │   │   │   ├── 2018
        │   │   │   │   └── 12
        │   │   │   │       └── 2018-12-31.csv
        │   │   │   └── 2019
        │   │   │       └── 1
        │   │   │           └── 2019-01-01.csv
        │   │   └── 5m
        │   │       ├── 2018
        │   │       │   └── 12
        │   │       │       └── 2018-12-31.csv
        │   │       └── 2019
        │   │           └── 1
        │   │               └── 2019-01-01.csv
        │   ├── quotes
        │   │   ├── 2018
        │   │   │   └── 12
        │   │   │       └── 2018-12-31.csv
        │   │   └── 2019
        │   │       └── 1
        │   │           └── 2019-01-01.csv
        │   └── trades
        │       ├── 2018
        │       │   └── 12
        │       │       └── 2018-12-31.csv
        │       └── 2019
        │           └── 1
        │               └── 2019-01-01.csv
        └── XBTUSD
            ├── ... # Same as above.
```
## Notes
- In addition to the parameters shown in the example above, you can pass an extra parameter "--save_to" to create the directory structure at a preferred path.
- Confirm that you have the necessary storage space. To give you an idea, as of December 2019, a full backfill of only XBTUSD will require ~125G of free space (~75G quotes + ~50G trades + ~300MB bars).

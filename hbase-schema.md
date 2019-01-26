## Trade Table
Already solved: https://cloud.google.com/bigtable/docs/schema-design-time-series

KEY: AAPL#1547816737396#streaknum

cf| column query    | value
==|=================|====================
MD| SYMBOL          | AAPL
MD| TIMESTAMP       | 1547816737396
MD| PRICE           | 155.85
MD| SIZE            | 10
MD| EXCHANGE        | 8
MD| FLAGS           | 825373494

1d = 20,487,467 rows in 25mins

## Aggregate Tables
KEY: AAPL#1547824740000

cf| column query    | value
==|=================|====================
MD| OPEN            | 155.85
MD| HIGH            | 10
MD| LOW             | 8
MD| CLOSE           | 825373494
MD| VOLUME          | 10
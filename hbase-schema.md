## Trade Table
See: https://cloud.google.com/bigtable/docs/schema-design-time-series

KEY:
symbol  | timestamp  | streaknum
========|============|===========
5c (5)  | long (8)   | short (2)


cf| column query    | value
==|=================|====================
t | PRICE           | 155.85
t | SIZE            | 10
t | FLAGS           | 825373494

1d = 20,487,467 rows in 25mins

## Aggregate Tables
KEY:
symbol  | timestamp
========|===========
5c (5)  | long (8)

cf| column query    | value
==|=================|====================
MD| OPEN            | 155.85
MD| HIGH            | 10
MD| LOW             | 8
MD| CLOSE           | 825373494
MD| VOLUME          | 10
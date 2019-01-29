## Trade Table
See: https://cloud.google.com/bigtable/docs/schema-design-time-series

KEY:
symbol  | timestamp  | streaknum
========|============|===========
5c (5)  | long (8)   | short (2)
0-4     | 5-13       | 13-14

cf| column query    | value
==|=================|====================
t | PRICE           | 155.85
t | SIZE            | 10
t | FLAGS           | 825373494

From 01-02-2019 to 01-24-2019 (22 days): 602,650,370 rows


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

From 01-02-2019 to 01-24-2019 (22 days, 17 trading):
aggPeriod | rows          | per day (/17)
==========|===============|==============
(none)    | 602,650,370   | 35,450,021
1s        | 117,510,731   |  6,912,395
1m        |  23,756,991   |  1,397,470
5m        |   7,379,264   |    434,074
1h        |   1,009,144   |     59,361
1d        |     207,411   |     12,200

All in 16GB...
4.6GB in 6 days
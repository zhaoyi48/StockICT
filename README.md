# StockICT

## Before Use

Create database

## QuickStart

```
docker run -d \
--name StockICT \
--read-only  -v /config/config.ini:/config/config.ini \
realforcez/StockICT
```

## Config File Example
```
[MYSQL]
database_username = <database_username>
database_password = <database_password>
database_ip = <database_ip>
database_name = <database_name>

[Time.interval]
stock_zh_a_spot_em.stock_zh_a_spot_em.append.minutes = 1
#<function>.<tablename>.<insert_type>.<seconds/minutes/hours> = <value>
[Time.point]
stock_sse_summary.stock_sse_summary.append.hour = 20:30
#<function>.<tablename>.<insert_type>.<minutes/hour/day> = <value>
```
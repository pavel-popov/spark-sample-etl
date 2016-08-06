# Test ETL process for OneFactor

* [Original requirements][requirements.pdf]
* Technologies:
  - Apache Spark
  - Scala API for Apache Spark
  - Cloudera CDH 5.7 (VirtualBox VM)
  - [Simple test data generator][stdg]


## Introduction

Currently our Megastartup company has two clients:

- **Eighty** – Big federal chain selling office equipment
- **Minodo Pizza** – Local pizzeria chain

## Test Data

Test data is generated using [Simple test data generator][stdg] utility using the following commands:

- Eighty – 2,500,000 rows:

      ./stdg -schema test_data/schema-eighty.json \
         -rows 2500000 \
         -columns branch,client_id,region,first_purchase,orders_count,payment_sum \
         1> test_data/eighty.csv

- Minodo Pizza – 100,000 rows:

      ./stdg -schema schema-minodo_pizza.json \
         -rows 100000 \
         -columns email,filial_id,reg_timestamp,orders_last_month,payment_sum \
         1> test_data/minodo.csv

## RAID (Restrictions, Assumptions, Issues, Defects)

### Dates handling

As per current requirements we need to provide cutomer age in days. Some clients also provide data without time part. Hence it's not useful to bother with time-zones and keep timestamps as Unix timestamps. We'll keep dates as strings in ISO-8601 format (YYYY-MM-DD).



## HDFS directory structure

- `/raw` - all raw data from upstreams goes here
  * `/raw/client/eighty` - directory for source files from Eighty client
  * `/raw/client/minodo_pizza` - directory for source files from "Minodo Pizza" client

- `/staging` - staging data

- `/core` - core data in Normal Form
  * `/core/lov` - list of values folder (static valus, manual infrequent change)
  * `/core/dim` - dimensions folder (slowly changing dimensions)
  * `/core/facts` - facts folder

- `/datamart/customer` - Data Mart with Customer data

## Source code structure

- `/etl` – ETL subsystem
- `/ml` – Machine Learning subsystem
- `/importer` – Importer subsystem downloading data from client' informational systems
- `/publisher` – Publisher subsystem which publishes result data to clients

[requirements.pdf]: https://raw.githubusercontent.com/schmooser/onefactor_etl/master/assets/requirements.pdf
[stdg]: #

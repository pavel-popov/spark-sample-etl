# Test ELT for 1Factor

* [Original requirements][requirements.pdf]
* Technologies:
  - Apache Spark
  - Scala API for Apache Spark
  - Cloudera CDH 5.7 (VirtualBox VM)
  - [Simple test data generator][stdg]


## Source data description


### Clients

- Eighty - Big federal chain selling office equiment
- Minodo Pizza - Local pizzeria chain

### Test Data

Test data is generated using [Simple test data generator] utility using the following commands:

- Eighty - 2,500,000 rows:

      ./stdg -schema schema-eighty.json -rows 2500000 -columns branch,client_id,region,first_purchase,orders_count,payment_sum 1> eighty.csv

- Minodo Pizza - 100,000 rows:

      ./stdg -schema schema-minodo_pizza.json -rows 1000 -columns email,filial_id,reg_timestamp,orders_last_month,payment_sum 1> minodo_pizza.csv


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


[requirements.pdf]: https://raw.githubusercontent.com/schmooser/onefactor_etl/master/assets/requirements.pdf
[stdg]: #

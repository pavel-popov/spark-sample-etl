# Тестовое задание для OneFactor

[Требования][requirements.pdf]

Допустим, стартап называется Megastartup, а два его текущих клиента:

- Eighty - крупная федеральная сеть по продаже офисной техники
- Minodo Pizza - небольшая сеть пиццерий

## Глоссарий

- **Client** / **Клиент** - юр. лицо работающее с Megastartup
- **Customer** / **Покупатель** - покупатель услуг/товаров у Клиента

## Допущения и предположения

- Физическими лицами офисная техника покупается достаточно редко, а юридических лиц меньше чем физических лиц. Можно предположить, что несмотря на то Eighty намного крупнее чем Minodo Pizza по выручке, количество клиентов за период будет одного порядка
- Выгрузка в рекомендательную систему производится со следующими параметрами:
  * период выгрузки, в текущих условиях равный 1 календарному месяцу (например, "2016-08")
  * код клиента `client_code`, в настоящее время доступно два кода - "eighty", "minodo"
- Объединения и сравнения массивов Покупателей разных Клиентов не происходит – в текущих входных данных не содержится информации позволяющей это сделать. Также специфика бизнесов Клиентов сильно отличается и пользы подобного процесса нет.
- Для тестового использования в качестве файловой системы используется локальная файловая система. При промышленной эксплуатации предполагается использование HDFS.
- Все данные от Клиентов содержат в первой строке заголовок с перечислением атрибутов в файле.
- Перед началом первичной загрузки в каталоге с файлами данных создаются необходимые пустые файлы – это делается автоматически файлом запуска приложения.

## Логическая модель данных

### Типы наборов данных

Логическая модель включает в себя три типа наборов данных:

- **LOV** - List Of Values - справочнки. Статические данные, добавление новых значений возможно в ручном режиме, удаление данных не предусмотрено. В первой версии приложения таким набором будет "Регион РФ" (REGION)
- **DIM** - Dimensions - измерения. Медленно меняющиеся наборы данных, содержащих статическую информацию. В первой версии приложения таким набором будет "Покупатель" (CUSTOMER). Измерения могут содержать ссылки на LOV
- **FACT** - Facts - фактовые данные, часто меняющиеся (на добавление) данные. В первой версии приложения таким набором будет "Заказы" (ORDERS)


### Логические сущности и атрибуты

#### CUSTOMER

- Тип: DIM

| Attribute    | Data Domain | Description         | PK   |
| ------------ | ----------- | ------------------- | ---- |
| CLIENT\_CODE | Character   | Client Code         |      |
| CUST\_ID     | Character   | Customer identifier | Y    |
| REG\_DT      | Date        | Registration date   |      |
| REGION\_ID   | Numeric     | Region identifier   |      |




#### ORDERS

- Тип: FACT


| Attribute    | Data Domain | Description                     | PK   | Link To           |
| ------------ | ----------- | ------------------------------- | ---- | ----------------- |
| CUST\_ID     | Character   | Unique Customer identifier      | Y    | CUSTOMER.CUST\_ID |
| PERIOD       | Date        | First day of reporting month    | Y    |                   |
| ORDERS\_CNT  | Numeric     | Count of orders in a period     |      |                   |
| PAYMENT\_SUM | Numeric     | Amount of purchases in a period |      |                   |



## Физическая модель данных

Данные от Клиентов попадают на файловую систему в исходном формате. Для текущей реализации считаем, что исходные файлы подготовлены и находятся на локальной файловой системе.

Данные, полученные в результате ETL-процессов, сохраняются на файловую систему в виде плоских CSV-файлов. Схема файлов соответствует Логической модели за удалением избыточных атрибутов, определяемых структурой каталогов. Формат файлов описан ниже.

Структура каталогов файловой системы (относительно некоторой "домашней директории"):

* `dim/customer/<client_code>/<period>` – директория для измерения CUSTOMER, каждая директория по периоду содержит новых Покупателей, появившихся в периоде
* `fact/orders/<client_code>/<period>` – факты по покупкам за отчетный период
* `dm/orders/<client_code>/<period>` - результаты работа ETL-процессов

При добавлении новых типов измерений и фактов они помещаются в ту же структуру каталогов с именами в формате: `dim/<dimension_name>/<client_code>` и `fact/<fact_name>/<client_code>`.

### Физические сущности и атрибуты

Здесь и далее используются типы данных Scala.

#### customer

* Тип: DIM

| Attribute  | Data Type | Description                     |
| ---------- | --------- | ------------------------------- |
| CUST\_ID   | String    | Customer identifier             |
| REG\_DT    | String    | Registration date in YYYY-MM-DD |
| REGION\_ID | Short     | Region identifier               |

Атрибуты, получаемые из пути к файлу:

* CLIENT\_CODE

#### orders

* Тип: FACT

| Attribute    | Data Type | Description                     |
| ------------ | --------- | ------------------------------- |
| CUST\_ID     | String    | Customer identifier             |
| ORDERS\_CNT  | Int       | Count of orders in a period     |
| PAYMENT\_SUM | Double    | Amount of purchases in a period |

Атрибуты получаемые из пути к файлу:

* PERIOD
* CLIENT\_CODE



### Описание ETL-процессов

Загрузка состоит из 3 этапов:

1. Загрузка данных в стейджинговый массив данных Staging, в котором данные представлены в очищенном виде и приведены к единому виду для всех источников:

   | Attribute        | Data Type      | Description                              |
   | ---------------- | -------------- | ---------------------------------------- |
   | CustID           | String         | Unique Customer Identifier               |
   | Region           | Int            | Region Identifier, based on static lookup |
   | RegistrationDate | String         | Registration Date in YYYY-MM-DD format   |
   | OrdersCnt        | Int            | Count of orders within the period        |
   | PaymentSum       | Option[Double] | Sum of payments per period               |

   Сохранение стейджингового массива данных на диск не проиходит, он хранится в памяти
   во время обработки.

2. Загрузка Покупателей из массива Staging:

   * Из Staging в массив Current Customers загружается список Покупателей, совершивших покупки в текущем периоде.
   * В массив Existing Customers из области данных на диске (или из HDFS) загружается полный список всех Покупателей, когда либо совершавших покупки.
   * Эти массивы сравниваются, формируется Delta изменений (новые Покупатели) и формируется общий массив Customers как Existing Customers UNION ALL Delta.

3. Из массива Staging формируется массив Orders, содержащий информацию о
   покупках за текущий период.

4. Массив Customers (2) соединяется левым соединением (LEFT JOIN) с массивом
   Orders (3) по ключу CustID и полученные данные сохраняются в DataMart область


## Тестовые данные

Тестовые данные подготовлены с помощью приложения [Simple Test Data Generator](https://github.com/pavel-popov/stdg), схемы для тестовых данных сохранены в директории `test\_data`.

Команды для подготовки тестовых данных:

* Eighty:

        stdg -columns branch,client_id,region,first_purchase,orders_count,payment_sum \
             -schema schema-eighty.json -rows 25000 > eighty.csv

* Minodo:

        stdg -columns email,filial_id,reg_timestamp,orders_last_month,payment_sum \
             -schema schema-minodo.json -rows 10000 > minodo.csv

Тестовые данные доступны в файлах `test\_data/eighty.csv` и `test\_data/minodo.csv`.


## Сборка проекта

Для сборки используется Maven:

    mvn clean && mvn package

## Запуск ETL-процессов

Для запуска приложен скрипт `run_etl.sh`, который надо запустить с параметрами:
* `source\_data` - путь к файлу с данными
* `client\_code` - код клиента, eighty или minodo
* `period` - отчетный период, в формате YYYY-MM

Пример запуска для Eighty:

    ./run_etl.sh test_data/eighty.csv eighty 2016-06

Пример запуска для Minodo:

    ./run_etl.sh test_data/minodo.csv minodo 2016-06


[requirements.pdf]: https://raw.githubusercontent.com/pavel-popov/onefactor_etl/master/assets/requirements.pdf

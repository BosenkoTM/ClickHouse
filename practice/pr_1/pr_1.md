# Практическая работа № 1. Введение в первичные индексы, типы данных и базовые запросы ClickHouse

## Введение
В этом руководстве мы подробно рассмотрим, как ClickHouse создаёт и использует разреженный первичный индекс таблицы, чем он отличается от традиционных реляционных СУБД, и какие практики по индексации являются рекомендуемыми. 

Мы будем использовать подмножество из 8,87 млн строк анонимизированного набора данных веб-трафика (таблицы `hits`) и данные о такси Нью-Йорка (`trips`, `taxi_zone_dictionary`).

---

## Часть 1. Создание базовых таблиц и загрузка данных

### 1. Таблица без первичного ключа (Полное сканирование)
Чтобы понять важность индексов, создадим таблицу без первичного ключа.

```sql
CREATE TABLE hits_NoPrimaryKey
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY tuple();
```

**Загрузка данных:**
*(Внимание. Загрузка выполняется напрямую с серверов ClickHouse. Процесс может занять несколько минут).*
```sql
INSERT INTO hits_NoPrimaryKey SELECT
   intHash32(UserID) AS UserID,
   URL,
   EventTime
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz', 'TSV', 'WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, URLDomain String, RefererDomain String, Refresh UInt8, IsRobot UInt8, RefererCategories Array(UInt16), URLCategories Array(UInt16), URLRegions Array(UInt32), RefererRegions Array(UInt32), ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), UTCEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, RemoteIP6 FixedString(16), WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, RedirectTiming Int32, DOMInteractiveTiming Int32, DOMContentLoadedTiming Int32, DOMCompleteTiming Int32, LoadEventStartTiming Int32, LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32, FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32, YCLID UInt64, ShareService String, ShareURL String, ShareTitle String, ParsedParams Nested(Key1 String, Key2 String, Key3 String, Key4 String, Key5 String, ValueDouble Float64), IslandID FixedString(16), RequestNum UInt32, RequestTry UInt8')
WHERE URL != '';

OPTIMIZE TABLE hits_NoPrimaryKey FINAL;
```

### 2. Таблица с первичным ключом `(UserID, URL)`
Создадим таблицу, где данные физически упорядочены по пользователю, затем по URL.

```sql
CREATE TABLE hits_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY (UserID, URL)
ORDER BY (UserID, URL, EventTime)
SETTINGS index_granularity_bytes = 0, compress_primary_key = 0;
```

**Загрузка данных:**
```sql
INSERT INTO hits_UserID_URL SELECT intHash32(UserID) AS UserID, URL, EventTime FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz', 'TSV', 'WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, URLDomain String, RefererDomain String, Refresh UInt8, IsRobot UInt8, RefererCategories Array(UInt16), URLCategories Array(UInt16), URLRegions Array(UInt32), RefererRegions Array(UInt32), ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), UTCEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, RemoteIP6 FixedString(16), WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, RedirectTiming Int32, DOMInteractiveTiming Int32, DOMContentLoadedTiming Int32, DOMCompleteTiming Int32, LoadEventStartTiming Int32, LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32, FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32, YCLID UInt64, ShareService String, ShareURL String, ShareTitle String, ParsedParams Nested(Key1 String, Key2 String, Key3 String, Key4 String, Key5 String, ValueDouble Float64), IslandID FixedString(16), RequestNum UInt32, RequestTry UInt8') WHERE URL != '';

OPTIMIZE TABLE hits_UserID_URL FINAL;
```

### 3. Таблицы для изучения кардинальности и сжатия
Для анализа того, как порядок ключей влияет на сжатие данных (для колонки `IsRobot`), создадим две таблицы.

**Таблица `hits_URL_UserID_IsRobot` (Сортировка по убыванию кардинальности):**
```sql
CREATE TABLE hits_URL_UserID_IsRobot
(
    `UserID` UInt32,
    `URL` String,
    `IsRobot` UInt8
)
ENGINE = MergeTree
PRIMARY KEY (URL, UserID, IsRobot);

INSERT INTO hits_URL_UserID_IsRobot SELECT intHash32(c11::UInt64) AS UserID, c15 AS URL, c20 AS IsRobot FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz') WHERE URL != '';
```

**Таблица `hits_IsRobot_UserID_URL` (Сортировка по возрастанию кардинальности - Оптимально для сжатия):**
```sql
CREATE TABLE hits_IsRobot_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `IsRobot` UInt8
)
ENGINE = MergeTree
PRIMARY KEY (IsRobot, UserID, URL);

INSERT INTO hits_IsRobot_UserID_URL SELECT intHash32(c11::UInt64) AS UserID, c15 AS URL, c20 AS IsRobot FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz') WHERE URL != '';
```

---

## Часть 2. Использование дополнительных структур (Оптимизация)

### 1. Создание вспомогательной таблицы `hits_URL_UserID`
Позволяет быстро искать по URL.
```sql
CREATE TABLE hits_URL_UserID
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY (URL, UserID)
ORDER BY (URL, UserID, EventTime)
SETTINGS index_granularity_bytes = 0, compress_primary_key = 0;

INSERT INTO hits_URL_UserID SELECT * FROM hits_UserID_URL;
OPTIMIZE TABLE hits_URL_UserID FINAL;
```

### 2. Материализованное представление
```sql
CREATE MATERIALIZED VIEW mv_hits_URL_UserID
ENGINE = MergeTree()
PRIMARY KEY (URL, UserID)
ORDER BY (URL, UserID, EventTime)
POPULATE
AS SELECT * FROM hits_UserID_URL;
```

### 3. Создание проекции
Самый «прозрачный» способ перестроить индексы.
```sql
ALTER TABLE hits_UserID_URL
    ADD PROJECTION prj_url_userid
    (
        SELECT *
        ORDER BY (URL, UserID)
    );

ALTER TABLE hits_UserID_URL MATERIALIZE PROJECTION prj_url_userid;
```

---
*(Конец практического руководства)*
<br><br>

---

# Блок тестирования: 100 практических заданий (CodeRunner)

Ниже представлены задачи для проверки усвоения SQL в ClickHouse. 

## Раздел 1. Базовые запросы, SELECT, LIMIT, ORDER BY (Задачи 1-20)

**Задание 1. Вывод всех поездок**
Вам необходимо получить базовое представление о структуре данных такси. Выведите все доступные столбцы для первых пяти поездок в системе. Порядок вывода менять не нужно. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `*`
<details><summary>Решение</summary><code>SELECT * FROM trips LIMIT 5;</code></details>

**Задание 2. Самые дорогие поездки**
Аналитиков интересуют поездки с максимальной суммарной стоимостью. Выведите идентификатор поездки и ее полную стоимость. Отсортируйте результат по убыванию стоимости. Ограничьте вывод 10 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `total_amount`
<details><summary>Решение</summary><code>SELECT trip_id, total_amount FROM trips ORDER BY total_amount DESC LIMIT 10;</code></details>

**Задание 3. Самые короткие поездки (ненулевые)**
Необходимо найти аномально короткие, но совершенные поездки. Выведите ID поездки и дистанцию. Отсортируйте по возрастанию дистанции. Учитывайте только поездки, где дистанция больше нуля. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `trip_distance`
<details><summary>Решение</summary><code>SELECT trip_id, trip_distance FROM trips WHERE trip_distance > 0 ORDER BY trip_distance ASC LIMIT 5;</code></details>

**Задание 4. Хронология событий**
Требуется узнать, когда было зафиксировано самое первое событие в логах веб-трафика. Выведите время и URL этого события, отсортировав по времени по возрастанию. Ограничьте вывод 1 записью.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `EventTime`, `URL`
<details><summary>Решение</summary><code>SELECT EventTime, URL FROM hits_UserID_URL ORDER BY EventTime ASC LIMIT 1;</code></details>

**Задание 5. Типы оплаты**
Справочный запрос. Необходимо вывести список всех уникальных методов оплаты, которые встречаются в таблице такси. Ограничивать вывод не нужно.
*   **Схема:** `trips`
*   **Поля:** `payment_type`
<details><summary>Решение</summary><code>SELECT DISTINCT payment_type FROM trips;</code></details>

**Задание 6. Самые длинные поездки**
Служба безопасности такси просит список ID поездок и дистанций для 7 самых длинных по расстоянию маршрутов. Отсортируйте данные по убыванию дистанции.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `trip_distance`
<details><summary>Решение</summary><code>SELECT trip_id, trip_distance FROM trips ORDER BY trip_distance DESC LIMIT 7;</code></details>

**Задание 7. Сортировка по нескольким столбцам**
Для анализа загруженности выведите идентификаторы поездок, отсортировав их сначала по количеству пассажиров (по убыванию), а затем по дате посадки (по возрастанию). Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `passenger_count`, `pickup_datetime`
<details><summary>Решение</summary><code>SELECT trip_id, passenger_count, pickup_datetime FROM trips ORDER BY passenger_count DESC, pickup_datetime ASC LIMIT 5;</code></details>

**Задание 8: Недавние события пользователей**
Просмотр активности пользователей. Выведите ID пользователя и URL для последних 10 зафиксированных событий в базе данных. Сортировка по времени события по убыванию.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `UserID`, `URL`, `EventTime`
<details><summary>Решение</summary><code>SELECT UserID, URL FROM hits_UserID_URL ORDER BY EventTime DESC LIMIT 10;</code></details>

**Задание 9: Алиасы столбцов**
Для выгрузки в отчет необходимо переименовать столбцы в самом запросе. Выведите ID поездки как `Order_ID` и сумму тарифа как `Base_Fare`. Ограничьте вывод 3 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `fare_amount`
<details><summary>Решение</summary><code>SELECT trip_id AS Order_ID, fare_amount AS Base_Fare FROM trips LIMIT 3;</code></details>

**Задание 10: Смещение выборки (OFFSET)**
Для постраничной навигации выведите 5 поездок, пропустив первые 20 записей. Выводить нужно все поля без явной сортировки.
*   **Схема:** `trips`
*   **Поля:** `*`
<details><summary>Решение</summary><code>SELECT * FROM trips LIMIT 5 OFFSET 20;</code></details>

**Задание 11: Уникальные пользователи-боты**
Для настройки фильтров безопасности необходимо получить список уникальных идентификаторов пользователей, которые были классифицированы системой как боты. Ограничьте вывод 10 записями.
*   **Схема:** `hits_IsRobot_UserID_URL`
*   **Поля:** `UserID`
<details><summary>Решение</summary><code>SELECT DISTINCT UserID FROM hits_IsRobot_UserID_URL WHERE IsRobot = 1 LIMIT 10;</code></details>

**Задание 12: Крупные чаевые**
Проанализируйте поездки с большими чаевыми. Выведите ID поездки и сумму чаевых, отсортировав по размеру чаевых по убыванию. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `tip_amount`
<details><summary>Решение</summary><code>SELECT trip_id, tip_amount FROM trips ORDER BY tip_amount DESC LIMIT 5;</code></details>

**Задание 13: Поездки без пассажиров**
Технический сбой или перегон автомобиля? Выведите ID поездок, где зафиксировано 0 пассажиров. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `passenger_count`
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE passenger_count = 0 LIMIT 5;</code></details>

**Задание 14: Самые дешевые поездки (ненулевые)**
Найдите 5 поездок с минимальной итоговой стоимостью, исключая полностью бесплатные (стоимость строго больше 0). Выведите ID и стоимость, отсортировав по возрастанию стоимости.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `total_amount`
<details><summary>Решение</summary><code>SELECT trip_id, total_amount FROM trips WHERE total_amount > 0 ORDER BY total_amount ASC LIMIT 5;</code></details>

**Задание 15: Уникальные посещенные страницы**
Маркетологам нужен список страниц, которые посещались пользователями. Выведите список уникальных URL, отсортированных по алфавиту (по возрастанию). Ограничьте вывод 10 записями.
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`
<details><summary>Решение</summary><code>SELECT DISTINCT URL FROM hits_URL_UserID ORDER BY URL ASC LIMIT 10;</code></details>

**Задание 16: Крупные налоги**
Выведите ID поездки и сумму налога `mta_tax` для тех случаев, когда налог был сортирован по убыванию. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `mta_tax`
<details><summary>Решение</summary><code>SELECT trip_id, mta_tax FROM trips ORDER BY mta_tax DESC LIMIT 5;</code></details>

**Задание 17: Выборка по конкретному провайдеру**
Выведите все данные о 5 поездках, выполненных провайдером с `vendor_id` = 2.
*   **Схема:** `trips`
*   **Поля:** `*`
<details><summary>Решение</summary><code>SELECT * FROM trips WHERE vendor_id = 2 LIMIT 5;</code></details>

**Задание 18: История пользователя**
Выведите хронологию событий (URL и время) для пользователя с `UserID` = 2459550954. Сортировка по времени события от старых к новым. Ограничьте вывод 5 записями.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `URL`, `EventTime`
<details><summary>Решение</summary><code>SELECT URL, EventTime FROM hits_UserID_URL WHERE UserID = 2459550954 ORDER BY EventTime ASC LIMIT 5;</code></details>

**Задание 19: Типы автомобилей**
Выведите список уникальных типов такси (cab_type), отсортированных по убыванию (алфавитному).
*   **Схема:** `trips`
*   **Поля:** `cab_type`
<details><summary>Решение</summary><code>SELECT DISTINCT cab_type FROM trips ORDER BY cab_type DESC;</code></details>

**Задание 20: Дополнительные сборы**
Найдите поездки с максимальными дополнительными сборами (`extra`). Выведите ID и сбор, отсортировав по убыванию сбора. Вывести 5 записях.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `extra`
<details><summary>Решение</summary><code>SELECT trip_id, extra FROM trips ORDER BY extra DESC LIMIT 5;</code></details>


## Раздел 2: Фильтрация данных (WHERE, LIKE, IN, BETWEEN) (Задачи 21-40)

**Задание 21: Поиск по подстроке URL**
Необходимо найти события, связанные с сайтом auto.ru. Выведите URL и время события для записей, где URL содержит `auto.ru`. Ограничьте вывод 5 записями.
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`, `EventTime`
<details><summary>Решение</summary><code>SELECT URL, EventTime FROM hits_URL_UserID WHERE URL LIKE '%auto.ru%' LIMIT 5;</code></details>

**Задание 22: Диапазон стоимости поездки**
Финансовый отдел запрашивает поездки средней ценовой категории. Выведите ID поездки и итоговую сумму для поездок стоимостью от 20 до 30 включительно. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `total_amount`
<details><summary>Решение</summary><code>SELECT trip_id, total_amount FROM trips WHERE total_amount BETWEEN 20 AND 30 LIMIT 5;</code></details>

**Задание 23: Конкретные типы оплаты**
Выведите ID поездок, которые были оплачены наличными ('CSH') или без оплаты ('NOC'). Используйте оператор IN. Ограничьте результат 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `payment_type`
<details><summary>Решение</summary><code>SELECT trip_id, payment_type FROM trips WHERE payment_type IN ('CSH', 'NOC') LIMIT 5;</code></details>

**Задание 24: Выборка по дате**
Необходимо найти все поездки, совершенные строго 1 января 2016 года. Выведите дату посадки и ID поездки. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `pickup_date`, `trip_id`
<details><summary>Решение</summary><code>SELECT pickup_date, trip_id FROM trips WHERE pickup_date = '2016-01-01' LIMIT 5;</code></details>

**Задание 25: Сложное логическое условие**
Найдите длительные и дешевые поездки: дистанция больше 15 миль, а итоговая стоимость меньше 40. Выведите ID поездки. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE trip_distance > 15 AND total_amount < 40 LIMIT 5;</code></details>

**Задание 26: Защищенные соединения**
Вам поручено найти все события, где доступ к сайту осуществлялся по безопасному протоколу. Выведите уникальные URL, начинающиеся с `https://`. Ограничьте результат 10 записями.
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`
<details><summary>Решение</summary><code>SELECT DISTINCT URL FROM hits_URL_UserID WHERE URL LIKE 'https://%' LIMIT 10;</code></details>

**Задание 27: Человеческий трафик на конкретном сайте**
Необходимо найти логи посещений для пользователей-людей (`IsRobot` = 0) на страницах, содержащих `forum`. Выведите ID пользователя и URL. Ограничьте вывод 5 записями.
*   **Схема:** `hits_URL_UserID_IsRobot`
*   **Поля:** `UserID`, `URL`
<details><summary>Решение</summary><code>SELECT UserID, URL FROM hits_URL_UserID_IsRobot WHERE IsRobot = 0 AND URL LIKE '%forum%' LIMIT 5;</code></details>

**Задание 28: Исключение значений**
Анализируем нестандартные тарифы. Выведите ID поездки и код тарифа (`rate_code_id`), где код тарифа НЕ равен 1 (стандартный тариф). Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `rate_code_id`
<details><summary>Решение</summary><code>SELECT trip_id, rate_code_id FROM trips WHERE rate_code_id != 1 LIMIT 5;</code></details>

**Задание 29: Щедрость пассажиров**
Найдите поездки, в которых чаевые (`tip_amount`) составили строго больше 25% от базового тарифа (`fare_amount`). Выведите ID поездки и сумму чаевых. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `tip_amount`
<details><summary>Решение</summary><code>SELECT trip_id, tip_amount FROM trips WHERE tip_amount > (fare_amount * 0.25) LIMIT 5;</code></details>

**Задание 30: Поиск пользователей в диапазоне ID**
Маркетологи сформировали когорту пользователей. Выведите события (UserID, URL) для пользователей с ID от 500000 до 600000 включительно. Ограничьте вывод 5 записями.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `UserID`, `URL`
<details><summary>Решение</summary><code>SELECT UserID, URL FROM hits_UserID_URL WHERE UserID BETWEEN 500000 AND 600000 LIMIT 5;</code></details>

**Задание 31: Поездки с большим количеством пассажиров**
Выведите ID поездок, где находилось 5 или 6 пассажиров, и тип такси — 'yellow'. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE passenger_count IN (5, 6) AND cab_type = 'yellow' LIMIT 5;</code></details>

**Задание 32: Неизвестный способ оплаты**
Проверка данных на ошибки. Найдите поездки, где тип оплаты отмечен как неизвестный ('UNK'). Выведите ID поездки. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE payment_type = 'UNK' LIMIT 5;</code></details>

**Задание 33: Окончание URL**
Требуется найти старые веб-страницы. Выведите URL, которые заканчиваются на `.php`. Ограничьте вывод 5 записями.
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`
<details><summary>Решение</summary><code>SELECT URL FROM hits_URL_UserID WHERE URL LIKE '%.php' LIMIT 5;</code></details>

**Задание 34: Нулевой базовый тариф**
Найдите аномальные поездки, где базовый тариф (`fare_amount`) равен нулю, но итоговая стоимость (`total_amount`) больше нуля. Выведите ID и итоговую стоимость. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `total_amount`
<details><summary>Решение</summary><code>SELECT trip_id, total_amount FROM trips WHERE fare_amount = 0 AND total_amount > 0 LIMIT 5;</code></details>

**Задание 35: Исключение домена**
Выведите 5 событий (URL), где адрес сайта НЕ содержит строку `yandex`.
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`
<details><summary>Решение</summary><code>SELECT URL FROM hits_URL_UserID WHERE URL NOT LIKE '%yandex%' LIMIT 5;</code></details>

**Задание 36: Поездки в определенное время**
Выведите поездки (только ID и дату посадки), совершенные между '2016-01-10' и '2016-01-15'. Ограничьте 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `pickup_date`
<details><summary>Решение</summary><code>SELECT trip_id, pickup_date FROM trips WHERE pickup_date BETWEEN '2016-01-10' AND '2016-01-15' LIMIT 5;</code></details>

**Задание 37: Конкретные поставщики данных**
Выведите события, где `vendor_id` не равен ни 1, ни 2. Выведите только ID поездки. Ограничьте вывод 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE vendor_id NOT IN (1, 2) LIMIT 5;</code></details>

**Задание 38: Бот-трафик на конкретном сайте**
Требуется найти активность ботов на сайте. Выведите UserID, если `IsRobot` равен 1 и URL содержит `news`. Ограничьте вывод 5 записями.
*   **Схема:** `hits_URL_UserID_IsRobot`
*   **Поля:** `UserID`
<details><summary>Решение</summary><code>SELECT UserID FROM hits_URL_UserID_IsRobot WHERE IsRobot = 1 AND URL LIKE '%news%' LIMIT 5;</code></details>

**Задание 39: Фильтр по дистанции с границами**
Выведите ID поездки и дистанцию для поездок, где дистанция строго больше 10, но меньше или равна 12. Ограничьте 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `trip_distance`
<details><summary>Решение</summary><code>SELECT trip_id, trip_distance FROM trips WHERE trip_distance > 10 AND trip_distance <= 12 LIMIT 5;</code></details>

**Задание 40: Комбинация OR и AND**
Найдите поездки, которые либо совершались без пассажиров (`passenger_count` = 0), либо были полностью бесплатными (`total_amount` = 0). Выведите ID поездки. Ограничьте 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE passenger_count = 0 OR total_amount = 0 LIMIT 5;</code></details>


## Раздел 3: Агрегация, Группировка и HAVING (Задачи 41-60)

**Задание 41: Подсчет всех строк**
Рассчитайте абсолютное количество записей в таблице веб-трафика (без группировки).
*   **Схема:** `hits_NoPrimaryKey`
*   **Поля:** `count()`
<details><summary>Решение</summary><code>SELECT count() FROM hits_NoPrimaryKey;</code></details>

**Задание 42: Суммарная выручка**
Руководство просит узнать общий доход со всех поездок такси. Просуммируйте поле итоговой стоимости.
*   **Схема:** `trips`
*   **Поля:** `total_amount`
<details><summary>Решение</summary><code>SELECT sum(total_amount) FROM trips;</code></details>

**Задание 43: Средняя дистанция по типам такси**
Сгруппируйте данные по типу такси (`cab_type`) и посчитайте среднюю дистанцию поездки для каждого типа.
*   **Схема:** `trips`
*   **Поля:** `cab_type`, `trip_distance`
<details><summary>Решение</summary><code>SELECT cab_type, avg(trip_distance) FROM trips GROUP BY cab_type;</code></details>

**Задание 44: Самые активные пользователи**
Найдите пользователей с наибольшим количеством событий. Выведите `UserID` и количество событий. Отсортируйте по убыванию активности. Ограничьте результат 5 записями.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `UserID`, подсчет событий
<details><summary>Решение</summary><code>SELECT UserID, count() as cnt FROM hits_UserID_URL GROUP BY UserID ORDER BY cnt DESC LIMIT 5;</code></details>

**Задание 45: Выручка по дням**
Сгруппируйте данные по дате посадки (`pickup_date`) и просуммируйте `total_amount`. Отсортируйте результат по дате (по возрастанию). Ограничьте 5 записями.
*   **Схема:** `trips`
*   **Поля:** `pickup_date`, сумма
<details><summary>Решение</summary><code>SELECT pickup_date, sum(total_amount) FROM trips GROUP BY pickup_date ORDER BY pickup_date LIMIT 5;</code></details>

**Задание 46: Максимальные чаевые по типу оплаты**
Сгруппируйте поездки по типу оплаты (`payment_type`) и найдите максимальную сумму чаевых для каждого типа.
*   **Схема:** `trips`
*   **Поля:** `payment_type`, `tip_amount`
<details><summary>Решение</summary><code>SELECT payment_type, max(tip_amount) FROM trips GROUP BY payment_type;</code></details>

**Задание 47: Популярные URL (HAVING)**
Найдите URL, которые посетили более 5000 раз. Выведите URL и количество посещений, отсортируйте по количеству (по убыванию). Ограничьте 5 записями.
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`, счетчик
<details><summary>Решение</summary><code>SELECT URL, count() as cnt FROM hits_URL_UserID GROUP BY URL HAVING cnt > 5000 ORDER BY cnt DESC LIMIT 5;</code></details>

**Задание 48: Уникальные пользователи**
Определите охват аудитории: посчитайте точное количество уникальных пользователей (используйте функцию `uniq` или `count(DISTINCT)`).
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `UserID`
<details><summary>Решение</summary><code>SELECT uniq(UserID) FROM hits_UserID_URL;</code></details>

**Задание 49: Дорогие вендоры (HAVING)**
Сгруппируйте данные по поставщикам (`vendor_id`). Выведите только тех, у кого средняя итоговая стоимость поездки строго больше 15.
*   **Схема:** `trips`
*   **Поля:** `vendor_id`, средняя стоимость
<details><summary>Решение</summary><code>SELECT vendor_id, avg(total_amount) as avg_price FROM trips GROUP BY vendor_id HAVING avg_price > 15;</code></details>

**Задание 50: Трафик ботов и людей**
Используя таблицу классификации роботов, подсчитайте количество событий, сгруппировав их по флагу `IsRobot`.
*   **Схема:** `hits_IsRobot_UserID_URL`
*   **Поля:** `IsRobot`, счетчик
<details><summary>Решение</summary><code>SELECT IsRobot, count() FROM hits_IsRobot_UserID_URL GROUP BY IsRobot;</code></details>

**Задание 51: Минимальный тариф**
Для каждого количества пассажиров (`passenger_count`) найдите минимальную стоимость тарифа (`fare_amount`). Исключите поездки без пассажиров (0) из группировки (через WHERE). Сортировка по количеству пассажиров по возрастанию.
*   **Схема:** `trips`
*   **Поля:** `passenger_count`, `fare_amount`
<details><summary>Решение</summary><code>SELECT passenger_count, min(fare_amount) FROM trips WHERE passenger_count > 0 GROUP BY passenger_count ORDER BY passenger_count;</code></details>

**Задание 52: Количество пассажиров по типам такси**
Рассчитайте общее количество перевезенных пассажиров (`sum(passenger_count)`) для каждого типа такси (`cab_type`).
*   **Схема:** `trips`
*   **Поля:** `cab_type`, сумма пассажиров
<details><summary>Решение</summary><code>SELECT cab_type, sum(passenger_count) FROM trips GROUP BY cab_type;</code></details>

**Задание 53: Популярные домены (Группировка по LIKE)**
Это сложный запрос: попытайтесь сгруппировать количество событий по пользователям (`UserID`), но только для событий, где URL содержит `yandex`. Выведите 5 самых активных пользователей.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `UserID`, счетчик
<details><summary>Решение</summary><code>SELECT UserID, count() as cnt FROM hits_UserID_URL WHERE URL LIKE '%yandex%' GROUP BY UserID ORDER BY cnt DESC LIMIT 5;</code></details>

**Задание 54: Средние чаевые больше нуля**
Сгруппируйте поездки по дате (`pickup_date`). Рассчитайте средние чаевые, но учитывайте только те поездки, где чаевые были больше нуля (WHERE). Ограничьте 5 датами.
*   **Схема:** `trips`
*   **Поля:** `pickup_date`, средние чаевые
<details><summary>Решение</summary><code>SELECT pickup_date, avg(tip_amount) FROM trips WHERE tip_amount > 0 GROUP BY pickup_date LIMIT 5;</code></details>

**Задание 55: Многократные посетители**
Найдите пользователей, которые зашли на один и тот же URL более 10 раз. Сгруппируйте по `UserID` и `URL`. Выведите `UserID`, `URL` и количество посещений. Ограничьте 5 записями.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `UserID`, `URL`, счетчик
<details><summary>Решение</summary><code>SELECT UserID, URL, count() as visits FROM hits_UserID_URL GROUP BY UserID, URL HAVING visits > 10 LIMIT 5;</code></details>

**Задание 56: Общее пройденное расстояние**
Служба ТО просит рассчитать общую пройденную дистанцию (`sum(trip_distance)`) для каждой категории такси (`cab_type`).
*   **Схема:** `trips`
*   **Поля:** `cab_type`, дистанция
<details><summary>Решение</summary><code>SELECT cab_type, sum(trip_distance) FROM trips GROUP BY cab_type;</code></details>

**Задание 57: Средний налог**
Рассчитайте средний налог `mta_tax` для каждого типа оплаты (`payment_type`).
*   **Схема:** `trips`
*   **Поля:** `payment_type`, средний налог
<details><summary>Решение</summary><code>SELECT payment_type, avg(mta_tax) FROM trips GROUP BY payment_type;</code></details>

**Задание 58: Фильтрация после группировки**
Выведите даты (`pickup_date`), в которые суммарная выручка (`sum(total_amount)`) превысила 50000.
*   **Схема:** `trips`
*   **Поля:** `pickup_date`, сумма
<details><summary>Решение</summary><code>SELECT pickup_date, sum(total_amount) as total FROM trips GROUP BY pickup_date HAVING total > 50000;</code></details>

**Задание 59: Подсчет уникальных URL для пользователя**
Рассчитайте, сколько уникальных страниц (URL) посетил каждый пользователь. Выведите 5 пользователей с наибольшим разнообразием посещенных страниц.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `UserID`, уникальные URL
<details><summary>Решение</summary><code>SELECT UserID, uniq(URL) as unique_urls FROM hits_UserID_URL GROUP BY UserID ORDER BY unique_urls DESC LIMIT 5;</code></details>

**Задание 60: Выручка по кодам тарифов**
Сгруппируйте по коду тарифа (`rate_code_id`) и выведите суммарную стоимость поездок (`total_amount`).
*   **Схема:** `trips`
*   **Поля:** `rate_code_id`, сумма
<details><summary>Решение</summary><code>SELECT rate_code_id, sum(total_amount) FROM trips GROUP BY rate_code_id;</code></details>


## Раздел 4: Функции дат и обработки строк (Задачи 61-80)

**Задание 61: Извлечение часа**
В какое время люди чаще всего вызывают такси? Извлеките час из `pickup_datetime` (используйте `toHour`) и посчитайте количество поездок для каждого часа. Отсортируйте по количеству убыванию. Ограничьте 5 записями.
*   **Схема:** `trips`
*   **Поля:** `pickup_datetime`, счетчик
<details><summary>Решение</summary><code>SELECT toHour(pickup_datetime) as h, count() as cnt FROM trips GROUP BY h ORDER BY cnt DESC LIMIT 5;</code></details>

**Задание 62: Год события**
Сгруппируйте события веб-трафика по годам (используйте `toYear` от поля `EventTime`). Выведите год и количество событий.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `EventTime`, счетчик
<details><summary>Решение</summary><code>SELECT toYear(EventTime) as year, count() FROM hits_UserID_URL GROUP BY year;</code></details>

**Задание 63: День недели**
Проанализируйте активность такси по дням недели (используйте `toDayOfWeek` от `pickup_date`). Выведите номер дня и количество поездок.
*   **Схема:** `trips`
*   **Поля:** `pickup_date`, счетчик
<details><summary>Решение</summary><code>SELECT toDayOfWeek(pickup_date) as dow, count() FROM trips GROUP BY dow;</code></details>

**Задание 64: Продолжительность поездки в секундах**
Рассчитайте длительность поездки в секундах, используя `dateDiff('second', start, end)`. Выведите ID поездки и длительность для 5 записей.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `pickup_datetime`, `dropoff_datetime`
<details><summary>Решение</summary><code>SELECT trip_id, dateDiff('second', pickup_datetime, dropoff_datetime) as duration FROM trips LIMIT 5;</code></details>

**Задание 65: Нижний регистр строк**
Для унификации приведите все `URL` к нижнему регистру (функция `lower`). Выведите 5 уникальных преобразованных URL.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `URL`
<details><summary>Решение</summary><code>SELECT DISTINCT lower(URL) FROM hits_UserID_URL LIMIT 5;</code></details>

**Задание 66: Длина строки URL**
Найдите самые длинные ссылки. Рассчитайте длину `URL` (функция `length`) и выведите сам URL и его длину. Отсортируйте по длине по убыванию. Ограничьте 5 записями.
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`
<details><summary>Решение</summary><code>SELECT URL, length(URL) as len FROM hits_URL_UserID ORDER BY len DESC LIMIT 5;</code></details>

**Задание 67: Округление стоимости**
Выведите ID поездки и ее стоимость, округленную до целого числа (функция `round`), для 5 первых записей.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `total_amount`
<details><summary>Решение</summary><code>SELECT trip_id, round(total_amount) FROM trips LIMIT 5;</code></details>

**Задание 68: Извлечение месяца**
Посчитайте количество событий веб-трафика по месяцам (функция `toMonth` от `EventTime`).
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `EventTime`, счетчик
<details><summary>Решение</summary><code>SELECT toMonth(EventTime) as m, count() FROM hits_UserID_URL GROUP BY m;</code></details>

**Задание 69: Форматирование даты**
Преобразуйте `pickup_datetime` в формат строки `YYYY-MM-DD` (функция `formatDate(col, '%Y-%m-%d')`). Выведите преобразованную дату для 5 поездок.
*   **Схема:** `trips`
*   **Поля:** `pickup_datetime`
<details><summary>Решение</summary><code>SELECT formatDate(pickup_datetime, '%Y-%m-%d') FROM trips LIMIT 5;</code></details>

**Задание 70: Верхний регистр**
Выведите названия типов оплаты (`payment_type`), преобразованные в верхний регистр (функция `upper`), без дубликатов.
*   **Схема:** `trips`
*   **Поля:** `payment_type`
<details><summary>Решение</summary><code>SELECT DISTINCT upper(payment_type) FROM trips;</code></details>

**Задание 71: День месяца**
Сколько поездок совершается 1-го числа каждого месяца? Отфильтруйте поездки с помощью функции `toDayOfMonth(pickup_date) = 1` и подсчитайте их количество.
*   **Схема:** `trips`
*   **Поля:** `pickup_date`, счетчик
<details><summary>Решение</summary><code>SELECT count() FROM trips WHERE toDayOfMonth(pickup_date) = 1;</code></details>

**Задание 72: Продолжительность в минутах**
Найдите среднюю продолжительность поездки в минутах (`dateDiff` с аргументом 'minute').
*   **Схема:** `trips`
*   **Поля:** `pickup_datetime`, `dropoff_datetime`
<details><summary>Решение</summary><code>SELECT avg(dateDiff('minute', pickup_datetime, dropoff_datetime)) FROM trips;</code></details>

**Задание 73: Извлечение подстроки (префикса)**
С помощью функции `substring(URL, 1, 4)` извлеките первые 4 символа из URL. Выведите уникальные префиксы (ограничьте 5 записями).
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`
<details><summary>Решение</summary><code>SELECT DISTINCT substring(URL, 1, 4) FROM hits_URL_UserID LIMIT 5;</code></details>

**Задание 74: Срез по годам для такси**
Извлеките год из даты посадки (`pickup_date`) и найдите максимальную стоимость поездки (`total_amount`) в каждом году.
*   **Схема:** `trips`
*   **Поля:** `pickup_date`, `total_amount`
<details><summary>Решение</summary><code>SELECT toYear(pickup_date) as y, max(total_amount) FROM trips GROUP BY y;</code></details>

**Задание 75: Округление вверх**
Функция `ceil` округляет число вверх. Рассчитайте среднюю сумму `total_amount` по `passenger_count` и округлите результат вверх. Отсортируйте по количеству пассажиров.
*   **Схема:** `trips`
*   **Поля:** `passenger_count`, `total_amount`
<details><summary>Решение</summary><code>SELECT passenger_count, ceil(avg(total_amount)) FROM trips GROUP BY passenger_count ORDER BY passenger_count;</code></details>

**Задание 76: Время поездки больше часа**
Найдите поездки, которые длились больше часа (разница в минутах больше 60). Выведите ID поездки. Ограничьте 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `pickup_datetime`, `dropoff_datetime`
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE dateDiff('minute', pickup_datetime, dropoff_datetime) > 60 LIMIT 5;</code></details>

**Задание 77: Хеширование строк**
Сгенерируйте хеш-код для URL с помощью встроенной функции `cityHash64`. Выведите исходный URL и его хеш. Ограничьте 5 записями.
*   **Схема:** `hits_URL_UserID`
*   **Поля:** `URL`
<details><summary>Решение</summary><code>SELECT URL, cityHash64(URL) FROM hits_URL_UserID LIMIT 5;</code></details>

**Задание 78: Усечение до часа**
Функция `toStartOfHour` округляет время до начала часа. Сгруппируйте события по началу часа и подсчитайте их. Отсортируйте по убыванию и выведите 5 топ-часов.
*   **Схема:** `hits_UserID_URL`
*   **Поля:** `EventTime`
<details><summary>Решение</summary><code>SELECT toStartOfHour(EventTime) as h, count() as c FROM hits_UserID_URL GROUP BY h ORDER BY c DESC LIMIT 5;</code></details>

**Задание 79: Объединение строк (Конкатенация)**
Объедините строки: создайте поле "Тип: [cab_type]". Используйте оператор `||` или функцию `concat`. Выведите 5 уникальных значений.
*   **Схема:** `trips`
*   **Поля:** `cab_type`
<details><summary>Решение</summary><code>SELECT DISTINCT concat('Тип: ', cab_type) FROM trips LIMIT 5;</code></details>

**Задание 80: Извлечение квартала**
Сгруппируйте данные о такси по кварталам года посадки (`toQuarter(pickup_date)`). Выведите номер квартала и количество поездок.
*   **Схема:** `trips`
*   **Поля:** `pickup_date`
<details><summary>Решение</summary><code>SELECT toQuarter(pickup_date) as q, count() FROM trips GROUP BY q;</code></details>


## Раздел 5: CASE, JOIN и Словари (Задачи 81-100)

**Задание 81: Классификация поездок (CASE)**
С помощью конструкции `CASE` разбейте поездки на категории по дистанции: если `trip_distance` < 2, то 'Short', если <= 5, то 'Medium', иначе 'Long'. Выведите ID поездки и категорию для 5 записей.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `trip_distance`
<details><summary>Решение</summary><code>SELECT trip_id, CASE WHEN trip_distance < 2 THEN 'Short' WHEN trip_distance <= 5 THEN 'Medium' ELSE 'Long' END as dist_type FROM trips LIMIT 5;</code></details>

**Задание 82: Классификация трафика**
Используя таблицу классификации роботов, выведите URL и текстовый тип пользователя: если `IsRobot` = 1, то 'Bot', иначе 'Human'. Выведите 5 записей.
*   **Схема:** `hits_URL_UserID_IsRobot`
*   **Поля:** `URL`, `IsRobot`
<details><summary>Решение</summary><code>SELECT URL, CASE WHEN IsRobot = 1 THEN 'Bot' ELSE 'Human' END as type FROM hits_URL_UserID_IsRobot LIMIT 5;</code></details>

**Задание 83: Расшифровка вендоров**
Используйте `CASE` для расшифровки `vendor_id`: 1 — 'Creative Mobile', 2 — 'VeriFone', остальные — 'Unknown'. Выведите ID поездки и название вендора (5 записей).
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `vendor_id`
<details><summary>Решение</summary><code>SELECT trip_id, CASE WHEN vendor_id = 1 THEN 'Creative Mobile' WHEN vendor_id = 2 THEN 'VeriFone' ELSE 'Unknown' END as vendor_name FROM trips LIMIT 5;</code></details>

**Задание 84: Соединение с районом (INNER JOIN)**
Свяжите таблицу поездок со словарем `taxi_zone_dictionary`. Соединение (JOIN) производится по условию `toUInt64(trips.pickup_nyct2010_gid) = taxi_zone_dictionary.LocationID`. Выведите `trip_id` и `Zone` (название района). Ограничьте 5 записями.
*   **Схема:** `trips`, `taxi_zone_dictionary`
*   **Поля:** `trip_id`, `pickup_nyct2010_gid`, `LocationID`, `Zone`
<details><summary>Решение</summary><code>SELECT t.trip_id, d.Zone FROM trips t JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID LIMIT 5;</code></details>

**Задание 85: Агрегация с JOIN**
Подсчитайте количество поездок для каждого района посадки (`Borough`), используя INNER JOIN со словарем. Отсортируйте по убыванию поездок. Ограничьте 5 записями.
*   **Схема:** `trips`, `taxi_zone_dictionary`
*   **Поля:** `pickup_nyct2010_gid`, `LocationID`, `Borough`
<details><summary>Решение</summary><code>SELECT d.Borough, count() as cnt FROM trips t JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID GROUP BY d.Borough ORDER BY cnt DESC LIMIT 5;</code></details>

**Задание 86: Извлечение значения словаря (dictGet)**
В ClickHouse вместо JOIN часто используют функции словаря. Выведите ID поездки и район (`Borough`), используя функцию `dictGet('taxi_zone_dictionary', 'Borough', toUInt64(pickup_nyct2010_gid))`. Ограничьте 5 записями.
*   **Схема:** `trips`, словарь `taxi_zone_dictionary`
*   **Поля:** `trip_id`, `pickup_nyct2010_gid`
<details><summary>Решение</summary><code>SELECT trip_id, dictGet('taxi_zone_dictionary', 'Borough', toUInt64(pickup_nyct2010_gid)) FROM trips LIMIT 5;</code></details>

**Задание 87: Проверка наличия в словаре (dictHas)**
Проверьте, существует ли в словаре `taxi_zone_dictionary` ключ `LocationID`, равный 132. Запрос должен вернуть 1 (true) или 0 (false).
*   **Схема:** `taxi_zone_dictionary`
*   **Поля:** `dictHas`
<details><summary>Решение</summary><code>SELECT dictHas('taxi_zone_dictionary', 132);</code></details>

**Задание 88: Безопасное извлечение из словаря (dictGetOrDefault)**
Подсчитайте количество поездок по `Borough`, используя `dictGetOrDefault` (если код неизвестен, выводить 'Unknown'). Группируйте по результату функции.
*   **Схема:** `trips`
*   **Поля:** `pickup_nyct2010_gid`
<details><summary>Решение</summary><code>SELECT dictGetOrDefault('taxi_zone_dictionary', 'Borough', toUInt64(pickup_nyct2010_gid), 'Unknown') as b_name, count() FROM trips GROUP BY b_name;</code></details>

**Задание 89: LEFT JOIN поездок и словаря**
Найдите поездки, для которых в словаре нет названия района. Выполните LEFT JOIN и отфильтруйте строки, где `d.Zone` имеет пустое значение (`''` или `IS NULL`). Выведите ID поездки (5 штук).
*   **Схема:** `trips`, `taxi_zone_dictionary`
*   **Поля:** `trip_id`, `Zone`
<details><summary>Решение</summary><code>SELECT t.trip_id FROM trips t LEFT JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID WHERE d.Zone = '' LIMIT 5;</code></details>

**Задание 90: Поездки в аэропорт JFK через JOIN**
Известно, что LocationID = 132 — это аэропорт JFK. Выполните JOIN таблицы поездок (по `dropoff_nyct2010_gid`) и словаря. Отфильтруйте по `Zone = 'JFK Airport'`. Выведите 5 ID поездок.
*   **Схема:** `trips`, `taxi_zone_dictionary`
*   **Поля:** `trip_id`, `dropoff_nyct2010_gid`, `Zone`
<details><summary>Решение</summary><code>SELECT t.trip_id FROM trips t JOIN taxi_zone_dictionary d ON toUInt64(t.dropoff_nyct2010_gid) = d.LocationID WHERE d.Zone = 'JFK Airport' LIMIT 5;</code></details>

**Задание 91: Множественный CASE**
Классифицируйте поездки по чаевым: 0 — 'No tips', до 2 — 'Low', до 5 — 'Medium', больше 5 — 'High'. Выведите `tip_amount` и класс для 5 записей.
*   **Схема:** `trips`
*   **Поля:** `tip_amount`
<details><summary>Решение</summary><code>SELECT tip_amount, CASE WHEN tip_amount = 0 THEN 'No tips' WHEN tip_amount <= 2 THEN 'Low' WHEN tip_amount <= 5 THEN 'Medium' ELSE 'High' END as tip_class FROM trips LIMIT 5;</code></details>

**Задание 92: Исключение выходных через CASE**
Создайте флаг `is_weekend` (1 если суббота или воскресенье, 0 в остальные дни) на основе `pickup_date`. Выведите дату и флаг (5 записей).
*   **Схема:** `trips`
*   **Поля:** `pickup_date`
<details><summary>Решение</summary><code>SELECT pickup_date, CASE WHEN toDayOfWeek(pickup_date) IN (6,7) THEN 1 ELSE 0 END as is_weekend FROM trips LIMIT 5;</code></details>

**Задание 93: Средняя стоимость по районам (JOIN)**
Используя INNER JOIN со словарем, найдите среднюю итоговую стоимость поездки (`total_amount`) для района (`Borough`) 'Manhattan'.
*   **Схема:** `trips`, `taxi_zone_dictionary`
*   **Поля:** `pickup_nyct2010_gid`, `Borough`, `total_amount`
<details><summary>Решение</summary><code>SELECT avg(t.total_amount) FROM trips t JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID WHERE d.Borough = 'Manhattan';</code></details>

**Задание 94: Выручка по часам с учетом типа такси**
Сложная агрегация: сгруппируйте по часу посадки и типу такси (`cab_type`). Выведите час, тип и сумму `total_amount`. Сортировка по часу и типу (по 5 записей).
*   **Схема:** `trips`
*   **Поля:** `pickup_datetime`, `cab_type`, `total_amount`
<details><summary>Решение</summary><code>SELECT toHour(pickup_datetime) as h, cab_type, sum(total_amount) FROM trips GROUP BY h, cab_type ORDER BY h, cab_type LIMIT 5;</code></details>

**Задание 95: Кросс-соединение (Концепция)**
*(Техническое задание)* В ClickHouse доступно `CROSS JOIN`. Объедините таблицу поездок (отфильтрованную до 1 записи) и словарь (отфильтрованный до 1 записи) через CROSS JOIN. Выведите `trip_id` и `Zone`.
*   **Схема:** `trips`, `taxi_zone_dictionary`
*   **Поля:** `trip_id`, `Zone`
<details><summary>Решение</summary><code>SELECT t.trip_id, d.Zone FROM (SELECT trip_id FROM trips LIMIT 1) t CROSS JOIN (SELECT Zone FROM taxi_zone_dictionary LIMIT 1) d;</code></details>

**Задание 96: Использование подзапроса в FROM**
Напишите запрос, который выбирает `trip_id` и `fare_amount` из подзапроса `(SELECT * FROM trips WHERE passenger_count > 3)`. Ограничьте 5 записями.
*   **Схема:** `trips`
*   **Поля:** `trip_id`, `fare_amount`
<details><summary>Решение</summary><code>SELECT trip_id, fare_amount FROM (SELECT * FROM trips WHERE passenger_count > 3) LIMIT 5;</code></details>

**Задание 97: Использование IN с подзапросом**
Найдите 5 поездок (выведите `trip_id`), где `pickup_nyct2010_gid` находится в списке ID районов (`LocationID`) боро 'Brooklyn' из словаря.
*   **Схема:** `trips`, `taxi_zone_dictionary`
*   **Поля:** `trip_id`, `pickup_nyct2010_gid`, `Borough`
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE toUInt64(pickup_nyct2010_gid) IN (SELECT LocationID FROM taxi_zone_dictionary WHERE Borough = 'Brooklyn') LIMIT 5;</code></details>

**Задание 98: Доля чаевых через математику и агрегацию**
Посчитайте глобальную долю чаевых (сумма `tip_amount` деленная на сумму `fare_amount` для всей таблицы).
*   **Схема:** `trips`
*   **Поля:** `tip_amount`, `fare_amount`
<details><summary>Решение</summary><code>SELECT sum(tip_amount) / sum(fare_amount) FROM trips;</code></details>

**Задание 99: Получение всей строки словаря**
Для проверки словаря просто выведите все колонки словаря `taxi_zone_dictionary` для 5 первых записей.
*   **Схема:** `taxi_zone_dictionary`
*   **Поля:** `*`
<details><summary>Решение</summary><code>SELECT * FROM taxi_zone_dictionary LIMIT 5;</code></details>

**Задание 100: Комплексный запрос (Аналитика)**
Найдите самый популярный район назначения (`dropoff`) для поездок, начинающихся в 'Manhattan'. Используйте словарь для получения названий `Borough` (через `dictGet`). Отсортируйте по популярности (убывание), выведите ТОП-1.
*   **Схема:** `trips`, `taxi_zone_dictionary`
*   **Поля:** `pickup_nyct2010_gid`, `dropoff_nyct2010_gid`
<details><summary>Решение</summary><code>SELECT dictGet('taxi_zone_dictionary', 'Borough', toUInt64(dropoff_nyct2010_gid)) as drop_borough, count() as cnt FROM trips WHERE dictGet('taxi_zone_dictionary', 'Borough', toUInt64(pickup_nyct2010_gid)) = 'Manhattan' GROUP BY drop_borough ORDER BY cnt DESC LIMIT 1;</code></details>

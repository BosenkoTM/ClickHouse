# Практическая работа №2. Первичные индексы, типы данных и базовые запросы в ClickHouse

## Введение в индексацию ClickHouse
В отличие от традиционных реляционных БД (PostgreSQL, MySQL), где индекс строится для каждой строки (B-Tree), ClickHouse использует **разреженные индексы**.

**Основные понятия:**
*   **Гранула (Granule).** Минимальный неделимый набор строк (по умолчанию 8192), который ClickHouse считывает за один раз.
*   **Метка (Mark).** Запись в индексе, указывающая на начало гранулы.
*   **Разреженный индекс.** Хранит данные не для каждой строки, а только для первой строки каждой гранулы. Это позволяет индексу целиком помещаться в оперативной памяти.

## Создание таблиц и влияние первичного ключа

### Задача 1. Полное сканирование (Full Scan)
Создадим таблицу без первичного ключа и посмотрим, как работает поиск.

```sql
CREATE TABLE hits_NoPrimaryKey
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY tuple(); -- Пустой ключ
```

При запросе `SELECT ... WHERE UserID = 749927693` ClickHouse будет вынужден прочитать **все** 8.87 млн строк. Это неэффективно.

### Задача 2. Таблица с первичным ключом
Создадим таблицу, где данные физически отсортированы по `UserID` и `URL`.

```sql
CREATE TABLE hits_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY (UserID, URL)
ORDER BY (UserID, URL, EventTime);
```

**Почему это важно:**
1.  Данные на диске хранятся в порядке `UserID -> URL -> EventTime`.
2.  ClickHouse использует **бинарный поиск** по индексу, если фильтр стоит по первому столбцу ключа (`UserID`).
3.  Если фильтр стоит по второму столбцу (`URL`), используется **алгоритм исключения**, который эффективен только если у первого столбца (`UserID`) низкая кардинальность (мало уникальных значений).

## Оптимизация. Проекции и Материализованные представления
Если нам нужно быстро искать и по `UserID`, и по `URL`, мы используем дублирование данных с разным порядком сортировки.

**Создание проекции:**
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

## Базовый синтаксис SQL в ClickHouse

### Извлечение данных (SELECT, FROM, LIMIT)
```sql
SELECT URL, EventTime 
FROM hits_UserID_URL 
LIMIT 10;
```

### Фильтрация и сортировка (WHERE, ORDER BY)
```sql
SELECT UserID, URL 
FROM hits_UserID_URL 
WHERE IsRobot = 1 
ORDER BY EventTime DESC;
```

### Агрегация (GROUP BY, HAVING)
```sql
SELECT UserID, count() as total_clicks
FROM hits_UserID_URL
GROUP BY UserID
HAVING total_clicks > 100
ORDER BY total_clicks DESC;
```

---

# 50 Тестовых заданий по теме "SQL и ClickHouse"

Ниже приведены задачи для самопроверки. Решения скрыты под спойлерами.

### Блок 1. Базовые запросы (1-10)

1. **Задача.** Выведите все столбцы из таблицы `trips`, ограничив результат первыми 5 строками.
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips LIMIT 5;</code>
</details>

2. **Задача.** Выведите уникальные значения `vendor_id` из таблицы `trips`.
<details>
<summary>Решение</summary>
<code>SELECT DISTINCT vendor_id FROM trips;</code>
</details>

3. **Задача.** Выведите `trip_id` и `fare_amount`, отсортировав их по стоимости от самой дорогой к дешевой.
<details>
<summary>Решение</summary>
<code>SELECT trip_id, fare_amount FROM trips ORDER BY fare_amount DESC;</code>
</details>

4. **Задача.** Посчитайте общее количество строк в таблице `hits_NoPrimaryKey`.
<details>
<summary>Решение</summary>
<code>SELECT count() FROM hits_NoPrimaryKey;</code>
</details>

5. **Задача.** Выведите `pickup_datetime` и `total_amount`, переименовав `total_amount` в `revenue`.
<details>
<summary>Решение</summary>
<code>SELECT pickup_datetime, total_amount AS revenue FROM trips;</code>
</details>

6. **Задача.** Выведите 10 самых длинных дистанций поездок (`trip_distance`).
<details>
<summary>Решение</summary>
<code>SELECT trip_distance FROM trips ORDER BY trip_distance DESC LIMIT 10;</code>
</details>

7. **Задача.** Выведите идентификаторы поездок, где стоимость (`fare_amount`) ровно 50.
<details>
<summary>Решение</summary>
<code>SELECT trip_id FROM trips WHERE fare_amount = 50;</code>
</details>

8. **Задача.** Выведите список всех `URL` без повторений.
<details>
<summary>Решение</summary>
<code>SELECT DISTINCT URL FROM hits_UserID_URL;</code>
</details>

9. **Задача:** Выведите `UserID`, отсортированный по возрастанию.
<details>
<summary>Решение</summary>
<code>SELECT UserID FROM hits_UserID_URL ORDER BY UserID ASC;</code>
</details>

10. **Задача:** Выведите все данные о поездках, совершенных `cab_type` = 'yellow'.
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE cab_type = 'yellow';</code>
</details>

---

### Блок 2: Фильтрация (WHERE) и типы данных (11-20)

11. **Задача:** Найти поездки с количеством пассажиров больше 4 и стоимостью меньше 20.
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE passenger_count > 4 AND fare_amount < 20;</code>
</details>

12. **Задача:** Выведите поездки, совершенные в период между '2016-01-01' и '2016-01-02'.
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE pickup_date BETWEEN '2016-01-01' AND '2016-01-02';</code>
</details>

13. **Задача:** Найти все записи в `hits`, где `URL` содержит слово 'google'.
<details>
<summary>Решение</summary>
<code>SELECT * FROM hits_UserID_URL WHERE URL LIKE '%google%';</code>
</details>

14. **Задача:** Выведите поездки, где `payment_type` является 'CSH' или 'CRE'.
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE payment_type IN ('CSH', 'CRE');</code>
</details>

15. **Задача:** Найти поездки, где не указаны чаевые (`tip_amount` равен 0).
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE tip_amount = 0;</code>
</details>

16. **Задача:** Выберите поездки, где дистанция больше 10 миль, но меньше 100.
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE trip_distance > 10 AND trip_distance < 100;</code>
</details>

17. **Задача:** Выведите `trip_id`, где `vendor_id` НЕ равен 1.
<details>
<summary>Решение</summary>
<code>SELECT trip_id FROM trips WHERE vendor_id != 1;</code>
</details>

18. **Задача:** Найти поездки, где общая сумма (`total_amount`) больше 1000.
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE total_amount > 1000;</code>
</details>

19. **Задача:** Выведите `URL`, которые заканчиваются на '.ru'.
<details>
<summary>Решение</summary>
<code>SELECT URL FROM hits_UserID_URL WHERE URL LIKE '%.ru';</code>
</details>

20. **Задача:** Найти пользователей (`UserID`), у которых идентификатор в диапазоне от 100 до 500.
<details>
<summary>Решение</summary>
<code>SELECT DISTINCT UserID FROM hits_UserID_URL WHERE UserID >= 100 AND UserID <= 500;</code>
</details>

---

### Блок 3: Агрегация и группировка (21-30)

21. **Задача:** Посчитайте среднюю стоимость поездки (`total_amount`).
<details>
<summary>Решение</summary>
<code>SELECT avg(total_amount) FROM trips;</code>
</details>

22. **Задача:** Найдите максимальное количество пассажиров в одной поездке.
<details>
<summary>Решение</summary>
<code>SELECT max(passenger_count) FROM trips;</code>
</details>

23. **Задача:** Посчитайте количество поездок для каждого `cab_type`.
<details>
<summary>Решение</summary>
<code>SELECT cab_type, count() FROM trips GROUP BY cab_type;</code>
</details>

24. **Задача:** Найдите суммарную выручку (`total_amount`) по каждой дате `pickup_date`.
<details>
<summary>Решение</summary>
<code>SELECT pickup_date, sum(total_amount) FROM trips GROUP BY pickup_date;</code>
</details>

25. **Задача:** Выведите `vendor_id`, у которых средняя дистанция поездки больше 5 миль.
<details>
<summary>Решение</summary>
<code>SELECT vendor_id, avg(trip_distance) as avg_dist FROM trips GROUP BY vendor_id HAVING avg_dist > 5;</code>
</details>

26. **Задача:** Посчитайте количество уникальных пользователей в таблице `hits`.
<details>
<summary>Решение</summary>
<code>SELECT uniq(UserID) FROM hits_UserID_URL;</code>
</details>

27. **Задача:** Найти топ-3 даты с самым большим количеством поездок.
<details>
<summary>Решение</summary>
<code>SELECT pickup_date, count() as cnt FROM trips GROUP BY pickup_date ORDER BY cnt DESC LIMIT 3;</code>
</details>

28. **Задача:** Рассчитать общую сумму доплат (`extra`) для каждого типа оплаты.
<details>
<summary>Решение</summary>
<code>SELECT payment_type, sum(extra) FROM trips GROUP BY payment_type;</code>
</details>

29. **Задача:** Найти минимальную и максимальную стоимость поездки.
<details>
<summary>Решение</summary>
<code>SELECT min(total_amount), max(total_amount) FROM trips;</code>
</details>

30. **Задача:** Посчитать средние чаевые для поездок, где пассажиров больше 2.
<details>
<summary>Решение</summary>
<code>SELECT avg(tip_amount) FROM trips WHERE passenger_count > 2;</code>
</details>

---

### Блок 4: Даты и строки (31-40)

31. **Задача:** Извлечь год из `pickup_datetime`.
<details>
<summary>Решение</summary>
<code>SELECT toYear(pickup_datetime) FROM trips LIMIT 5;</code>
</details>

32. **Задача:** Извлечь час совершения поездки.
<details>
<summary>Решение</summary>
<code>SELECT toHour(pickup_datetime) FROM trips LIMIT 5;</code>
</details>

33. **Задача:** Посчитать количество поездок по месяцам.
<details>
<summary>Решение</summary>
<code>SELECT toMonth(pickup_datetime) as m, count() FROM trips GROUP BY m;</code>
</details>

34. **Задача:** Выведите `pickup_datetime` в формате 'YYYY-MM-DD'.
<details>
<summary>Решение</summary>
<code>SELECT formatDate(pickup_datetime, '%Y-%m-%d') FROM trips;</code>
</details>

35. **Задача:** Найти поездки, совершенные в воскресенье (номер дня недели 7).
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE toDayOfWeek(pickup_datetime) = 7;</code>
</details>

36. **Задача:** Перевести `URL` в нижний регистр.
<details>
<summary>Решение</summary>
<code>SELECT lower(URL) FROM hits_UserID_URL LIMIT 5;</code>
</details>

37. **Задача:** Посчитать длину каждого `URL`.
<details>
<summary>Решение</summary>
<code>SELECT length(URL) FROM hits_UserID_URL LIMIT 5;</code>
</details>

38. **Задача:** Округлить `total_amount` до целого числа.
<details>
<summary>Решение</summary>
<code>SELECT round(total_amount) FROM trips;</code>
</details>

39. **Задача:** Найти разницу в секундах между посадкой и высадкой.
<details>
<summary>Решение</summary>
<code>SELECT dateDiff('second', pickup_datetime, dropoff_datetime) FROM trips;</code>
</details>

40. **Задача:** Проверить, является ли дата `pickup_date` первым числом месяца.
<details>
<summary>Решение</summary>
<code>SELECT * FROM trips WHERE toDayOfMonth(pickup_date) = 1;</code>
</details>

---

### Блок 5: Кейсы, Join и Индексы (41-50)

41. **Задача:** С помощью `CASE` классифицируйте поездки: если `total_amount` < 10 — 'Cheap', иначе 'Expensive'.
<details>
<summary>Решение</summary>
<code>SELECT total_amount, CASE WHEN total_amount < 10 THEN 'Cheap' ELSE 'Expensive' END as type FROM trips;</code>
</details>

42. **Задача:** Соедините таблицу `trips` со словарем `taxi_zone_dictionary` по `pickup_nyct2010_gid`.
<details>
<summary>Решение</summary>
<code>SELECT t.*, d.Borough FROM trips t JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID;</code>
</details>

43. **Задача:** Выведите название района (`Borough`) из словаря для `LocationID` = 132 (используя `dictGet`).
<details>
<summary>Решение</summary>
<code>SELECT dictGet('taxi_zone_dictionary', 'Borough', 132);</code>
</details>

44. **Задача:** Почему запрос `WHERE URL = '...'` работает медленно в таблице с ключом `(UserID, URL)`?
<details>
<summary>Решение</summary>
<code>Потому что URL - второй столбец в ключе, и ClickHouse не может использовать эффективный бинарный поиск, если не указан UserID.</code>
</details>

45. **Задача:** Как принудительно объединить части таблицы в MergeTree?
<details>
<summary>Решение</summary>
<code>OPTIMIZE TABLE table_name FINAL;</code>
</details>

46. **Задача:** Какая функция в ClickHouse используется для генерации хеша от строки?
<details>
<summary>Решение</summary>
<code>cityHash64(string) или farmHash64(string).</code>
</details>

47. **Задача:** Выполните `LEFT JOIN` между `trips` и словарем, чтобы оставить все поездки, даже если района нет в словаре.
<details>
<summary>Решение</summary>
<code>SELECT t.trip_id, d.Borough FROM trips t LEFT JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID;</code>
</details>

48. **Задача:** Напишите запрос, который возвращает 1, если в словаре есть ключ 132, и 0 если нет.
<details>
<summary>Решение</summary>
<code>SELECT dictHas('taxi_zone_dictionary', 132);</code>
</details>

49. **Задача:** Как посмотреть размер первичного индекса таблицы в памяти?
<details>
<summary>Решение</summary>
<code>SELECT primary_key_bytes_in_memory FROM system.parts WHERE table = 'имя_таблицы';</code>
</details>

50. **Задача:** Создайте материализованное представление, которое считает сумму `total_amount` по дням.
<details>
<summary>Решение</summary>
<code>CREATE MATERIALIZED VIEW daily_revenue ENGINE = SummingMergeTree() ORDER BY pickup_date AS SELECT pickup_date, sum(total_amount) as revenue FROM trips GROUP BY pickup_date;</code>
</details>
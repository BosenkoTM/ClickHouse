# Практическая работа №1. Первичные индексы, типы данных и базовые запросы в ClickHouse

## 1. Введение в индексацию ClickHouse
В отличие от традиционных реляционных БД (PostgreSQL, MySQL), где индекс строится для каждой строки (B-Tree), ClickHouse использует **разреженные индексы**.

**Основные понятия:**
*   **Гранула (Granule).** Минимальный неделимый набор строк (по умолчанию 8192), который ClickHouse считывает за один раз.
*   **Метка (Mark).** Запись в индексе, указывающая на начало гранулы.
*   **Разреженный индекс.** Хранит данные не для каждой строки, а только для первой строки каждой гранулы. Это позволяет индексу целиком помещаться в оперативной памяти.

## 2. Создание таблиц и влияние первичного ключа

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

## 3. Оптимизация. Проекции и Материализованные представления
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

## 4. Базовый синтаксис SQL в ClickHouse

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

Ниже представлены 50 компактных заданий для системы **CodeRunner**. Каждое задание содержит: схему, необходимые поля, постановку задачи и свернутое решение.

---

### Блок 1: Базовая выборка и сортировка

1. **Схема:** `trips`. **Поля:** `trip_id`, `total_amount`. **Задача:** Вывести ID и полную стоимость 5 самых дорогих поездок.
<details><summary>Решение</summary><code>SELECT trip_id, total_amount FROM trips ORDER BY total_amount DESC LIMIT 5;</code></details>

2. **Схема:** `hits_UserID_URL`. **Поля:** `UserID`, `URL`. **Задача:** Вывести первые 10 уникальных URL, посещенных пользователями.
<details><summary>Решение</summary><code>SELECT DISTINCT URL FROM hits_UserID_URL LIMIT 10;</code></details>

3. **Схема:** `trips`. **Поля:** `trip_id`, `trip_distance`. **Задача:** Вывести 3 поездки с самой короткой дистанцией (больше 0), отсортировав по ID.
<details><summary>Решение</summary><code>SELECT trip_id, trip_distance FROM trips WHERE trip_distance > 0 ORDER BY trip_distance ASC, trip_id ASC LIMIT 3;</code></details>

4. **Схема:** `hits_UserID_URL`. **Поля:** `EventTime`. **Задача:** Вывести время самого раннего (первого) события в таблице.
<details><summary>Решение</summary><code>SELECT min(EventTime) FROM hits_UserID_URL;</code></details>

5. **Схема:** `trips`. **Поля:** `vendor_id`. **Задача:** Вывести список всех доступных идентификаторов поставщиков без повторений.
<details><summary>Решение</summary><code>SELECT DISTINCT vendor_id FROM trips;</code></details>

6. **Схема:** `trips`. **Поля:** `trip_id`, `fare_amount`. **Задача:** Вывести 5 поездок, где стоимость тарифа (`fare_amount`) находится в диапазоне от 10 до 20, отсортировав по стоимости.
<details><summary>Решение</summary><code>SELECT trip_id, fare_amount FROM trips WHERE fare_amount BETWEEN 10 AND 20 ORDER BY fare_amount LIMIT 5;</code></details>

7. **Схема:** `hits_UserID_URL`. **Поля:** `UserID`, `URL`. **Задача:** Вывести 10 записей для пользователя `UserID` = 2459550954, отсортировав по времени (по убыванию).
<details><summary>Решение</summary><code>SELECT UserID, URL FROM hits_UserID_URL WHERE UserID = 2459550954 ORDER BY EventTime DESC LIMIT 10;</code></details>

8. **Схема:** `trips`. **Поля:** `trip_id`, `passenger_count`. **Задача:** Вывести поездки, где было ровно 3 или 6 пассажиров, ограничить 5 записями.
<details><summary>Решение</summary><code>SELECT trip_id, passenger_count FROM trips WHERE passenger_count IN (3, 6) LIMIT 5;</code></details>

9. **Схема:** `trips`. **Поля:** `count()`. **Задача:** Посчитать общее количество записей в таблице `trips`.
<details><summary>Решение</summary><code>SELECT count() FROM trips;</code></details>

10. **Схема:** `hits_UserID_URL`. **Поля:** `URL`. **Задача:** Вывести 5 URL, которые содержат подстроку 'google'.
<details><summary>Решение</summary><code>SELECT URL FROM hits_UserID_URL WHERE URL LIKE '%google%' LIMIT 5;</code></details>

---

### Блок 2: Фильтрация и условия (WHERE)

11. **Схема:** `trips`. **Поля:** `trip_id`, `payment_type`. **Задача:** Вывести все поездки, оплаченные наличными (`payment_type` = 'CSH').
<details><summary>Решение</summary><code>SELECT trip_id, payment_type FROM trips WHERE payment_type = 'CSH';</code></details>

12. **Схема:** `trips`. **Поля:** `trip_id`, `tip_amount`. **Задача:** Найти поездки, где сумма чаевых превышает 50, отсортировать по сумме.
<details><summary>Решение</summary><code>SELECT trip_id, tip_amount FROM trips WHERE tip_amount > 50 ORDER BY tip_amount DESC;</code></details>

13. **Схема:** `hits_UserID_URL`. **Поля:** `UserID`, `IsRobot`. **Задача:** Вывести уникальные ID пользователей, которые были помечены как боты (`IsRobot` = 1).
<details><summary>Решение</summary><code>SELECT DISTINCT UserID FROM hits_UserID_URL WHERE IsRobot = 1;</code></details>

14. **Схема:** `trips`. **Поля:** `trip_id`, `cab_type`. **Задача:** Найти все поездки типа 'green' с дистанцией более 10 миль.
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE cab_type = 'green' AND trip_distance > 10;</code></details>

15. **Схема:** `hits_UserID_URL`. **Поля:** `URL`. **Задача:** Найти записи, где URL заканчивается на '.html'.
<details><summary>Решение</summary><code>SELECT URL FROM hits_UserID_URL WHERE URL LIKE '%.html' LIMIT 5;</code></details>

16. **Схема:** `trips`. **Поля:** `trip_id`, `extra`. **Задача:** Вывести поездки, где дополнительные сборы (`extra`) отрицательные или равны нулю.
<details><summary>Решение</summary><code>SELECT trip_id, extra FROM trips WHERE extra <= 0 LIMIT 5;</code></details>

17. **Схема:** `trips`. **Поля:** `pickup_date`, `total_amount`. **Задача:** Выбрать поездки за '2016-01-01' стоимостью более 100.
<details><summary>Решение</summary><code>SELECT * FROM trips WHERE pickup_date = '2016-01-01' AND total_amount > 100;</code></details>

18. **Схема:** `hits_UserID_URL`. **Поля:** `UserID`. **Задача:** Найти пользователей с ID в диапазоне от 1000000 до 2000000, ограничить 5 результатами.
<details><summary>Решение</summary><code>SELECT DISTINCT UserID FROM hits_UserID_URL WHERE UserID BETWEEN 1000000 AND 2000000 LIMIT 5;</code></details>

19. **Схема:** `trips`. **Поля:** `trip_id`, `tolls_amount`. **Задача:** Вывести поездки, в которых оплата за платные дороги (`tolls_amount`) не равна 0.
<details><summary>Решение</summary><code>SELECT trip_id, tolls_amount FROM trips WHERE tolls_amount != 0 LIMIT 5;</code></details>

20. **Схема:** `trips`. **Поля:** `trip_id`, `passenger_count`. **Задача:** Найти поездки без пассажиров (`passenger_count` = 0).
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE passenger_count = 0 LIMIT 5;</code></details>

---

### Блок 3: Агрегация (GROUP BY, SUM, AVG)

21. **Схема:** `trips`. **Поля:** `vendor_id`. **Задача:** Посчитать количество поездок для каждого поставщика (`vendor_id`).
<details><summary>Решение</summary><code>SELECT vendor_id, count() FROM trips GROUP BY vendor_id;</code></details>

22. **Схема:** `trips`. **Поля:** `cab_type`. **Задача:** Рассчитать среднюю дистанцию поездки для каждого типа такси.
<details><summary>Решение</summary><code>SELECT cab_type, avg(trip_distance) FROM trips GROUP BY cab_type;</code></details>

23. **Схема:** `hits_UserID_URL`. **Поля:** `UserID`. **Задача:** Найти 5 пользователей с самым большим количеством кликов.
<details><summary>Решение</summary><code>SELECT UserID, count() as cnt FROM hits_UserID_URL GROUP BY UserID ORDER BY cnt DESC LIMIT 5;</code></details>

24. **Схема:** `trips`. **Поля:** `pickup_date`. **Задача:** Найти суммарную выручку (`total_amount`) по каждой дате.
<details><summary>Решение</summary><code>SELECT pickup_date, sum(total_amount) FROM trips GROUP BY pickup_date ORDER BY pickup_date;</code></details>

25. **Схема:** `trips`. **Поля:** `payment_type`. **Задача:** Найти максимальную сумму чаевых для каждого способа оплаты.
<details><summary>Решение</summary><code>SELECT payment_type, max(tip_amount) FROM trips GROUP BY payment_type;</code></details>

26. **Схема:** `trips`. **Поля:** `passenger_count`. **Задача:** Посчитать среднюю стоимость поездки для разного количества пассажиров.
<details><summary>Решение</summary><code>SELECT passenger_count, avg(total_amount) FROM trips GROUP BY passenger_count;</code></details>

27. **Схема:** `hits_UserID_URL`. **Поля:** `IsRobot`. **Задача:** Посчитать количество событий отдельно для роботов и людей.
<details><summary>Решение</summary><code>SELECT IsRobot, count() FROM hits_UserID_URL GROUP BY IsRobot;</code></details>

28. **Схема:** `trips`. **Поля:** `vendor_id`. **Задача:** Вывести тех поставщиков, у которых средняя стоимость поездки выше 15 (используйте HAVING).
<details><summary>Решение</summary><code>SELECT vendor_id, avg(total_amount) as m FROM trips GROUP BY vendor_id HAVING m > 15;</code></details>

29. **Схема:** `trips`. **Поля:** `cab_type`. **Задача:** Найти общее количество перевезенных пассажиров для каждого типа такси.
<details><summary>Решение</summary><code>SELECT cab_type, sum(passenger_count) FROM trips GROUP BY cab_type;</code></details>

30. **Схема:** `hits_UserID_URL`. **Поля:** `URL`. **Задача:** Найти ТОП-3 самых посещаемых URL.
<details><summary>Решение</summary><code>SELECT URL, count() as cnt FROM hits_UserID_URL GROUP BY URL ORDER BY cnt DESC LIMIT 3;</code></details>

---

### Блок 4: Функции дат и строк

31. **Схема:** `trips`. **Поля:** `pickup_datetime`. **Задача:** Извлечь только час начала поездки из поля `pickup_datetime`.
<details><summary>Решение</summary><code>SELECT toHour(pickup_datetime) FROM trips LIMIT 5;</code></details>

32. **Схема:** `trips`. **Поля:** `pickup_date`. **Задача:** Посчитать количество поездок, совершенных в понедельник (код 1).
<details><summary>Решение</summary><code>SELECT count() FROM trips WHERE toDayOfWeek(pickup_date) = 1;</code></details>

33. **Схема:** `hits_UserID_URL`. **Поля:** `EventTime`. **Задача:** Вывести год совершения события для первых 5 записей.
<details><summary>Решение</summary><code>SELECT toYear(EventTime) FROM hits_UserID_URL LIMIT 5;</code></details>

34. **Схема:** `trips`. **Поля:** `pickup_datetime`, `dropoff_datetime`. **Задача:** Рассчитать длительность поездки в секундах для первых 5 строк.
<details><summary>Решение</summary><code>SELECT dateDiff('second', pickup_datetime, dropoff_datetime) FROM trips LIMIT 5;</code></details>

35. **Схема:** `hits_UserID_URL`. **Поля:** `URL`. **Задача:** Привести все URL к нижнему регистру и вывести первые 5.
<details><summary>Решение</summary><code>SELECT lower(URL) FROM hits_UserID_URL LIMIT 5;</code></details>

36. **Схема:** `trips`. **Поля:** `pickup_date`. **Задача:** Показать количество поездок по месяцам.
<details><summary>Решение</summary><code>SELECT toMonth(pickup_date) as month, count() FROM trips GROUP BY month;</code></details>

37. **Схема:** `trips`. **Поля:** `total_amount`. **Задача:** Округлить стоимость каждой поездки до целого числа.
<details><summary>Решение</summary><code>SELECT round(total_amount) FROM trips LIMIT 5;</code></details>

38. **Схема:** `hits_UserID_URL`. **Поля:** `URL`. **Задача:** Посчитать длину строки каждого URL и вывести 5 записей.
<details><summary>Решение</summary><code>SELECT length(URL) FROM hits_UserID_URL LIMIT 5;</code></details>

39. **Схема:** `trips`. **Поля:** `pickup_datetime`. **Задача:** Найти все поездки, совершенные после 23:00.
<details><summary>Решение</summary><code>SELECT * FROM trips WHERE toHour(pickup_datetime) >= 23 LIMIT 5;</code></details>

40. **Схема:** `trips`. **Поля:** `pickup_date`. **Задача:** Вывести дату в формате 'DD.MM.YYYY' (используйте formatDate).
<details><summary>Решение</summary><code>SELECT formatDate(pickup_date, '%d.%m.%Y') FROM trips LIMIT 5;</code></details>

---

### Блок 5: Кейсы, JOIN и словари

41. **Схема:** `trips`. **Поля:** `total_amount`. **Задача:** Если стоимость > 50, вывести 'high', иначе 'low' (используйте CASE).
<details><summary>Решение</summary><code>SELECT total_amount, CASE WHEN total_amount > 50 THEN 'high' ELSE 'low' END FROM trips LIMIT 5;</code></details>

42. **Схема:** `trips`, `taxi_zone_dictionary`. **Задача:** Получить название боро (`Borough`) для каждой поездки, используя `JOIN` по полю `pickup_nyct2010_gid`.
<details><summary>Решение</summary><code>SELECT t.trip_id, d.Borough FROM trips t JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID LIMIT 5;</code></details>

43. **Схема:** `trips`. **Поля:** `pickup_nyct2010_gid`. **Задача:** Получить название зоны (`Zone`) через функцию `dictGet` из словаря `taxi_zone_dictionary`.
<details><summary>Решение</summary><code>SELECT dictGet('taxi_zone_dictionary', 'Zone', toUInt64(pickup_nyct2010_gid)) FROM trips LIMIT 5;</code></details>

44. **Схема:** `trips`, `taxi_zone_dictionary`. **Задача:** Посчитать количество поездок для каждого района (`Borough`) через JOIN.
<details><summary>Решение</summary><code>SELECT d.Borough, count() FROM trips t JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID GROUP BY d.Borough;</code></details>

45. **Схема:** `hits_UserID_URL`. **Поля:** `IsRobot`. **Задача:** Заменить 1 на 'Robot', 0 на 'Human' в итоговой выборке.
<details><summary>Решение</summary><code>SELECT CASE WHEN IsRobot = 1 THEN 'Robot' ELSE 'Human' END as type FROM hits_UserID_URL LIMIT 5;</code></details>

46. **Схема:** `trips`. **Поля:** `trip_id`, `tip_amount`, `fare_amount`. **Задача:** Вывести процент чаевых от основной стоимости тарифа.
<details><summary>Решение</summary><code>SELECT trip_id, (tip_amount / fare_amount) * 100 FROM trips WHERE fare_amount > 0 LIMIT 5;</code></details>

47. **Схема:** `trips`. **Поля:** `dictHas`. **Задача:** Проверить, существует ли в словаре `taxi_zone_dictionary` ключ с ID 132.
<details><summary>Решение</summary><code>SELECT dictHas('taxi_zone_dictionary', 132);</code></details>

48. **Схема:** `trips`, `taxi_zone_dictionary`. **Задача:** Найти среднюю стоимость поездки для района 'Manhattan' (через JOIN).
<details><summary>Решение</summary><code>SELECT avg(t.total_amount) FROM trips t JOIN taxi_zone_dictionary d ON toUInt64(t.pickup_nyct2010_gid) = d.LocationID WHERE d.Borough = 'Manhattan';</code></details>

49. **Схема:** `hits_UserID_URL`. **Поля:** `EventTime`. **Задача:** Найти количество событий, совершенных в утренние часы (с 6 до 11 утра).
<details><summary>Решение</summary><code>SELECT count() FROM hits_UserID_URL WHERE toHour(EventTime) BETWEEN 6 AND 11;</code></details>

50. **Схема:** `trips`. **Поля:** `trip_id`. **Задача:** Найти поездки, где дистанция больше 0, но стоимость равна 0.
<details><summary>Решение</summary><code>SELECT trip_id FROM trips WHERE trip_distance > 0 AND total_amount = 0;</code></details>

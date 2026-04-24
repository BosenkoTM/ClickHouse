# Лекция 3. DDL, DML, DQL и продвинутые инструменты (ARRAY JOIN, LIMIT BY)

## Постановка задачи
В нашем университете работает система онлайн-тестирования. Мы собираем логи попыток сдачи тестов студентами разных факультетов. Каждый тест может относиться к нескольким предметным областям (тегам). Нам необходимо:
1. Создать хранилище для этих попыток тестирования (DDL).
2. Заполнить его сырыми данными (DML).
3. С помощью запросов (DQL) определить среднюю успеваемость факультетов.
4. Выявить самые сложные темы, развернув теги (`ARRAY JOIN`).
5. Найти **топ-2 лучших попытки** для каждого курса (`LIMIT BY`).

## 1. DDL. Создание структуры базы и таблицы

Создадим базу данных (если её нет) и таблицу. В качестве движка используем `MergeTree`.

```sql
-- Создаем базу данных, в облачной среде Cloud не создавать, так как уже создана.
CREATE DATABASE IF NOT EXISTS edu_analytics;

USE edu_analytics;

-- Удаляем таблицу, если она осталась от прошлых запусков (чтобы начать с чистого листа)
DROP TABLE IF EXISTS test_attempts;

-- Создаем таблицу для логов тестирования
CREATE TABLE test_attempts
(
    attempt_time DateTime,
    student_id UInt32,
    faculty LowCardinality(String),
    course_name String,
    attempt_tags Array(String), -- Массив тегов (тем), которые покрывает тест
    total_score UInt8           -- Балл от 0 до 100
)
ENGINE = MergeTree()
ORDER BY (faculty, course_name, attempt_time);
```

## 2. DML. Наполнение данными (50+ записей)

Вставим данные батчем (пачкой), имитируя активность студентов за несколько дней.

```sql
INSERT INTO test_attempts (attempt_time, student_id, faculty, course_name, attempt_tags, total_score) VALUES
-- IT факультет: курс Python
('2024-11-01 10:00:00', 101, 'IT', 'Python Basics', ['programming', 'syntax'], 85),
('2024-11-01 10:15:00', 102, 'IT', 'Python Basics', ['programming', 'syntax', 'loops'], 92),
('2024-11-01 10:30:00', 103, 'IT', 'Python Basics', ['programming', 'loops'], 78),
('2024-11-02 11:00:00', 104, 'IT', 'Python Basics', ['syntax'], 60),
('2024-11-02 11:30:00', 105, 'IT', 'Python Basics', ['programming', 'functions'], 95),
('2024-11-03 12:00:00', 106, 'IT', 'Python Basics', ['functions', 'loops'], 40),
('2024-11-03 12:15:00', 107, 'IT', 'Python Basics', ['syntax'], 100),
('2024-11-03 12:30:00', 108, 'IT', 'Python Basics', ['programming', 'syntax'], 88),
('2024-11-04 10:00:00', 109, 'IT', 'Python Basics', ['functions'], 72),
('2024-11-04 10:10:00', 110, 'IT', 'Python Basics', ['programming'], 81),

-- IT факультет: курс Базы данных
('2024-11-01 14:00:00', 101, 'IT', 'Databases', ['sql', 'ddl'], 90),
('2024-11-01 14:20:00', 102, 'IT', 'Databases', ['sql', 'dml'], 85),
('2024-11-02 15:00:00', 103, 'IT', 'Databases', ['sql', 'joins'], 65),
('2024-11-02 15:30:00', 104, 'IT', 'Databases', ['nosql'], 80),
('2024-11-03 16:00:00', 105, 'IT', 'Databases', ['sql', 'ddl', 'dml'], 100),
('2024-11-03 16:15:00', 106, 'IT', 'Databases', ['joins'], 50),
('2024-11-04 14:00:00', 107, 'IT', 'Databases', ['sql'], 95),
('2024-11-04 14:20:00', 108, 'IT', 'Databases', ['nosql', 'json'], 88),
('2024-11-04 14:40:00', 109, 'IT', 'Databases', ['sql', 'joins'], 70),
('2024-11-04 15:00:00', 110, 'IT', 'Databases', ['ddl'], 75),

-- Экономический факультет: курс Макроэкономика
('2024-11-01 09:00:00', 201, 'Economics', 'Macroeconomics', ['theory', 'graphs'], 70),
('2024-11-01 09:15:00', 202, 'Economics', 'Macroeconomics', ['theory'], 65),
('2024-11-02 09:30:00', 203, 'Economics', 'Macroeconomics', ['math', 'graphs'], 85),
('2024-11-02 10:00:00', 204, 'Economics', 'Macroeconomics', ['theory', 'math'], 90),
('2024-11-03 11:00:00', 205, 'Economics', 'Macroeconomics', ['graphs'], 55),
('2024-11-03 11:15:00', 206, 'Economics', 'Macroeconomics', ['theory', 'math', 'graphs'], 98),
('2024-11-03 11:30:00', 207, 'Economics', 'Macroeconomics', ['math'], 77),
('2024-11-04 09:00:00', 208, 'Economics', 'Macroeconomics', ['theory'], 82),
('2024-11-04 09:30:00', 209, 'Economics', 'Macroeconomics', ['graphs'], 60),
('2024-11-04 10:00:00', 210, 'Economics', 'Macroeconomics', ['theory', 'graphs'], 88),

-- Экономический факультет: Финансы
('2024-11-01 13:00:00', 201, 'Economics', 'Finance', ['math', 'accounting'], 92),
('2024-11-01 13:30:00', 202, 'Economics', 'Finance', ['accounting'], 80),
('2024-11-02 14:00:00', 203, 'Economics', 'Finance', ['math'], 75),
('2024-11-02 14:30:00', 204, 'Economics', 'Finance', ['accounting', 'taxes'], 85),
('2024-11-03 15:00:00', 205, 'Economics', 'Finance', ['math', 'taxes'], 90),
('2024-11-03 15:30:00', 206, 'Economics', 'Finance', ['accounting'], 65),
('2024-11-04 13:00:00', 207, 'Economics', 'Finance', ['math', 'accounting', 'taxes'], 98),
('2024-11-04 13:30:00', 208, 'Economics', 'Finance', ['taxes'], 70),
('2024-11-04 14:00:00', 209, 'Economics', 'Finance', ['math'], 88),
('2024-11-04 14:30:00', 210, 'Economics', 'Finance', ['accounting', 'taxes'], 95),

-- Гуманитарный факультет: курс История
('2024-11-01 11:00:00', 301, 'Humanities', 'History', ['dates', '20th_century'], 85),
('2024-11-01 11:20:00', 302, 'Humanities', 'History', ['dates'], 60),
('2024-11-02 12:00:00', 303, 'Humanities', 'History', ['20th_century', 'essay'], 95),
('2024-11-02 12:40:00', 304, 'Humanities', 'History', ['essay'], 100),
('2024-11-03 13:00:00', 305, 'Humanities', 'History', ['dates', 'essay'], 88),
('2024-11-03 13:30:00', 306, 'Humanities', 'History', ['20th_century'], 75),
('2024-11-04 11:00:00', 307, 'Humanities', 'History', ['essay'], 92),
('2024-11-04 11:30:00', 308, 'Humanities', 'History', ['dates', '20th_century'], 70),
('2024-11-04 12:00:00', 309, 'Humanities', 'History', ['dates'], 55),
('2024-11-04 12:30:00', 310, 'Humanities', 'History', ['essay', '20th_century'], 89);
```

---

## 3. Практика DQL. Запросы к данным

### Запрос 1. Базовая аналитика (`GROUP BY`, `ORDER BY`)
*Какова средняя оценка и количество сдач тестов по каждому факультету?*

```sql
SELECT 
    faculty,
    count() AS total_attempts,
    round(avg(total_score), 2) AS avg_score
FROM test_attempts
GROUP BY faculty
ORDER BY avg_score DESC;
```
*Результат покажет, какой факультет лидирует по среднему баллу.*

---

### Запрос 2. Поиск проблемных тем с помощью `ARRAY JOIN`
В колонке `attempt_tags` лежат массивы тем, которые затронул тест. Нам нужно понять, **какая конкретная тема (тег) дается студентам тяжелее всего** (имеет самый низкий средний балл). Для этого мы развернем массивы в строки.

```sql
SELECT 
    tag,
    count() AS times_tested,
    round(avg(total_score), 2) AS average_tag_score
FROM test_attempts
ARRAY JOIN attempt_tags AS tag
GROUP BY tag
ORDER BY average_tag_score ASC -- сортируем по возрастанию (сначала самые сложные темы)
LIMIT 5;
```
*Без `ARRAY JOIN` нам пришлось бы создавать сложную реляционную структуру из трех таблиц со связями.*

---

### Запрос 3. Рейтинг лучших с помощью `LIMIT BY`
Ректорат просит вывесить на доску почета **двух студентов с максимальными баллами по КАЖДОМУ курсу**.
Если использовать просто `LIMIT`, мы получим лучших по всему университету. Используем `LIMIT BY course_name`.

```sql
SELECT 
    course_name,
    faculty,
    student_id,
    total_score
FROM test_attempts
ORDER BY course_name ASC, total_score DESC -- Обязательно сортируем по убыванию балла!
LIMIT 2 BY course_name;
```
*ClickHouse отсортирует данные, затем разобьет их на блоки по `course_name` и возьмет ровно по 2 верхние строчки из каждого блока.*

---

### Запрос 4. Мутация данных (`ALTER DELETE`)
Допустим, преподаватель истории обнаружил, что тест 2-го ноября был выдан с ошибками, и все результаты за эту дату нужно аннулировать. В ClickHouse мы используем DML мутацию.

```sql
-- Проверяем, сколько таких строк
SELECT count() FROM test_attempts 
WHERE course_name = 'History' AND toDate(attempt_time) = '2024-11-02';

-- Выполняем мутацию (удаление)
ALTER TABLE test_attempts
DELETE WHERE course_name = 'History' AND toDate(attempt_time) = '2024-11-02';
```
*Напоминание: Мутации переписывают блоки данных на диске. Не используйте их для частых точечных удалений, только для массовых корректировок или очистки.*
```

# Лекция 2. Простые, Сложные типы и LowCardinality на практике

## Постановка задачи

Университет внедрил новую платформу электронного обучения (LMS). Мы получаем непрерывный поток данных: логи действий студентов (просмотры видео, сдача тестов, клики на материалы). 

**Цель аналитики:**

Отслеживать вовлеченность студентов, анализировать их активность по факультетам, платформам (с чего сидят) и темам (тегам), а также собирать статистику по успешности ответов на конкретные вопросы тестов. 

**Архитектурное решение:**

Чтобы обеспечить моментальный отклик дашбордов для деканатов, мы **отказываемся от нормализации**. Мы не будем делать отдельные таблицы для факультетов, курсов и пользователей. Все события будут складываться в единую "широкую" таблицу `lms_events`. 
- Для повторяющихся строк (Факультет, ОС, Тип события) применим `LowCardinality`.
- Для списков тегов к лекциям применим тип `Array`.
- Для разбалловки по отдельным вопросам теста применим тип `Map`.

---

## 1. Создание таблицы

Предполагается, что база данных уже создана (например, `USE edu_analytics;`).

```sql
CREATE TABLE lms_events
(
    -- Базовые типы
    event_time DateTime64(3, 'Europe/Moscow'), -- время с точностью до миллисекунд
    student_id UInt32,                         -- беззнаковое целое, хватит на 4 млрд студентов
    is_successful Boolean,                     -- успешно ли завершено действие (например, пройден ли тест)
    
    -- LowCardinality: строк будут миллионы, но факультетов и типов событий мало
    faculty LowCardinality(String),
    event_type LowCardinality(String),         -- 'video_play', 'quiz_submit', 'material_download'
    platform LowCardinality(String),           -- 'Desktop', 'Mobile_App', 'Mobile_Web'
    
    -- Сложные типы
    tags Array(String),                        -- теги материала: ['math', 'calculus', 'mandatory']
    test_scores Map(String, UInt8)             -- мапа для хранения баллов по заданиям: {'task_1': 10, 'task_2': 5}
)
ENGINE = MergeTree()
ORDER BY (faculty, event_type, event_time);
```

> **Пояснение к ключу сортировки (ORDER BY).** Мы сортируем данные сначала по факультету и типу события, так как деканаты чаще всего будут фильтровать аналитику именно в этих разрезах.

---

## 2. Наполнение таблицы (Сквозной пример)

Сымитируем работу системы за один день.

```sql
INSERT INTO lms_events (event_time, student_id, is_successful, faculty, event_type, platform, tags, test_scores)
VALUES 
-- Студенты IT-факультета сдают тесты и смотрят видео
('2024-10-25 10:15:00.123', 1001, true, 'IT_Faculty', 'quiz_submit', 'Desktop', ['python', 'backend'], map('task_1', 10, 'task_2', 10, 'task_3', 8)),
('2024-10-25 10:20:45.000', 1002, false, 'IT_Faculty', 'quiz_submit', 'Mobile_App', ['python', 'backend'], map('task_1', 10, 'task_2', 0, 'task_3', 0)),
('2024-10-25 11:05:10.555', 1001, true, 'IT_Faculty', 'video_play', 'Desktop',['python', 'async'], map()),

-- Студенты экономического факультета
('2024-10-25 09:30:00.000', 2050, true, 'Economics', 'material_download', 'Desktop', ['macroeconomics', 'graphs'], map()),
('2024-10-25 14:15:33.777', 2051, true, 'Economics', 'quiz_submit', 'Mobile_Web', ['microeconomics'], map('q_math', 5, 'q_theory', 4)),

-- Студент гуманитарного факультета
('2024-10-25 16:40:22.111', 3100, true, 'Humanities', 'video_play', 'Mobile_App', ['history', '20_century'], map()),
('2024-10-25 17:00:00.000', 3100, true, 'Humanities', 'quiz_submit', 'Mobile_App', ['history'], map('essay', 95));
```

---

## 3. Аналитические запросы (Примеры использования типов)

### Запрос 1. Быстрая агрегация по LowCardinality
Подсчитаем, с каких платформ студенты разных факультетов чаще всего сдают тесты. 
*Благодаря `LowCardinality` этот запрос на миллионах строк отработает за миллисекунды, так как ClickHouse будет группировать числовые индексы под капотом.*

```sql
SELECT 
    faculty,
    platform,
    count() AS quiz_attempts,
    round(countIf(is_successful) / count() * 100, 2) AS success_rate_percent
FROM lms_events
WHERE event_type = 'quiz_submit'
GROUP BY faculty, platform
ORDER BY quiz_attempts DESC;
```

### Запрос 2. Работа со сложным типом Array
Найдем, сколько раз студенты взаимодействовали с материалами, у которых есть тег `python`. 
*Функция `has()` проверяет наличие элемента в массиве, избавляя нас от необходимости делать тяжелые `JOIN` с таблицами-справочниками тегов.*

```sql
SELECT 
    student_id,
    count() AS python_interactions
FROM lms_events
WHERE has(tags, 'python')
GROUP BY student_id
ORDER BY python_interactions DESC;
```

### Запрос 3. Работа со сложным типом Map (Словари)
Предположим, преподавателю нужно узнать средний балл студентов конкретно за `task_1` в тестах. Мы обращаемся к ключу внутри `Map` как к обычному массиву: `test_scores['task_1']`.

```sql
SELECT 
    student_id,
    test_scores['task_1'] AS score_task_1
FROM lms_events
WHERE event_type = 'quiz_submit' 
  AND mapContains(test_scores, 'task_1') -- фильтруем только те тесты, где было это задание
ORDER BY score_task_1 DESC;
```

### Вывод
В этом сквозном примере мы полностью отказались от реляционной модели (Студенты -> Факультеты -> События -> Теги -> Оценки), сведя всё в плоскую, широкую таблицу `lms_events`. Правильный подбор типов (`LowCardinality`, `Array`, `Map`, `UInt32`) обеспечил компактность хранения и колоссальную скорость чтения для нужд аналитики образовательного процесса.
```

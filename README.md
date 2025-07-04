# Скрипт для обработки CSV-файлов

[![CI](https://github.com/Salvatore112/csv_processor/actions/workflows/ci.yml/badge.svg)](https://github.com/Salvatore112/short_link/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Скрипт для обработки CSV-файла, который поддерживает следующие операции:

### Фильтрация
Поддерживаются операторы сравнения:
- «больше» (`>`)
- «меньше» (`<`)
- «равно» (`=`)

### Агрегация
Доступные операции агрегации:
- Среднее значение (`avg`)
- Минимальное значение (`min`)
- Максимальное значение (`max`)
- Сумма значений (`sum`)

### Сортировка
Поддерживаемые направления сортировки:
- По возрастанию (`asc`)
- По убыванию (`desc`)

# Требования

Выполнены все функциональные и нефункциональные требования, код покрыт на 94% тестами. 

Были реализованы дополнительные требования:

1. Добавлена сортировка (`--order-by`)
2. Добавлены аннотации
3. Функциональность, связанная с агрегацией, вынесена в отдельный метод `aggregate`, где для добавления нового вида агрегации достаточно просто добавить ещё одну ветку `elif`. В качестве примера было добавлено суммирование (`sum`).

# Примеры

В качестве тестового файла был использован предложенный `products.csv`

### Простое чтение файла

```bash
python csv_processor.py --file ./products.csv 
```

```
+---------------------------------------------+
| name             | brand   | price | rating |
+------------------+---------+-------+--------+
| iphone 15 pro    | apple   | 999   | 4.9    |
| galaxy s23 ultra | samsung | 1199  | 4.8    |
| redmi note 12    | xiaomi  | 199   | 4.6    |
| iphone 14        | apple   | 799   | 4.7    |
| galaxy a54       | samsung | 349   | 4.2    |
| poco x5 pro      | xiaomi  | 299   | 4.4    |
| iphone se        | apple   | 429   | 4.1    |
| galaxy z flip 5  | samsung | 999   | 4.6    |
| redmi 10c        | xiaomi  | 149   | 4.1    |
| iphone 13 mini   | apple   | 599   | 4.5    |
+---------------------------------------------+
```

### Чтение и фильтрация

```bash
python csv_processor.py --file ./products.csv --where "brand=apple"
```

```
+-----------------------------------------+
| name           | brand | price | rating |
+----------------+-------+-------+--------+
| iphone 15 pro  | apple | 999   | 4.9    |
| iphone 14      | apple | 799   | 4.7    |
| iphone se      | apple | 429   | 4.1    |
| iphone 13 mini | apple | 599   | 4.5    |
+-----------------------------------------+

```

### Чтение, фильтрация и сортировка

```bash
python csv_processor.py --file ./products.csv --where "brand=apple" --order-by "price=asc"
```

```
+-----------------------------------------+
| name           | brand | price | rating |
+----------------+-------+-------+--------+
| iphone se      | apple | 429   | 4.1    |
| iphone 13 mini | apple | 599   | 4.5    |
| iphone 14      | apple | 799   | 4.7    |
| iphone 15 pro  | apple | 999   | 4.9    |
+-----------------------------------------+

```


### Чтение, фильтрация и агрегация

```bash
python csv_processor.py --file ./products.csv --where "brand=apple" --aggregate "price=min"
```

```
+-------+
| min   |
+-------+
| 429.0 |
+-------+

```

import pytest
import csv

from csv_processor import CSVProcessor
from unittest.mock import patch


@pytest.fixture
def sample_csv(tmp_path):
    filename = tmp_path / "test.csv"
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["name", "brand", "price", "rating"])
        writer.writerow(["iphone", "apple", "999", "4.9"])
        writer.writerow(["galaxy", "samsung", "1199", "4.8"])
        writer.writerow(["redmi", "xiaomi", "199", "4.6"])
        writer.writerow(["poco", "xiaomi", "299", "4.4"])
    return str(filename)


@pytest.fixture
def whitespace_csv(tmp_path):
    filename = tmp_path / "whitespace.csv"
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["name", "brand"])
        writer.writerow([" iphone ", " apple "])
    return str(filename)


@pytest.fixture
def empty_values_csv(tmp_path):
    filename = tmp_path / "empty_values.csv"
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["name", "price"])
        writer.writerow(["", "100"])
        writer.writerow(["test", ""])
    return str(filename)


@pytest.fixture
def empty_csv(tmp_path):
    filename = tmp_path / "empty.csv"
    with open(filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["name", "brand"])  # header only
    return str(filename)


def test_read_csv(sample_csv):
    processor = CSVProcessor(sample_csv)
    assert len(processor.data) == 4
    assert processor.data[0]["name"] == "iphone"


def test_filter_equals(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data("brand=apple")
    assert len(filtered) == 1
    assert filtered[0]["name"] == "iphone"


def test_filter_with_spaces(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data(" brand  =  apple ")
    assert len(filtered) == 1


def test_filter_greater_than(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data("rating>4.7")
    assert len(filtered) == 2
    assert all(float(row["rating"]) > 4.7 for row in filtered)


def test_filter_less_than(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data("price<300")
    assert len(filtered) == 2
    assert all(int(row["price"]) < 300 for row in filtered)


def test_filter_empty_result(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data("brand=nonexistent")
    assert len(filtered) == 0


def test_aggregate_avg(sample_csv):
    processor = CSVProcessor(sample_csv)
    result = processor.aggregate("price=avg")
    assert result["avg"] == pytest.approx(674.0)


def test_aggregate_min(sample_csv):
    processor = CSVProcessor(sample_csv)
    result = processor.aggregate("rating=min")
    assert result["min"] == 4.4


def test_aggregate_max(sample_csv):
    processor = CSVProcessor(sample_csv)
    result = processor.aggregate("rating=max")
    assert result["max"] == 4.9


def test_aggregate_sum(sample_csv):
    processor = CSVProcessor(sample_csv)
    result = processor.aggregate("price=sum")
    assert result["sum"] == 2696


def test_aggregate_empty_data(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data("brand=nonexistent")
    result = processor.aggregate("price=avg", filtered)
    assert result["avg"] == 0


def test_aggregate_single_row(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data("name=iphone")
    result = processor.aggregate("price=avg", filtered)
    assert result["avg"] == 999.0


def test_aggregate_precision(sample_csv):
    processor = CSVProcessor(sample_csv)
    processor.data.append(
        {"name": "test", "brand": "test", "price": "299.99", "rating": "3.0"}
    )
    result = processor.aggregate("price=avg")
    assert isinstance(result["avg"], float)


def test_filter_and_aggregate(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data("brand=xiaomi")
    result = processor.aggregate("price=avg", filtered)
    assert result["avg"] == pytest.approx(249.0)


def test_sort_data_ascending(sample_csv):
    processor = CSVProcessor(sample_csv)
    sorted_data = processor.sort_data("price=asc")
    assert sorted_data[0]["name"] == "redmi"
    assert sorted_data[-1]["name"] == "galaxy"


def test_sort_data_descending(sample_csv):
    processor = CSVProcessor(sample_csv)
    sorted_data = processor.sort_data("rating=desc")
    assert sorted_data[0]["name"] == "iphone"
    assert sorted_data[-1]["name"] == "poco"


def test_sort_empty_data(sample_csv):
    processor = CSVProcessor(sample_csv)
    empty_data = []
    sorted_data = processor.sort_data("price=asc", empty_data)
    assert len(sorted_data) == 0


def test_sort_data_invalid_order(sample_csv):
    processor = CSVProcessor(sample_csv)
    with pytest.raises(ValueError):
        processor.sort_data("rating=invalid")


def test_invalid_condition(sample_csv):
    processor = CSVProcessor(sample_csv)
    with pytest.raises(ValueError):
        processor.filter_data("invalid_condition")


def test_invalid_aggregation(sample_csv):
    processor = CSVProcessor(sample_csv)
    with pytest.raises(ValueError):
        processor.aggregate("price=invalid")


def test_filter_invalid_column(sample_csv):
    processor = CSVProcessor(sample_csv)
    with pytest.raises(KeyError):
        processor.filter_data("nonexistent_column=value")


def test_aggregate_non_numeric(sample_csv):
    processor = CSVProcessor(sample_csv)
    with pytest.raises(ValueError):
        processor.aggregate("name=avg")


def test_parse_condition_complex(sample_csv):
    processor = CSVProcessor(sample_csv)
    col, op, val = processor._parse_condition("price>=1000")
    assert col == "price"
    assert op == ">="
    assert val == "1000"


def test_parse_condition_invalid(sample_csv):
    processor = CSVProcessor(sample_csv)
    with pytest.raises(ValueError):
        processor._parse_condition("invalid condition")


def test_non_float_comparison(sample_csv):
    processor = CSVProcessor(sample_csv)
    with pytest.raises(ValueError):
        processor.filter_data("name>100")


def test_display_results_table(capsys, sample_csv):
    processor = CSVProcessor(sample_csv)
    test_data = [{"name": "test", "value": "123"}]
    processor.display_results(test_data)
    captured = capsys.readouterr()
    assert "test" in captured.out
    assert "123" in captured.out


def test_display_results_aggregate(capsys, sample_csv):
    processor = CSVProcessor(sample_csv)
    test_data = {"avg": 123.45}
    processor.display_results(test_data)
    captured = capsys.readouterr()
    assert "avg" in captured.out
    assert "123.45" in captured.out


def test_display_empty_table(capsys, sample_csv):
    processor = CSVProcessor(sample_csv)
    processor.display_results([])
    captured = capsys.readouterr()
    assert "No data matches" in captured.out


def test_display_mixed_data_types(capsys, sample_csv):
    processor = CSVProcessor(sample_csv)
    mixed_data = [
        {"name": "short", "value": "1"},
        {"name": "very long name", "value": "1000000"},
    ]
    processor.display_results(mixed_data)
    captured = capsys.readouterr()
    assert "very long name" in captured.out


def test_nonexistent_file():
    with pytest.raises(FileNotFoundError):
        CSVProcessor("nonexistent.csv")


def test_empty_csv_file(empty_csv):
    processor = CSVProcessor(empty_csv)
    assert len(processor.data) == 0


def test_missing_column_in_data(sample_csv):
    processor = CSVProcessor(sample_csv)
    processor.data.append({"name": "test", "brand": "test"})
    with pytest.raises(KeyError):
        processor.filter_data("price>100")


def test_empty_values(empty_values_csv):
    processor = CSVProcessor(empty_values_csv)
    with pytest.raises(ValueError):
        processor.aggregate("price=avg")


def test_main_with_error(capsys):
    testargs = ["prog", "--file", "test.csv", "--aggregate", "invalid=avg"]
    with patch("sys.argv", testargs):
        with patch("csv_processor.CSVProcessor") as mock_processor:
            mock_instance = mock_processor.return_value
            mock_instance.data = [{"brand": "apple", "price": "999"}]
            mock_instance.aggregate.side_effect = ValueError("Test error")

            from csv_processor import main

            main()

    captured = capsys.readouterr()
    assert "Error" in captured.out


def test_display_single_column(capsys, sample_csv):
    processor = CSVProcessor(sample_csv)
    test_data = [{"name": "test"}]
    processor.display_results(test_data)
    captured = capsys.readouterr()
    assert "test" in captured.out
    assert "| name |" in captured.out


def test_csv_with_quoted_values(tmp_path):
    filename = tmp_path / "quoted.csv"
    with open(filename, "w", newline="") as file:
        file.write('"name","brand","price"\n')
        file.write('"ipho,ne","apple","999"\n')
    processor = CSVProcessor(str(filename))
    assert processor.data[0]["name"] == "ipho,ne"


def test_permission_error(tmp_path, monkeypatch):
    filename = tmp_path / "protected.csv"
    filename.touch(mode=0o000)
    with pytest.raises(PermissionError):
        CSVProcessor(str(filename))


def test_aggregate_single_value(sample_csv):
    processor = CSVProcessor(sample_csv)
    filtered = processor.filter_data("name=iphone")
    result = processor.aggregate("price=sum", filtered)
    assert result["sum"] == 999


def test_sort_empty_data_with_param(sample_csv):
    processor = CSVProcessor(sample_csv)
    empty_data = []
    sorted_data = processor.sort_data("price=asc", empty_data)
    assert len(sorted_data) == 0


def test_display_long_values(capsys, sample_csv):
    processor = CSVProcessor(sample_csv)
    long_data = [{"name": "a" * 50, "value": "1" * 50}]
    processor.display_results(long_data)
    captured = capsys.readouterr()
    assert "a" * 50 in captured.out
    assert "1" * 50 in captured.out

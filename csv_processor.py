import argparse
import csv
from typing import List, Dict, Union, Optional


class CSVProcessor:
    def __init__(self, filename: str):
        self.filename = filename
        self.data = self._read_csv()

    def _read_csv(self) -> List[Dict[str, str]]:
        with open(self.filename, mode="r") as file:
            reader = csv.DictReader(file)
            return [row for row in reader]

    def filter_data(
        self, condition: str, data: Optional[List[Dict[str, str]]] = None
    ) -> List[Dict[str, str]]:
        if data is None:
            data = self.data

        column, operator, value = self._parse_condition(condition)

        filtered_data = []
        for row in data:
            row_value = row[column]

            if operator == "=":
                if row_value == value:
                    filtered_data.append(row)
            elif operator == ">":
                if float(row_value) > float(value):
                    filtered_data.append(row)
            elif operator == "<":
                if float(row_value) < float(value):
                    filtered_data.append(row)

        return filtered_data

    def _parse_condition(self, condition: str) -> tuple[str, str, str]:
        operators = [">=", "<=", ">", "<", "="]

        for op in operators:
            if op in condition:
                column, value = condition.split(op)
                return column.strip(), op, value.strip()

        raise ValueError(f"Invalid condition format: {condition}")

    def sort_data(
        self, order_by: str, data: Optional[List[Dict[str, str]]] = None
    ) -> List[Dict[str, str]]:
        if data is None:
            data = self.data

        column, order = order_by.split("=")
        column = column.strip()
        order = order.strip().lower()

        if order not in ["asc", "desc"]:
            raise ValueError("Supported arrangement ways are 'asc' or 'desc'")
        try:

            def key_func(x):
                return float(x[column])

        except ValueError:

            def key_func(x):
                return x[column]

        reverse = order == "desc"
        return sorted(data, key=key_func, reverse=reverse)

    def aggregate(
        self, aggregation: str, data: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Union[float, str]]:
        if data is None:
            data = self.data

        column, operation = aggregation.split("=")
        column = column.strip()
        operation = operation.strip()

        numeric_values = []
        for row in data:
            try:
                numeric_values.append(float(row[column]))
            except ValueError:
                raise ValueError(f"Cannot aggregate non-numeric column: {column}")

        if not numeric_values:
            return {operation: 0}

        if operation == "avg":
            result = sum(numeric_values) / len(numeric_values)
        elif operation == "min":
            result = min(numeric_values)
        elif operation == "max":
            result = max(numeric_values)
        elif operation == "sum":
            result = sum(numeric_values)
        else:
            raise ValueError(f"Unknown aggregation operation: {operation}")

        return {operation: round(result, 2)}

    def display_results(
        self, data: Union[List[Dict[str, str]], Dict[str, Union[float, str]]]
    ) -> None:
        if isinstance(data, list):
            if not data:
                print("No data matches the filter criteria")
                return

            headers = data[0].keys()

            col_widths = {header: len(header) for header in headers}
            for row in data:
                for header in headers:
                    col_widths[header] = max(col_widths[header], len(row[header]))

            total_width = sum(col_widths.values()) + (3 * (len(headers) - 1)) + 4

            print("+" + "-" * (total_width - 2) + "+")

            header_line = (
                "| "
                + " | ".join(header.ljust(col_widths[header]) for header in headers)
                + " |"
            )
            print(header_line)

            separator = (
                "+"
                + "+".join("-" * (col_widths[header] + 2) for header in headers)
                + "+"
            )
            print(separator)

            for row in data:
                row_line = (
                    "| "
                    + " | ".join(
                        row[header].ljust(col_widths[header]) for header in headers
                    )
                    + " |"
                )
                print(row_line)

            print("+" + "-" * (total_width - 2) + "+")

        else:
            for key, value in data.items():
                value_str = str(value)
                max_length = max(len(key), len(value_str))

                print("+" + "-" * (max_length + 2) + "+")
                print(f"| {key.ljust(max_length)} |")
                print("+" + "-" * (max_length + 2) + "+")
                print(f"| {value_str.ljust(max_length)} |")
                print("+" + "-" * (max_length + 2) + "+")


parser = argparse.ArgumentParser(description="Process CSV file")
parser.add_argument("--file", required=True, help="Path to CSV file")
parser.add_argument("--where", help='Filter condition (e.g. "rating>4.5")')
parser.add_argument("--order-by", help='Sort by column (e.g. "rating=desc")')
parser.add_argument("--aggregate", help='Aggregation (e.g. "price=avg")')

args = parser.parse_args()

try:
    processor = CSVProcessor(args.file)

    data = processor.data
    if args.where:
        data = processor.filter_data(args.where, data)

    if args.order_by:
        data = processor.sort_data(args.order_by, data)

    if args.aggregate:
        result = processor.aggregate(args.aggregate, data)
        processor.display_results(result)
    else:
        processor.display_results(data)

except Exception as e:
    print(f"Error: {e}")

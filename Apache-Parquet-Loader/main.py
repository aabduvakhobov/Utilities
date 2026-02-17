import os
import sys
import glob

import pyarrow
from pyarrow import parquet
from pyarrow import flight
from pyarrow import compute


# Helper Functions.
def table_exists(flight_client, table_name):
    tables = map(lambda flight: flight.descriptor.path, flight_client.list_flights())
    return [bytes(table_name, "UTF-8")] in tables


def create_time_series_table(flight_client, table_name, schema, error_bound):
    # Construct the CREATE TIME SERIES TABLE string with column names
    # quoted to also support special characters in column names such
    # as spaces and punctuation.
    columns = []
    for field in schema:
        if field.type == pyarrow.timestamp("us"):
            columns.append(f"`{field.name}` TIMESTAMP")
        elif field.type == pyarrow.float32():
            columns.append(f"`{field.name}` FIELD({error_bound}%)")
        elif field.type == pyarrow.string():
            columns.append(f"`{field.name}` TAG")
        else:
            raise ValueError(f"Unsupported Data Type: {field.type}")

    sql = f"CREATE TIME SERIES TABLE {table_name} ({', '.join(columns)})"

    # Execute the CREATE TIME SERIES TABLE command.
    ticket = flight.Ticket(str.encode(sql))
    result = flight_client.do_get(ticket)
    return list(result)


def read_parquet_file_or_folder(path):
    # Read Apache Parquet file or folder.
    arrow_table = parquet.read_table(path)

    # Ensure the schema only uses supported types.
    arrays = []
    fields = []

    for field in arrow_table.schema:
        column = arrow_table[field.name]

        if field.type in [pyarrow.float16(), pyarrow.float64()]:
            # Ensure fields are float32 as others are not supported.
            column = compute.cast(column, pyarrow.float32())
            fields.append(pyarrow.field(field.name, pyarrow.float32()))
        elif field.type in [
            pyarrow.timestamp("s"),
            pyarrow.timestamp("ms"),
            pyarrow.timestamp("ns"),
        ]:
            # Ensure timestamps are timestamp[us] as others are not supported.
            column = compute.cast(column, pyarrow.timestamp("us"))
            fields.append(pyarrow.field(field.name, pyarrow.timestamp("us")))
        else:
            fields.append(field)

        arrays.append(column)

    # Create a new table with the supported types.
    return pyarrow.Table.from_arrays(arrays, schema=pyarrow.schema(fields))


def do_put_arrow_table(flight_client, table_name, arrow_table):
    upload_descriptor = flight.FlightDescriptor.for_path(table_name)
    writer, _ = flight_client.do_put(upload_descriptor, arrow_table.schema)
    writer.write(arrow_table)
    writer.close()


# Main Function.
if __name__ == "__main__":
    if len(sys.argv) != 4 and len(sys.argv) != 5:
        print(
            f"usage: {sys.argv[0]} host time_series_table_name parquet_file_or_folder [relative_error_bound]"
        )
        sys.exit(1)

    flight_client = flight.FlightClient(f"grpc://{sys.argv[1]}")
    table_name = sys.argv[2]
    error_bound = sys.argv[4] if len(sys.argv) == 5 else "0.0"

    if os.path.isdir(sys.argv[3]):
        parquet_files = glob.glob(sys.argv[3] + os.sep + "*.parquet")
        parquet_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(sys.argv[3]):
        parquet_files = [sys.argv[3]]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")

    arrow_table = read_parquet_file_or_folder(parquet_files[0])
    if not table_exists(flight_client, table_name):
        create_time_series_table(
            flight_client, table_name, arrow_table.schema, error_bound
        )

    for index, parquet_file in enumerate(parquet_files):
        print(f"- Processing {parquet_file} ({index + 1} of {len(parquet_files)})")
        arrow_table = read_parquet_file_or_folder(parquet_file)
        do_put_arrow_table(flight_client, table_name, arrow_table)

    # Flush the data to disk.
    action = flight.Action("FlushMemory", b"")
    result = flight_client.do_action(action)
    print(list(result))

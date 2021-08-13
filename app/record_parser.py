from .varint_parser import parse_varint
from .models import Table, Record


def parse_column_value(stream, serial_type):
    if (serial_type >= 13) and (serial_type % 2 == 1):
        # Text encoding
        n_bytes = (serial_type - 13) // 2
        return stream.read(n_bytes)
    elif (serial_type >= 12) and (serial_type % 2 == 0):
        # BLOB encoding
        n_bytes = (serial_type - 12) // 2
        return stream.read(n_bytes)
    elif serial_type == 9:
        return 1
    elif serial_type == 4:
        # 32 bit twos-complement integer
        return int.from_bytes(stream.read(4), "big")
    elif serial_type == 2:
        # 16 bit twos-complement integer
        return int.from_bytes(stream.read(2), "big")
    elif serial_type == 1:
        # 8 bit twos-complement integer
        return int.from_bytes(stream.read(1), "big")
    elif serial_type == 0:
        return None
    else:
        raise Exception(f"Unhandled serial_type {serial_type}")


def parse_record(stream, table: Table) -> Record:
    _number_of_bytes_in_header = parse_varint(stream)

    serial_types = [parse_varint(stream) for i in range(len(table.columns))]
    column_values = [parse_column_value(stream, serial_type) for serial_type in serial_types]

    return Record(
        column_names_to_values={
            column.name: column_values[i] for i, column in enumerate(table.columns)
        }
    )

import pyarrow
import pyarrow.json as pjson
import pyarrow.parquet as pq
import json

def fun3(src_file, dst_file):
    data = []
    column_names = set()
    for line in open(src_file):
        row = json.loads(line)
        names = set(row.keys())
        column_names = column_names.union(names)
        data.append(row)

    schema = sorted(list(column_names))

    # 第二次循环，给每列填充数据
    column_data = {}
    for row in data:
        for column in schema:
            if column not in column_data:
                _col = []
                column_data[column] = _col
            else:
                _col = column_data[column]
            _col.append(row.get(column))

    array_data = []
    for column, _col in column_data.items():
        pydata = pyarrow.array(_col)
        array_data.append(pydata)

    table = pyarrow.Table.from_arrays(array_data, schema)
    print('table', table.num_rows, table.num_columns, table.schema)
    pq.write_table(table, dst_file)


def fun1(src_file, dst_file):
    table = pjson.read_json(src_file)
    print('table', table.num_rows, table.num_columns, table.schema)
    pq.write_table(table, dst_file)


if __name__ == '__main__':
    src_file = 'input.json'
    dst_file = 'result.parquet'
    fun3(src_file, dst_file)

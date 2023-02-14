################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import pyarrow
import pyarrow.parquet as pq
import pyarrow.json as pj
import json


def method2(src_file, dst_file):
    # 先获取到所有按行存储的json数据，并统计有多少列
    data = []
    column_names = set()
    for line in open(src_file):
        row = json.loads(line)
        names = set(row.keys())
        column_names = column_names.union(names)
        data.append(row)

    schema = sorted(list(column_names))

    # 第二次循环，给每列填充数据，将行数据转成列数据，要求同一列的所有数据格式必须一致
    # 有些列可能在某些行不存在，要填null
    column_data = {}
    for row in data:
        for column in schema:
            if column not in column_data:
                _col = []
                column_data[column] = _col
            else:
                _col = column_data[column]
            _col.append(row.get(column))

    # 将列数据转成 pyarrow.array 格式
    # array_data中数据顺序必须和schema一致
    array_data = []
    for column in schema:
        array_data.append(pyarrow.array(column_data[column]))

    # 将列数据转成Table
    table = pyarrow.Table.from_arrays(array_data, schema)
    print('table', table.num_rows, table.num_columns, table.schema)
    # 将table存储为parquet格式
    pq.write_table(table, dst_file)


def method1(src_file, dst_file):
    # 直接调用c++来转换
    table = pj.read_json(src_file)
    print('table', table.num_rows, table.num_columns, table.schema)
    pq.write_table(table, dst_file)


if __name__ == '__main__':
    src_file = 'input.json'
    dst_file = 'result.parquet'
    method1(src_file, dst_file)

from sqlalchemy import create_engine, MetaData, Table, select, text, tuple_, not_, inspect, insert
from sqlalchemy.ext.automap import automap_base
from tabulate import tabulate
from itertools import groupby

EXCLUDE_TABLES = [
    ('DB1', 'TAB3')
]


def get_info_tabs(engine, conn, metadata):
    data = Table('TABLES', metadata, autoload_with=engine)
    cols = [data.c.TABLE_SCHEMA,
            data.c.TABLE_NAME
            ]
    query = select(*cols)
    query = query.filter(~tuple_(data.c.TABLE_SCHEMA, data.c.TABLE_NAME).in_(EXCLUDE_TABLES))
    query = query.where(data.c.TABLE_SCHEMA.in_(['DB1', 'DB2', 'DB3']))
    query = query.order_by(data.c.TABLE_SCHEMA, data.c.TABLE_NAME)

    # print(query)
    return [{
        'TABLE_SCHEMA': row.TABLE_SCHEMA,
        'TABLE_NAME': row.TABLE_NAME
    } for row in conn.execute(query)]


def get_info_cols(engine, conn, metadata, diff_tables):
    data = Table('COLUMNS', metadata, autoload_with=engine)
    cols = [data.c.TABLE_SCHEMA,
            data.c.TABLE_NAME,
            data.c.COLUMN_NAME,
            data.c.ORDINAL_POSITION,
            data.c.COLUMN_TYPE,
            data.c.COLUMN_KEY,
            data.c.COLUMN_DEFAULT
            ]
    not_in_values = [tuple(d.values()) for d in diff_tables]
    query = select(*cols)
    query = query.filter(~tuple_(data.c.TABLE_SCHEMA, data.c.TABLE_NAME).in_(EXCLUDE_TABLES))
    query = query.filter(~tuple_(data.c.TABLE_SCHEMA, data.c.TABLE_NAME).in_(not_in_values))
    query = query.where(data.c.TABLE_SCHEMA.in_(['DB1', 'DB2', 'DB3']))
    query = query.order_by(data.c.TABLE_SCHEMA, data.c.TABLE_NAME, data.c.ORDINAL_POSITION)
    # print(query)
    return [{
        'TABLE_SCHEMA': row.TABLE_SCHEMA,
        'TABLE_NAME': row.TABLE_NAME,
        'COLUMN_NAME': row.COLUMN_NAME,
        'ORDINAL_POSITION': row.ORDINAL_POSITION,
        'COLUMN_TYPE': row.COLUMN_TYPE,
        'COLUMN_KEY': row.COLUMN_KEY,
        'COLUMN_DEFAULT': row.COLUMN_DEFAULT
    } for row in conn.execute(query)]


DbMcsConfig = {
    "host": "localhost",
    "user": "root",
    "passwd": "5402",
    "port": 3301,
}

DbWltConfig = {
    "host": "localhost",
    "user": "root",
    "passwd": "5402",
    "port": 3302,
}


def create_connect(info, table_schema):
    engine = create_engine(f"mysql+pymysql://{info['user']}:{info['passwd']}@{info['host']}:{info['port']}/{table_schema}")
    conn = engine.connect()
    meta = MetaData()
    return engine, conn, meta


def dict_diff(src: [], dst: []):
    src_values = [tuple(d.values()) for d in src]
    # print(src_values)

    dst_values = [tuple(d.values()) for d in dst]
    # print(dst_values)

    diff_values = set(src_values) - set(dst_values)
    # print(diff_values)

    keys = tuple(dst[0].keys())
    result = [dict(zip(keys, d)) for d in diff_values]

    return result


src_engine, src_conn, src_metadata = create_connect(DbWltConfig, 'information_schema')
dst_engine, dst_conn, dst_metadata = create_connect(DbMcsConfig, 'information_schema')

src_tabs = get_info_tabs(src_engine, src_conn, src_metadata)
# print(src_result)

dst_tabs = get_info_tabs(dst_engine, dst_conn, dst_metadata)
# print(dst_result)

diff_tables = dict_diff(src_tabs, dst_tabs)

print(tabulate(diff_tables, headers='keys', tablefmt='psql', showindex=True))

src_cols = get_info_cols(src_engine, src_conn, src_metadata, diff_tables)
# print(src_result)

dst_cols = get_info_cols(dst_engine, dst_conn, dst_metadata, diff_tables)
# print(dst_result)

diff_cols = dict_diff(src_cols, dst_cols)

print(tabulate(diff_cols, headers='keys', tablefmt='psql', showindex=True))

if len(diff_cols) > 0:
    print('diff exists !!!')
    exit()

print(tabulate(src_tabs, headers='keys', tablefmt='psql', showindex=True))

grouped_tabs = {}
for name, group in groupby(src_tabs, key=lambda tab: tab['TABLE_SCHEMA']):
    grouped_tabs[name] = list(group)
print(grouped_tabs)


def get_select(engine, conn, metadata, table_name):
    data = Table(table_name, src_metadata, autoload_with=src_engine)
    query = select(data)
    return conn.execute(query)


def set_insert(engine, conn, metadata, table_name, new_data):
    data = Table(table_name, src_metadata, autoload_with=src_engine)
    query = insert(data).values(**new_data).on_duplicate_key_update(
        name=insert(data).values.name
    )
    print(query)
    return conn.execute(query)


for key in grouped_tabs:
    tabs = grouped_tabs[key]
    for index, tab in enumerate(tabs):
        if index == 0:
            src_engine, src_conn, src_metadata = create_connect(DbWltConfig, tab['TABLE_SCHEMA'])
            dst_engine, dst_conn, dst_metadata = create_connect(DbMcsConfig, tab['TABLE_SCHEMA'])

        src_results = get_select(src_engine, src_conn, src_metadata, tab['TABLE_NAME'])
        dst_results = set_insert(dst_engine, dst_conn, dst_metadata, tab['TABLE_NAME'], src_results)
        print(dst_results.rowcount)

# for row in src_tabs:
#     engine, conn, metadata = DB_CONN.get(row['TABLE_SCHEMA'])
#     data = Table(row['TABLE_NAME'], metadata, autoload_with=engine)
#     query = select(data)
#     for d in conn.execute(query):
#         print(d)


# src_engine, src_conn, src_metadata = create_connect(DbMcsConfig, 'DB1')
# dst_engine, dst_conn, dst_metadata = create_connect(DbWltConfig, 'DB1')
#
# inspector = inspect(src_engine)
# tables = inspector.get_table_names()
# print(tables)
#
# for table in tables:
#     src_table = Table(table, src_metadata, autoload_with=src_engine)
#     dst_table = Table(table, dst_metadata, autoload_with=dst_engine)
#
#     select_stmt = select(src_table)
#     print('select_stmt', select_stmt)
#     data = src_conn.execute(select_stmt).fetchall()
#     print('data', data)
#
#     for row in data:
#         insert_stmt = insert(dst_table).values(**row)
#         print('insert_stmt', insert_stmt)
#         dst_conn.execute(insert_stmt)

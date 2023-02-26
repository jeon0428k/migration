from sqlalchemy import create_engine, MetaData, Table, select, text, tuple_, not_
from sqlalchemy.ext.automap import automap_base
from tabulate import tabulate

EXCLUDE_TABLES = [
    ('DB1', 'TAB3')
]


def get_tabs(engine, conn, metadata):
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


def get_cols(engine, conn, metadata, diff_tables):
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

    diff_values = set(dst_values) - set(src_values)
    # print(diff_values)

    keys = tuple(dst[0].keys())
    result = [dict(zip(keys, d)) for d in diff_values]

    return result


src_engine, src_conn, src_metadata = create_connect(DbMcsConfig, 'information_schema')
dst_engine, dst_conn, dst_metadata = create_connect(DbWltConfig, 'information_schema')

src_result = get_tabs(src_engine, src_conn, src_metadata)
# print(src_result)

dst_result = get_tabs(dst_engine, dst_conn, dst_metadata)
# print(dst_result)

diff_tables = dict_diff(src_result, dst_result)

print(tabulate(diff_tables, headers='keys', tablefmt='psql', showindex=True))

# if len(diff_tables) > 0:
#     for row in diff_tables:

src_result = get_cols(src_engine, src_conn, src_metadata, diff_tables)
# print(src_result)

dst_result = get_cols(dst_engine, dst_conn, dst_metadata, diff_tables)
# print(dst_result)

diff_cols = dict_diff(src_result, dst_result)

print(tabulate(diff_cols, headers='keys', tablefmt='psql', showindex=True))

import pymysql
from tabulate import tabulate


class DbConfig:
    host = "localhost"
    user = "root"
    password = "5402"
    port = 3301
    db = "DB1"


conn = pymysql.connect(host=DbConfig.host, port=DbConfig.port, user=DbConfig.user, passwd=DbConfig.password, charset="utf8")
with conn:
    cur = conn.cursor(pymysql.cursors.DictCursor)
    with cur:
        sql = '''
SELECT * 
FROM DB1.TAB1
        '''
        cur.execute(sql)
        result = cur.fetchall()
        print(tabulate(result, headers='keys', tablefmt='psql', showindex=True))

        if result:
            row = result[0]
            keys = ["{0}={0}".format(k) for k, v in row.items()]
            keys = ','.join(keys)
            print(keys)

            values = [tuple(data.values()) for data in result]
            values = str(values).strip('[]').replace("None", "null")
            print(values)

            sql = '''
INSERT INTO DB1.TAB1
VALUES {0}
ON DUPLICATE KEY UPDATE {1}
            '''.format(values, keys)
            print(sql)



# with conn:
#     cur = conn.cursor(pymysql.cursors.DictCursor)
#     with cur:
#         sql = open("files/info_tabs.sql").read()
#         cur.execute(sql)
#         result = cur.fetchall()
#         print(tabulate(result, headers='keys', tablefmt='psql', showindex=True))
#         print("=============================")
#
#         sql = open("files/info_cols.sql").read()
#         params = [(data['TABLE_SCHEMA'], data['TABLE_NAME']) for data in result]
#         cur.execute(sql.format(','.join(['%s'] * len(params))), params)
#         result = cur.fetchall()
#         print(tabulate(result, headers='keys', tablefmt='psql', showindex=True))

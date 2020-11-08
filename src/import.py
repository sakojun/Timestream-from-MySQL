import boto3
from botocore.client import Config
import Constant
import MySQLdb
import logging
from timestream import Timestream

logger = logging.getLogger(__name__)
log_file = logging.FileHandler('logger.log')
logger.setLevel(logging.ERROR)
logger.addHandler(log_file)


ts = Timestream()
ts.create_database()
ts.create_table(Constant.TABLE_NAME)

conn = MySQLdb.connect(
    user='root',
    passwd='mysql',
    host='127.0.0.1',
    db='db',
    port=3306
)

cur = conn.cursor()

sql = "select count(*) from table"
cur.execute(sql)
rows = cur.fetchall()
for row in rows:
    rows_count = row[0]

print(rows_count)

for i in range(0, rows_count + 100, 100):

    sql = "select * from table where id > {} limit 100".format(i)
    cur.execute(sql)
    rows = cur.fetchall()

    tmp_records = []
    for row in rows:
        print(row)

        current_time = row[6].timestamp()

        tmp_common_attributes = {
            'MeasureValueType': 'BIGINT',
            'TimeUnit': 'SECONDS'
        }

        tmp_records.append(
            {
                'Dimensions': [
                    {
                        'Name': 'uuid',
                        'Value': row[2],
                        'DimensionValueType': 'VARCHAR'
                    },
                ],
                'MeasureName': 'status',
                'MeasureValue': str(row[3]),
                'Time': str(int(current_time))
            }
        )

    ts.write_records_with_common_attributes(Constant.TABLE_NAME, tmp_common_attributes, tmp_records)

cur.close
conn.close

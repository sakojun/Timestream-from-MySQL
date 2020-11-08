import boto3
from botocore.client import Config
import Constant

# https://docs.aws.amazon.com/timestream/latest/developerguide/getting-started.python.code-samples.write-data-optimized.html

class Timestream:
    session = boto3.Session()
    client = session.client('timestream-write',
                            config=Config(read_timeout=20, max_pool_connections=5000,
                                          retries={'max_attempts': 10}))

    def create_database(self, db_name):
        print("Creating Database")
        try:
            self.client.create_database(DatabaseName=Constant.DATABASE_NAME)
            print("Database [%s] created successfully." % Constant.DATABASE_NAME)
        except self.client.exceptions.ConflictException:
            print("Database [%s] exists. Skipping database creation" % Constant.DATABASE_NAME)
        except Exception as err:
            print("Create database failed:", err)

    def create_table(self, table_name):
        print("Creating table")
        retention_properties = {
            'MemoryStoreRetentionPeriodInHours': Constant.HT_TTL_HOURS,
            'MagneticStoreRetentionPeriodInDays': Constant.CT_TTL_DAYS
        }
        try:
            self.client.create_table(DatabaseName=Constant.DATABASE_NAME, TableName=table_name,
                                     RetentionProperties=retention_properties)
            print("Table [%s] successfully created." % table_name)
        except self.client.exceptions.ConflictException:
            print("Table [%s] exists on database [%s]. Skipping table creation" % (
                table_name, Constant.DATABASE_NAME))
        except Exception as err:
            print("Create table failed:", err)

    def write_records_with_common_attributes(self, table_name, common_attributes, records):
        print("Writing records extracting common attributes")
        try:
            result = self.client.write_records(DatabaseName=Constant.DATABASE_NAME, TableName=table_name,
                                               Records=records, CommonAttributes=common_attributes)
            print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except Exception as err:
            logger.error("Error:{}".format(err))
            logger.error(common_attributes + records)

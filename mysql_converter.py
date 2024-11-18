import re


def convert_sql(mysql_query):
    pattern = r'CREATE TABLE `([^`]+)`'
    return re.sub(pattern, r'CREATE TABLE \1', mysql_query)


test_query = '''CREATE TABLE `employeelist` (
  eid INTEGER,
  firstName varchar(31) NOT NULL DEFAULT '',
  lastName varchar(31) NOT NULL DEFAULT ''
);'''

converted_query = convert_sql(test_query)
print("Original query:")
print(test_query)
print("\nConverted query:")
print(converted_query)

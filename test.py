# Python3 code to demonstrate
# Getting datetime object using a date_string

# importing datetime module
import datetime
# datestring for which datetime_obj required
date_string = '2009-01-09 02:54:25 UTC'
print("string datetime: ")
print(date_string)
print("datestring class is :", type(date_string))

# using strptime() to get datetime object
datetime_obj = datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S %Z').year

print("converted to datetime:")

# Printing datetime
print(datetime_obj)

# Checking class of datetime_obj.
print("datetime_obj class is :", type(datetime_obj))

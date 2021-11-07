import datetime
import pytz

def is_dst(dt,timeZone):
   aware_dt = timeZone.localize(dt)
   return aware_dt.dst() != datetime.timedelta(0,0)


timeZone = pytz.timezone("US/Eastern")
curr_datetime = ( datetime.datetime.now())
dt = datetime.datetime(curr_datetime.year ,curr_datetime.month,curr_datetime.day)

if is_dst(dt, timeZone):
    file_fmt = 4
else:
    file_fmt = 5

print('{:02}'.format(file_fmt))

import time
import calendar
import sys
import hashlib
import re

from service import env
from service import api

from db import sql
from db import query


def main():
    start = time.gmtime(time.time() - 86400)
    # this is the query end time (i.e. this is 2020-10-28T01:00:00Z)
    # start = time.strptime("2021-03-05 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
    startStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print('Script start at', startStr)

    newPyCompleted = time.strptime(time.strftime(
        "%Y-%m-%d 00:00:00", start), "%Y-%m-%d %H:%M:%S")
    newCompleted = time.strftime("%Y-%m-%d 00:00:00", start)

    env_var = env.Env()
    queries = query.Queries()
    apis = api.APIrequests()

    print("Connecting to Redshift...")
    # connect to Redshift
    # On QA, this will be connected to the QA reporting Redshift database
    # On PROD, this will be connected to the PROD reporting Redshift database
    redshift = sql.Redshift(
        user=env_var.redshift_user,
        password=env_var.redshift_pw,
        host=env_var.redshift_host,
        database=env_var.redshift_db,
        port=env_var.redshift_port
    )

    # This is the query start time (i.e. this is 2020-10-28T00:00:00Z)
    completed = time.strftime(
        "%Y-%m-%d 00:00:00", time.gmtime(time.time() - (86400*2)))

    print("Script last completed at", completed)

    pyCompleted = time.strptime(completed + " +0000", "%Y-%m-%d %H:%M:%S %z")

    try:
        if pyCompleted < newPyCompleted:

            print("************************************************************")

            print("Getting Fastly number...")
            print("Query start: " + completed)
            print("Query end: " + newCompleted)

            servicesList = apis.getFastlyServiceList(
                fastlyKey=env_var.fastly_key, fastlyUrl='https://api.fastly.com/service')

            fromDate = time.strftime(
                "%Y-%m-%d%%2000:00", time.gmtime(time.time() - (86400*2)))
            toDate = time.strftime(
                "%Y-%m-%d%%2000:00", time.gmtime(time.time() - (86400)))

            url = 'https://api.fastly.com/stats/field/bandwidth?region=all&from=' + fromDate + \
                '&to=' + toDate + '&by=day'

            totalBandwidth = apis.getFastlyServicesBandwidth(
                fastlyKey=env_var.fastly_key, fastlyUrl=url, fastlyServicesList=servicesList)

            print("Getting Redshift number...")
            redshift.execute(
                queries.totalBandwidth(startStr=completed,
                                       endStr=newCompleted)
            )

            redshiftTotalBandwidth = redshift.returnResult()

            if abs((int(totalBandwidth) - int(redshiftTotalBandwidth[0][0])) / int(totalBandwidth)) > 0.005:
                raise KeyError('Record exceed tolorent: fastly ' +
                               totalBandwidth + ' & Redshift ' + redshiftTotalBandwidth[0][0])
            print('Fastly Bandwidth: {:.2f} GB'.format(
                (int(totalBandwidth)/1000000000)))
            print('Redshift  Bandwidth: {:.2f} GB'.format(
                (int(redshiftTotalBandwidth[0][0])/1000000000)))
            print('Record within tolorent')

            print("************************************************************")

            redshift.closeEverything()
            print("Connection closed")
        else:
            print("No new query")
            redshift.closeEverything()
            print("Connection closed")
    except:
        redshift.closeEverything()

        raise sys.exc_info()[0]


if __name__ == "__main__":
    main()

#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on Apr 09, 2017

@author: Daniel Andrzejewski <daniel@andrzejewski.ovh>

@file: sensu_decommission_stale_check_results.py
'''

import argparse
import cStringIO
import datetime
import json
import pycurl
import re
import sys
import time


OK = 0
WARN = 1
CRIT = 2


def Exit(status, summary):
    output = 'DECOMMISSION_STALE_CHECK_RESULTS {0}'.format(summary.replace('\n', ' '))
    print output
    sys.exit(status)


def CurlRequest(url, request_type, timeout):
    #initialize response buffer
    buff = cStringIO.StringIO()
    c = pycurl.Curl()

    c.setopt(c.URL, url)
    c.setopt(c.CUSTOMREQUEST, request_type)
    c.setopt(c.WRITEFUNCTION, buff.write)
    c.setopt(c.HEADERFUNCTION, buff.write)
    c.setopt(c.CONNECTTIMEOUT, int(timeout))

    c.perform() # request

    #get response header
    result = buff.getvalue()

    #get status and descritpion
    response_status = result.splitlines()[-1]

    #get HTTP response code from reply header
    response_code = int(c.getinfo(c.HTTP_CODE))

    buff.close()
    c.close()

    return response_code, response_status


def main():
    MAX_CONNECTION_TIMEOUT = 20.0

    description = '''
        This script decommissions stale sensu check results. Stale check result is
        a check result which execution timestamp is older than x seconds.
    '''

    argp = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)

    # command line options (arguments)
    argp.add_argument('-H', '--sensu-host', metavar='STRING', dest='host', required=True,
                      help='Sensu server e.g. sensu-server.example', default=None)

    argp.add_argument('-p', '--sensu-port', metavar='NUMBER', dest='port', required=True,
                      help='Sensu port e.g. 4567', default=None)

    argp.add_argument('-t', '--time', metavar='NUMBER', dest='hours', required=True,
                      help='Number of hours after which the check result shall be decommissioned e.g. 48', default=None)

    #get arguments to 'args'
    args = argp.parse_args()

    host = args.host
    port = int(args.port)
    time_hours = int(args.hours)

    # get the list of clients
    url = 'http://{host}:{port}/clients'.format(host=host, port=port)
    request_type = 'GET'

    try:
        response_code, response_status = CurlRequest(url, request_type, MAX_CONNECTION_TIMEOUT)

    except TypeError as e:
        Exit(status = CRIT, summary = str(e))

    except pycurl.error as e:
        Exit(status = CRIT, summary = str(e))

    if response_code <> 200:
        msg = 'Error: expected return code is 200 but received {0}'.format(response_code)
        Exit(status = CRIT, summary = msg)

    data = json.loads(response_status)

    clients = []

    for client in data:
        client_name = client['name']
        if client not in clients:
            clients.append(client_name)

    num_decommissioned_check_results = 0
    decommission_check_results = ''
    now_timestamp = int(time.time())

    for client in clients:

        # get all the client check results
        request_type = 'GET'
        url = 'http://{host}:{port}/results/{client}'.format(host=host, port=port,client=client)

        try:
            response_code, response_status = CurlRequest(url, request_type, MAX_CONNECTION_TIMEOUT)

        except TypeError as e:
            Exit(status = CRIT, summary = str(e))

        except pycurl.error as e:
            Exit(status = CRIT, summary = str(e))

        if response_code <> 200:
            msg = 'Error: expected return code is 200 but received {0}'.format(response_code)
            Exit(status = CRIT, summary = msg)

        results = json.loads(response_status)

        # compare last execution timestamp with the current timestamp
        for result in results:
            check = result['check']

            check_name = check['name']
            execution_timestamp = check['executed']
            execution_date = datetime.datetime.fromtimestamp(int(execution_timestamp)).strftime('%Y-%m-%d %H:%M:%S')

            time_delta_in_seconds = now_timestamp - execution_timestamp
            time_delta_in_hours = time_delta_in_seconds / 3600.0

            if time_delta_in_hours > time_hours:

                # decommission stale check result
                request_type = 'DELETE'
                url = 'http://{host}:{port}/results/{client}/{check}'.format(host=host, port=port, client=client, check=check_name)

                try:
                    response_code, response_status = CurlRequest(url, request_type, MAX_CONNECTION_TIMEOUT)

                except TypeError as e:
                    Exit(status = CRIT, summary = str(e))

                except pycurl.error as e:
                    Exit(status = CRIT, summary = str(e))

                if response_code <> 204:
                    msg = 'Error when decommissioning {0} of {1}: expected return code is 204 but received {2}'.format(check_name, client, response_code)
                    Exit(status = CRIT, summary = msg)
                else:
                    num_decommissioned_check_results += 1
                    print 'Decommissioned {0} check result on {1} client. {0} was last executed {2} hours ago ({3})'.format(check_name, client, round(time_delta_in_hours, 2), execution_date)

    summary = 'decommissioned {0} check results'.format(num_decommissioned_check_results)

    Exit(status = OK, summary = summary)


if __name__ == '__main__':
    main()


#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on Apr 09, 2017

@author: Daniel Andrzejewski <daniel@andrzejewski.ovh>

@file: sensu_decommission_stale_clients.py
'''

import argparse
import cStringIO
import json
import pycurl
import re
import sys


OK = 0
WARN = 1
CRIT = 2


def Exit(status, summary):
    output = 'DECOMMISSION_STALE_CLIENTS {0}'.format(summary.replace('\n', ' '))
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
        This script decommissions stale sensu clients. Stale client is a client
        which does not send a keepalive in longer than x seconds.
    '''

    argp = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)

    # command line options (arguments)
    argp.add_argument('-H', '--sensu-host', metavar='STRING', dest='host', required=True,
                      help='Sensu server e.g. sensu-server.example', default=None)

    argp.add_argument('-p', '--sensu-port', metavar='NUMBER', dest='port', required=True,
                      help='Sensu port e.g. 4567', default=None)

    argp.add_argument('-t', '--time', metavar='NUMBER', dest='seconds', required=True,
                      help='Keepalive time in seconds after which the client shall be decommissioned e.g. 14400', default=None)

    #get arguments to 'args'
    args = argp.parse_args()

    host = args.host
    port = int(args.port)
    keepalive_seconds = int(args.seconds)

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

    num_decommissioned_clients = 0
    decommission_clients = ''

    for client in clients:

        # get the client keepalive check result
        request_type = 'GET'
        url = 'http://{host}:{port}/results/{client}/keepalive'.format(host=host, port=port,client=client)

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

        keepalive_check_output = results['check']['output']

        # get number of seconds from the last received keepalive
        m = re.match(r'No keepalive sent from client for (\d*?) seconds .*', keepalive_check_output, re.M|re.I)

        if m is not None:
            seconds_from_last_keepalive = int(m.group(1))

            if seconds_from_last_keepalive > keepalive_seconds:

                # decommission stale client
                request_type = 'DELETE'
                url = 'http://{host}:{port}/clients/{client}'.format(host=host, port=port,client=client)

                try:
                    response_code, response_status = CurlRequest(url, request_type, MAX_CONNECTION_TIMEOUT)

                except TypeError as e:
                    Exit(status = CRIT, summary = str(e))

                except pycurl.error as e:
                    Exit(status = CRIT, summary = str(e))

                if response_code <> 202:
                    msg = 'Error when decommissioning {0}: expected return code is 202 but received {1}'.format(client, response_code)
                    Exit(status = CRIT, summary = msg)
                else:
                    num_decommissioned_clients += 1
                    decommission_clients += ' {client}'.format(client=client)

    summary = 'decommissioned {0} clients :{1}'.format(num_decommissioned_clients, decommission_clients)

    Exit(status = OK, summary = summary)


if __name__ == '__main__':
    main()


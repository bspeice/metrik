from __future__ import print_function
from luigi import build
from datetime import datetime
from argparse import ArgumentParser
from dateutil.parser import parse
from six import StringIO
from subprocess import check_output
from os import path

from metrik.conf import get_config
from metrik.flows.rates_flow import LiborFlow
from metrik.flows.equities_flow import EquitiesFlow
from metrik import __version__

flows = {
    'LiborFlow': LiborFlow,
    'EquitiesFlow': EquitiesFlow
}


def run_flow(flow_class, present, live):
    build([flow_class(present=present, live=live)])


def build_cron_file():
    EXEC = 'metrik'
    FLOW_FLAG = '-f'
    USER = 'root'

    metrik_command_location = path.dirname(check_output(['which', 'metrik']))
    cron_header = '''# cron.d entries for metrik

SHELL=/bin/sh
PATH={}

'''.format(metrik_command_location)
    cron_strings = []
    for flow_name, flow_class in flows.items():
        cron_string = flow_class.get_schedule().get_cron_string()
        cron_strings.append(
            cron_string + ' ' + USER + ' ' + EXEC + ' ' +
            FLOW_FLAG + ' ' + flow_name
        )

    return cron_header + '\n'.join(cron_strings) + '\n'


def list_flows():
    return "Available:\n\t" + "\n\t".join(flows.keys())


def handle_commandline():
    present_moment = datetime.now()
    parser = ArgumentParser(description='Capture ALL THE DATA off the Internet.')
    parser.add_argument('-c', '--cron', dest='cron', action='store_true',
                        help='Build the cron file used to schedule'
                             'running all flows')
    parser.add_argument('-d', '--date', dest='present',
                        help='Run a flow as if it was this time '
                             '(default: %(default)s).',
                        default=present_moment)
    parser.add_argument('-f', '--flow', dest='flow', help='The flow to be run')
    parser.add_argument('-l', '--list-flows', dest='list', action='store_true',
                        help='List all available flows to be run.')
    parser.add_argument('-o', '--config', action='store_true',
                        help='Output the default configuration to allow you '
                             'tweaking the settings. Please place at ~/.metrik,'
                             'or /etc/metrik')
    parser.add_argument('-t', '--tainted', action='store_true', default=False,
                        help='Run in \'tainted\' mode, which treats the system'
                             ' as if it were live even though a `-d` switch '
                             'may be present.')
    parser.add_argument('-v', '--version', action='version',
                        version=__version__)
    args = parser.parse_args()

    if args.cron:
        print(build_cron_file())
    elif args.list:
        print(list_flows())
    elif args.config:
        config = get_config()
        s = StringIO
        config.write(s)
        print(s.getvalue())
    elif args.flow:
        if type(args.present) is datetime:
            run_flow(flows[args.flow], args.present, True)
        else:
            run_flow(flows[args.flow], parse(args.present), args.tainted)
    else:
        print("No actions specified, exiting.")


if __name__ == '__main__':
    handle_commandline()

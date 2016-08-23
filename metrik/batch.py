from __future__ import print_function
from luigi import build
from datetime import datetime
from argparse import ArgumentParser
from dateutil.parser import parse

from metrik.flows.rates_flow import LiborFlow

flows = {
    'LiborFlow': LiborFlow
}


def run_flow(flow_class, present):
    build([flow_class(present=present)])


def build_cron_file():
    EXEC = 'metrik'
    FLOW_FLAG = '-f'

    cron_strings = []
    for flow_name, flow_class in flows.items():
        cron_string = flow_class.get_schedule().get_cron_string()
        cron_strings.append(
            cron_string + ' ' + EXEC + ' ' + FLOW_FLAG + ' ' + flow_name
        )

    return '\n'.join(cron_strings)


def list_flows():
    pass


def handle_commandline():
    parser = ArgumentParser(description='Capture ALL THE DATA off the Internet.')
    parser.add_argument('-c', '--cron', dest='cron', action='store_true',
                        help='Build the cron file used to schedule'
                             'running all flows')
    parser.add_argument('-d', '--date', dest='present',
                        help='Run a flow as if it was this time '
                             '(default: %(default)s).',
                        default=datetime.now())
    parser.add_argument('-f', '--flow', dest='flow', help='The flow to be run')
    parser.add_argument('-l', '--list-flows', dest='list', action='store_true',
                        help='List all available flows to be run.')
    args = parser.parse_args()

    if args.cron:
        print(build_cron_file())
    elif args.list:
        print(list_flows())
    elif args.flow:
        if type(args.present) is datetime:
            run_flow(flows[args.flow], args.present)
        else:
            run_flow(flows[args.flow], parse(args.present))
    else:
        print("No actions specified, exiting.")


if __name__ == '__main__':
    handle_commandline()

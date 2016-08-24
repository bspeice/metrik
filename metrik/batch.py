from __future__ import print_function
from luigi import build
from datetime import datetime
from argparse import ArgumentParser
from dateutil.parser import parse

from metrik.flows.rates_flow import LiborFlow
from metrik.flows.equities_flow import EquitiesFlow

flows = {
    'LiborFlow': LiborFlow,
    'EquitiesFlow': EquitiesFlow
}


def run_flow(flow_class, present, live):
    build([flow_class(present=present, live=live)])


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
    args = parser.parse_args()

    if args.cron:
        print(build_cron_file())
    elif args.list:
        print(list_flows())
    elif args.flow:
        if type(args.present) is datetime:
            run_flow(flows[args.flow], args.present, True)
        else:
            run_flow(flows[args.flow], parse(args.present), False)
    else:
        print("No actions specified, exiting.")


if __name__ == '__main__':
    handle_commandline()

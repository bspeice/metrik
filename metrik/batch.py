from luigi import build
from metrik.flows.libor_flow import LiborFlow
from datetime import datetime


if __name__ == '__main__':
    l = LiborFlow(datetime(2016, 5, 9).date())

    build([l], local_scheduler=True)
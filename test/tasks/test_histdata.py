from unittest import TestCase
from io import BytesIO
import pandas as pd

from metrik.tasks.histdata import HistDataWrapper


class HistDataWrapperTest(TestCase):

    def test_eur_usd_july2016_bid(self):
        zip_content = HistDataWrapper.fetch_zip('EURUSD', 2016, 7, 'BID')
        assert len(zip_content.getvalue()) > 0
        csv_content = HistDataWrapper.fetch_csv(zip_content, 'EURUSD', 2016, 7, 'BID')
        assert len(csv_content.getvalue()) > 0
        content_dict = HistDataWrapper.parse_csv(csv_content)
        assert len(content_dict) > 0

    def test_usd_cad_july2016_bid(self):
        zip_content = HistDataWrapper.fetch_zip('USDCAD', 2016, 7, 'BID')
        assert len(zip_content.getvalue()) > 0
        csv_content = HistDataWrapper.fetch_csv(zip_content, 'USDCAD', 2016, 7, 'BID')
        assert len(csv_content.getvalue()) > 0
        content_dict = HistDataWrapper.parse_csv(csv_content)
        assert len(content_dict) > 0

    def test_eur_usd_july2016_ask(self):
        zip_content = HistDataWrapper.fetch_zip('EURUSD', 2016, 7, 'ASK')
        assert len(zip_content.getvalue()) > 0
        csv_content = HistDataWrapper.fetch_csv(zip_content, 'EURUSD', 2016, 7, 'ASK')
        assert len(csv_content.getvalue()) > 0
        content_dict = HistDataWrapper.parse_csv(csv_content)
        assert len(content_dict) > 0
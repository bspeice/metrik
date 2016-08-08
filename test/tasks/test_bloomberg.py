from unittest import TestCase

from metrik.conf import USER_AGENT
from metrik.tasks.bloomberg import BloombergEquityInfo
from metrik.tasks.bloomberg import BloombergFXPrice


class BloombergTest(TestCase):
    def test_correct_info_apple(self):
        sector, industry, sub_industry = \
            BloombergEquityInfo.retrieve_info("AAPL:US", USER_AGENT)

        assert sector == 'Technology'
        assert industry == 'Hardware'
        assert sub_industry == 'Communications Equipment'

    def test_correct_info_kcg(self):
        sector, industry, sub_industry = \
            BloombergEquityInfo.retrieve_info("KCG:US", USER_AGENT)

        assert sector == 'Financials'
        assert industry == 'Institutional Financial Svcs'
        assert sub_industry == 'Institutional Brokerage'

    def test_fx_triangle_euj(self):
        eur_usd = BloombergFXPrice.retrieve_price('EURUSD:CUR', USER_AGENT)
        usd_jpy = BloombergFXPrice.retrieve_price('USDJPY:CUR', USER_AGENT)
        eur_jpy = BloombergFXPrice.retrieve_price('EURJPY:CUR', USER_AGENT)

        diff = abs(eur_usd * usd_jpy - eur_jpy)
        assert diff < .05

    def test_fx_triangle_ghc(self):
        gbp_hkd = BloombergFXPrice.retrieve_price('GBPHKD:CUR', USER_AGENT)
        hkd_inr = BloombergFXPrice.retrieve_price('HKDCAD:CUR', USER_AGENT)
        gbp_inr = BloombergFXPrice.retrieve_price('GBPCAD:CUR', USER_AGENT)

        diff = abs(gbp_hkd * hkd_inr - gbp_inr)
        assert diff < .05
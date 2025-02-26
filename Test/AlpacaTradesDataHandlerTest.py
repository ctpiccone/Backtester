import unittest

import AlpacaData
from Data import AlpacaDataHandler
import datetime
import pandas as pd
tickers = ["AAPL", "TSLA"]
START_DATE = datetime.datetime(2022, 8,30,6,2,41)
END_DATE = datetime.datetime(2022, 8, 30, 10, 0, 0)




class TestInitialize(unittest.TestCase):
    def setUp(self) -> None:
        self.dh = AlpacaData.TradesAlpacaDataHandler(tickers)


    def test_init(self):
        self.assertIsNone(self.dh.queue)
        self.assertTrue(self.dh.current_data == self.dh.data and [x for x in self.dh.data.values() if x is None])
        self.assertEqual(self.dh.tickers, tickers)


    def test_initialize(self):
        self.dh.set_dates(start_date=START_DATE, end_date=END_DATE)
        self.dh.initialize()
        for ticker in self.dh.current_data.keys():
            self.assertIsNone(self.dh.current_data[ticker])

    def test_initial(self):
        self.dh.set_dates(start_date=START_DATE, end_date=END_DATE)
        self.dh.initialize()
        #start date is too early, so no update will happen
        cur_date = self.dh.update(START_DATE)
        self.assertIsNone(self.dh.current_data["AAPL"])
        #not update with time that is present in data
        self.dh.update(cur_date)
        # test that AAPL is updated to first trade
        self.assertTrue(self.dh.data["AAPL"].iloc[[0]].equals(self.dh.current_data["AAPL"]))
        # test that TSLA is empty, because at this time there are no trades
        self.assertIsNone(self.dh.current_data["TSLA"])


class TestGetLatest(unittest.TestCase):

    def setUp(self) -> None:
        self.dh = AlpacaData.TradesAlpacaDataHandler(tickers)
        self.dh.set_dates(start_date=START_DATE, end_date=END_DATE)
        self.dh.initialize()
        self.dh.update(START_DATE)
        #make sure there is data present
        if len([i for i in list(self.dh.current_data.values()) if i is not None])==0:
            self.dh.update(self.dh.update(START_DATE))

    def test_initial_get_latest(self):
        self.dh.get_latest("AAPL")
        self.assertTrue(self.dh.data["AAPL"].iloc[[0]].equals(self.dh.get_latest("AAPL")))

    def test_TSLA_get_latest(self):
        cur_datetime = START_DATE.replace(tzinfo=datetime.timezone.utc)
        while self.dh.data["TSLA"] is None:
            cur_datetime = cur_datetime + datetime.timedelta(milliseconds=5000)
            self.dh.update(cur_datetime)
        # TSLA has data
        while cur_datetime<=self.dh.data["TSLA"].index[0]:
            self.assertIsNone(self.dh.current_data["TSLA"])
            cur_datetime = cur_datetime + datetime.timedelta(milliseconds=5000)
            self.dh.update(cur_datetime)
        # TSLA should only have one trade
        self.assertTrue(self.dh.data["TSLA"].iloc[[0]].equals(self.dh.get_latest("TSLA")))
        #Ensure that AAPL has correct number of rows
        self.assertTrue(self.dh.data["AAPL"][self.dh.data["AAPL"].index<=cur_datetime].equals(self.dh.current_data["AAPL"]))



    def test_get_multiple_latest(self):
        #Update AAPL 3 times, only ask for recent 2
        cur_datetime = START_DATE
        self.dh.update(self.dh.update(self.dh.update(cur_datetime)))
        self.assertTrue(self.dh.data["AAPL"].iloc[1:3].equals(self.dh.get_latest("AAPL", 2)))

    def test_ask_for_more(self):
        #Update AAPL 3 times, only ask for recent 2
        cur_datetime = START_DATE
        self.dh.update(self.dh.update(self.dh.update(cur_datetime)))
        self.assertTrue(self.dh.data["AAPL"].iloc[0:3].equals(self.dh.get_latest("AAPL", 4)))


class TestGetLatestDate(unittest.TestCase):

    def setUp(self) -> None:
        self.dh = AlpacaData.TradesAlpacaDataHandler(tickers)
        self.dh.set_dates(start_date=START_DATE, end_date=END_DATE)
        self.dh.initialize()
        self.dh.update(START_DATE)
        #make sure there is data present
        if len([i for i in list(self.dh.current_data.values()) if i is not None])==0:
            self.dh.update(self.dh.update(START_DATE))


    def test_initial_date(self):
        self.dh.update(START_DATE)
        #test both type and timezone
        self.assertTrue(type(self.dh.get_latest_datetime("AAPL")) == pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(self.dh.get_latest_datetime("AAPL").tzinfo == datetime.timezone.utc)
        self.assertEqual(self.dh.get_latest_datetime("AAPL"), self.dh.data["AAPL"].index[0])

    def test_updated_date(self):
        self.dh.update(self.dh.update(self.dh.get_latest_datetime("AAPL")))
        self.assertTrue(type(self.dh.get_latest_datetime("AAPL")) == pd._libs.tslibs.timestamps.Timestamp)
        self.assertTrue(self.dh.get_latest_datetime("AAPL").tzinfo == datetime.timezone.utc)
        self.assertEqual(self.dh.get_latest_datetime("AAPL"), self.dh.data["AAPL"].index[1])

    def test_TSLA_get_latest_date(self):
        cur_datetime = START_DATE.replace(tzinfo=datetime.timezone.utc)
        while self.dh.data["TSLA"] is None:
            cur_datetime = cur_datetime + datetime.timedelta(milliseconds=5000)
            self.dh.update(cur_datetime)
        # TSLA has data
        while cur_datetime<=self.dh.data["TSLA"].index[0]:
            self.assertIsNone(self.dh.current_data["TSLA"])
            cur_datetime = cur_datetime + datetime.timedelta(milliseconds=5000)
            self.dh.update(cur_datetime)
        # check that TSLA date is equal to first available date
        self.assertEqual(self.dh.data["TSLA"].index[0], self.dh.get_latest_datetime("TSLA"))
        # Ensure that AAPL has correct number of dates
        self.assertEqual(list(self.dh.data["AAPL"][self.dh.data["AAPL"].index <= cur_datetime].index), list(self.dh.current_data["AAPL"].index))

    def test_get_multiple_latest_dates(self):
        # Update AAPL 3 times, only ask for recent 2
        cur_datetime = START_DATE
        self.dh.update(self.dh.update(self.dh.update(cur_datetime)))
        self.assertEquals(list(self.dh.data["AAPL"].index[1:3]), list(self.dh.get_latest_datetime("AAPL", 2)))

    def test_ask_for_more_dates(self):
        # Update AAPL 3 times, only ask for recent 2
        cur_datetime = START_DATE
        self.dh.update(self.dh.update(self.dh.update(cur_datetime)))
        self.assertEqual(list(self.dh.data["AAPL"].index[0:3]), list(self.dh.get_latest_datetime("AAPL", 4)))

class TestUpdate(unittest.TestCase):

    def setUp(self) -> None:
        self.dh = AlpacaData.TradesAlpacaDataHandler(tickers)
        self.dh.set_dates(start_date=START_DATE, end_date=END_DATE)
        self.dh.initialize()
        self.dh.update(START_DATE)
        # make sure there is data present
        if len([i for i in list(self.dh.current_data.values()) if i is not None]) == 0:
            self.dh.update(self.dh.update(START_DATE))
        self.og_data = self.dh.data.copy()

    def test_data(self):
        cur_datetime = self.dh.start_date
        while True:
            self.dh.update(cur_datetime)
            cur_datetime = cur_datetime + datetime.timedelta(milliseconds=500000)
            if not self.dh.data["AAPL"].equals(self.og_data["AAPL"]) or not self.dh.data["TSLA"].equals(self.og_data["TSLA"]):
                break
        # dh has now had to refresh
        self.assertFalse(self.dh.current_data["AAPL"].iloc[[len(self.dh.current_data["AAPL"].index)-1]].equals(self.dh.data["AAPL"].iloc[[0]]))


if __name__ == '__main__':
    unittest.main()

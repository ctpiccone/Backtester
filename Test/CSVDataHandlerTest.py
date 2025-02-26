import unittest
from Data import CSVDataHandler
import pandas as pd
import queue
import datetime


class TestInitialize(unittest.TestCase):
    def setUp(self) -> None:
        self.dh = CSVDataHandler(r"C:\Users\cameron.Piccone\\Documents\TestData", tickers=["KR"])
        self.dh.initialize()

    def test_import(self):
        #ensure data was added
        self.assertTrue(len(self.dh.data) != 0)
        # ensure current data only has one timeframe
        self.assertTrue(sum([len(v) for v in self.dh.current_data.values()]) / len(self.dh.current_data) == 1)
        # ensure ticker is in both data and current
        self.assertIn("KR", self.dh.data.keys())
        self.assertIn("KR", self.dh.current_data.keys())

    def test_index(self):
        self.assertTrue(isinstance(self.dh.data[list(self.dh.data.keys())[0]].index[0], pd._libs.tslibs.timestamps.Timestamp))

    def test_ticker(self):
        self.assertTrue('.' not in self.dh.tickers)
        self.assertIn("KR", self.dh.tickers)


class TestUpdate(unittest.TestCase):
    def setUp(self) -> None:
        self.dh = CSVDataHandler(r"C:\Users\cameron.Piccone\\Documents\TestData")
        self.dh.initialize()
        self.dh.set_events(queue.Queue())

    def testLastDate(self):
        self.dh.update(datetime.timedelta(days=1) + self.dh.get_latest_datetime(list(self.dh.current_data.keys())[0]))
        self.assertTrue(sum([len(self.dh.current_data[k]) for k in self.dh.current_data.keys()])/len(self.dh.current_data) == 2)



if __name__ == '__main__':
    unittest.main()

import unittest
from time import sleep

from pyimport.timer import Timer


class TestTimer(unittest.TestCase):
    def test_timer(self):
        t = Timer()
        t.start()
        sleep(1)
        elapsed = t.stop()
        self.assertTrue(elapsed > 1.0)
        self.assertTrue(elapsed < 1.01)
        print(elapsed)

if __name__ == '__main__':
    unittest.main()

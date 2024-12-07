import unittest


class SteamingProcessingTestCase(unittest.TestCase):
    def setUp(self):
        self.example_value = 0

    def test_data_processing_example(self):
        self.assertEqual(self.example_value, 0)


if __name__ == "__main__":
    unittest.main()

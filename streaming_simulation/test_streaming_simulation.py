import unittest
from flask import Flask
from streaming_simulation import app
import os


class StreamingSimulationTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True
        current_path = os.path.dirname(os.path.abspath(__file__))
        app.config["file_path"] = os.path.join(current_path, "test_data.csv")
        app.config["delay"] = 10

    def test_data_stream(self):
        response = self.app.get("/data")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.is_streamed)
        data = next(response.response)
        self.assertIsNotNone(data)


if __name__ == "__main__":
    unittest.main()

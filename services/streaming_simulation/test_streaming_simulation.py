import unittest
from streaming_simulation import app


class StreamingSimulationTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True
        app.config["delay"] = 10
        app.config["address"] = "127.0.0.1"

    def test_data_stream(self):
        response = self.app.get("/data/0")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.is_streamed)
        data = next(response.response)
        self.assertIsNotNone(data)


if __name__ == "__main__":
    unittest.main()

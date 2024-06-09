import unittest
from main import *

class TestMain(unittest.TestCase):

    def setUp(self):
        self.df = download_json_to_dataframe(bi_recipes_url)
        self.df_difficulty = transform_recipes(self.df)

    def test_download_json_to_dataframe(self):
        self.assertIsNotNone(self.df)
        self.assertIsInstance(self.df, pd.DataFrame)

    def test_sanitize_dataframe(self):
        df_sanitized = sanitize_dataframe(self.df)
        self.assertIsNotNone(df_sanitized)
        self.assertIsInstance(df_sanitized, pd.DataFrame)

    def test_filter_recipes(self):
        df_filtered = filter_recipes(self.df_difficulty, column='ingredients', phrase='Chil')
        self.assertIsNotNone(df_filtered)
        self.assertIsInstance(df_filtered, pd.DataFrame)

    def test_parse_time(self):
        time_str = 'PT1H30M'
        total_minutes = parse_time(time_str)
        self.assertEqual(total_minutes, 90)

    def test_determine_difficulty(self):
        time = 45
        difficulty = determine_difficulty(time)
        self.assertEqual(difficulty, "Medium")

    def test_set_difficulty(self):
        df_difficulty = set_difficulty(self.df_difficulty)
        self.assertIsNotNone(df_difficulty)
        self.assertIsInstance(df_difficulty, pd.Series)

    def test_calculate_total_time(self):
        df_total_time = calculate_total_time(self.df_difficulty)
        self.assertIsNotNone(df_total_time)
        self.assertIsInstance(df_total_time, pd.DataFrame)

    def test_calculate_average_times(self):
        df_average_times = calculate_average_times(self.df_difficulty)
        self.assertIsNotNone(df_average_times)
        self.assertIsInstance(df_average_times, pd.DataFrame)

    def test_clean_difficulty_data(self):
        df_cleaned = clean_difficulty_data(self.df_difficulty)
        self.assertIsNotNone(df_cleaned)
        self.assertIsInstance(df_cleaned, pd.DataFrame)

if __name__ == '__main__':
    unittest.main()
import traceback
import pandas as pd
from fuzzywuzzy import fuzz
import re
import numpy as np

# https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
# https://pandas.pydata.org/pandas-docs/stable/user_guide/copy_on_write.html#copy-on-write
pd.options.mode.copy_on_write = True

parse_time_empty_error = "Empty string"
bi_recipes_url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"

def download_json_to_dataframe(url):
    return pd.read_json(url, lines=True)

def sanitize_dataframe(df):
    df_sanitized = remove_escape_character(df)
    df_sanitized = replace_ampersands(df_sanitized)
    df_sanitized = replace_semicolons_with_commas(df_sanitized)
    return df_sanitized

def remove_escape_character(df):
    return df.map(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

def replace_ampersands(df):
    return df.map(lambda x: x.replace('&amp', '&') if isinstance(x, str) else x)

def replace_semicolons_with_commas(df):
    return df.map(lambda x: x.replace(';', ',') if isinstance(x, str) else x)

def filter_recipes(df, column='ingredients', phrase='Chil'):
    def fuzzy_match(x):
        return fuzz.partial_token_set_ratio(x, phrase) > 90

    df_filtered = df[df[column].apply(fuzzy_match)]
    return df_filtered

def parse_time(time_str):
    if not time_str:
        return np.nan

    if time_str == 'PT':
        return 0

    time_str = time_str.replace('PT', '')
    hours, minutes = extract_time_values(time_str)
    total_minutes = calculate_total_minutes(hours, minutes)

    return total_minutes

def calculate_total_minutes(hours, minutes):
    total_minutes = 0
    if hours:
        total_minutes += int(hours.group(1)) * 60
    if minutes:
        total_minutes += int(minutes.group(1))
    return total_minutes

def extract_time_values(time_str):
    hours = re.search('(\d+)H', time_str)
    minutes = re.search('(\d+)M', time_str)
    return hours,minutes
    
def determine_difficulty(time):
    if np.isnan(time):
        return "Unknown"
    elif time > 60:
        return "Hard"
    elif 30 <= time <= 60:
        return "Medium"
    else: 
        return "Easy"

def set_difficulty(df):
    return df['totalTime'].apply(determine_difficulty)

def calculate_total_time(df):
    df_copy = df.copy()
    df_copy.loc[:, 'prepTimeInMins'] = df['prepTime'].apply(parse_time)
    df_copy.loc[:, 'cookTimeInMins'] = df['cookTime'].apply(parse_time)
    df_copy.loc[:, 'totalTime'] = df_copy['prepTimeInMins'] + df_copy['cookTimeInMins']
    return df_copy

def calculate_average_times(df):
    df = df.loc[df['difficulty'] != 'Unknown']

    average_times = df.groupby('difficulty')['totalTime'].mean()
    average_times_df = average_times.reset_index()
    average_times_df.columns = ['difficulty', 'averageTotalTime']
    return average_times_df

def dump_to_csv(df, filename):
    df.to_csv(filename, sep='|', index=False)
    print(f"Data successfully written to {filename}")

def clean_difficulty_data(df_difficulty):
    df_cleaned = df_difficulty.drop(['prepTimeInMins', 'cookTimeInMins', 'totalTime'], axis=1)
    df_cleaned.drop_duplicates(inplace=True)
    return df_cleaned

def transform_recipes(df):
    df_cleaned = sanitize_dataframe(df)
    df_filtered = filter_recipes(df_cleaned)
    df_total_time = calculate_total_time(df_filtered)
    df_difficulty = df_total_time.assign(new_column = 'difficulty')
    df_difficulty['difficulty'] = set_difficulty(df_difficulty);
    return df_difficulty

def generate_average_times_csv(df):
    df_average_times = calculate_average_times(df)
    dump_to_csv(df_average_times, 'Results.csv')

def generate_difficulty_csv(df):
    df_final = clean_difficulty_data(df)
    dump_to_csv(df_final, 'Chilies.csv')

if __name__ == "__main__":
    try:
        df = download_json_to_dataframe(bi_recipes_url)
        df_difficulty = transform_recipes(df)
        generate_average_times_csv(df_difficulty)
        generate_difficulty_csv(df_difficulty)
    except KeyError as ke:
        print(f"KeyError: {ke}")
        traceback.print_exc()
    except ValueError as ve:
        print(f"ValueError: {ve}")
        traceback.print_exc()
    except Exception as e:
        print(f"Unexpected error: {e}")
        traceback.print_exc()




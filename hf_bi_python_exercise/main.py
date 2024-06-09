import sys
import traceback
import pandas as pd
from fuzzywuzzy import fuzz
import re
import numpy as np
import concurrent.futures


# https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
# https://pandas.pydata.org/pandas-docs/stable/user_guide/copy_on_write.html#copy-on-write
pd.options.mode.copy_on_write = True

parse_time_empty_error = "Empty string"
bi_recipes_url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"

def download_json_to_dataframe(url):
    return pd.read_json(url, lines=True)

def sanitize_dataframe(df):
    """
    Sanitizes the given dataframe by removing escape characters,
    replacing ampersands, and replacing semicolons with commas.

    Args:
        df (pandas.DataFrame): The input dataframe to be sanitized.

    Returns:
        pandas.DataFrame: The sanitized dataframe.
    """

    df_sanitized = remove_escape_character(df)
    df_sanitized = replace_ampersands(df_sanitized)
    df_sanitized = replace_semicolons_with_commas(df_sanitized)
    return df_sanitized

def check_empty_dataframe(df):
    if df.empty:
        print("The input DataFrame is empty. Exiting the program.")
        sys.exit()

def remove_escape_character(df):
    """
    Removes escape characters from the given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to remove escape characters from.

    Returns:
        pandas.DataFrame: The DataFrame with escape characters removed.
    """
    return df.map(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

def replace_ampersands(df):
    """
    Replaces '&amp' with '&' in the given DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame to perform the replacement on.

    Returns:
    pandas.DataFrame: The DataFrame with '&' replacing '&amp'.
    """
    return df.map(lambda x: x.replace('&amp', '&') if isinstance(x, str) else x)

def replace_semicolons_with_commas(df):
    """
    Replaces semicolons with commas in a DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to be processed.

    Returns:
        pandas.DataFrame: The DataFrame with semicolons replaced by commas.
    """
    return df.map(lambda x: x.replace(';', ',') if isinstance(x, str) else x)

def filter_recipes(df, column='ingredients', phrase='Chil'):
    """
    Filters a DataFrame based on a given column and a partial phrase match.

    Args:
        df (pandas.DataFrame): The DataFrame to be filtered.
        column (str, optional): The column in the DataFrame to be used for filtering. Defaults to 'ingredients'.
        phrase (str, optional): The partial phrase to match in the specified column. Defaults to 'Chil'.

    Returns:
        pandas.DataFrame: The filtered DataFrame containing only the rows that match the partial phrase.

    """
    def fuzzy_match(x):
        """
        Check if the partial token set ratio between `x` and `phrase` is greater than 90.

        Parameters:
        x (str): The string to compare with `phrase`.

        Returns:
        bool: True if the partial token set ratio is greater than 90, False otherwise.
        """
        return fuzz.partial_token_set_ratio(x, phrase) > 90

    df_filtered = df[df[column].apply(fuzzy_match)]
    return df_filtered

def parse_time(time_str):
    """
    Parses a time string and returns the total minutes.

    Args:
        time_str (str): The time string to be parsed.

    Returns:
        int: The total minutes calculated from the time string.

    """
    if not time_str:
        return np.nan

    if time_str == 'PT':
        return 0

    time_str = time_str.replace('PT', '')
    hours, minutes = extract_time_values(time_str)
    total_minutes = calculate_total_minutes(hours, minutes)

    return total_minutes

def calculate_total_minutes(hours, minutes):
    """
    Calculates the total number of minutes based on the given hours and minutes.

    Args:
        hours (re.Match or None): A regular expression match object representing the hours.
        minutes (re.Match or None): A regular expression match object representing the minutes.

    Returns:
        int: The total number of minutes.

    """
    total_minutes = 0
    if hours:
        total_minutes += int(hours.group(1)) * 60
    if minutes:
        total_minutes += int(minutes.group(1))
    return total_minutes

def extract_time_values(time_str):
    """
    Extracts the hours and minutes from a time string.

    Args:
        time_str (str): The time string to extract values from.

    Returns:
        tuple: A tuple containing the hours and minutes extracted from the time string.
    """
    hours = re.search('(\d+)H', time_str)
    minutes = re.search('(\d+)M', time_str)
    return hours, minutes
    
def determine_difficulty(time):
    """
    Determines the difficulty level of a recipe based on the given time.

    Parameters:
    time (float): The total time required to prepare and cook the recipe in minutes.

    Returns:
    str: The difficulty level of the recipe. Possible values are "Unknown", "Easy", "Medium", or "Hard".
    """
    if np.isnan(time):
        return "Unknown"
    elif time > 60:
        return "Hard"
    elif 30 <= time <= 60:
        return "Medium"
    else: 
        return "Easy"

def set_difficulty(df):
    """
    Assigns a difficulty level to each recipe in the DataFrame based on the total time required.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing recipe information.

    Returns:
    pandas.Series: A Series containing the assigned difficulty level for each recipe.
    """
    return df['totalTime'].apply(determine_difficulty)

def calculate_total_time(df):
    """
    Calculates the total time for each recipe in the given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame containing recipe information.

    Returns:
        pandas.DataFrame: A copy of the input DataFrame with additional columns 'prepTimeInMins',
        'cookTimeInMins', and 'totalTime' representing the time in minutes for preparation,
        cooking, and the total time for each recipe, respectively.
    """
    df.loc[:, 'prepTimeInMins'] = df['prepTime'].apply(parse_time)
    df.loc[:, 'cookTimeInMins'] = df['cookTime'].apply(parse_time)
    df.loc[:, 'totalTime'] = df['prepTimeInMins'] + df['cookTimeInMins']
    return df

def calculate_average_times(df):
    """
    Calculate the average total time for each difficulty level in the given DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the recipe data.

    Returns:
    pandas.DataFrame: A DataFrame with two columns: 'difficulty' and 'averageTotalTime'.
                      Each row represents the average total time for a specific difficulty level.
    """
    df = df.loc[df['difficulty'] != 'Unknown']

    average_times = df.groupby('difficulty')['totalTime'].mean()
    average_times_df = average_times.reset_index()
    average_times_df.columns = ['difficulty', 'averageTotalTime']
    return average_times_df

def dump_to_csv(df, filename):
    """
    Dump the given DataFrame to a CSV file.

    Args:
        df (pandas.DataFrame): The DataFrame to be dumped.
        filename (str): The name of the output CSV file.

    Returns:
        None

    """
    df.to_csv(filename, sep='|', index=False)
    print(f"Data successfully written to {filename}")

def clean_difficulty_data(df_difficulty):
    """
    Cleans the difficulty data in the given DataFrame.

    Parameters:
    df_difficulty (DataFrame): The DataFrame containing the difficulty data.

    Returns:
    DataFrame: The cleaned DataFrame with the difficulty data.
    """
    df_cleaned = df_difficulty.drop(['prepTimeInMins', 'cookTimeInMins', 'totalTime'], axis=1)
    df_cleaned.drop_duplicates(inplace=True)
    return df_cleaned

def transform_recipes(df):
    """
    Transforms the input dataframe by performing the following steps:
    1. Sanitizes the dataframe by removing any invalid or missing values.
    2. Filters the recipes based on certain criteria.
    3. Calculates the total time for each recipe.
    4. Assigns a new column 'difficulty' to the dataframe.
    5. Sets the difficulty level for each recipe based on certain criteria.

    Args:
        df (pandas.DataFrame): The input dataframe containing the recipes.

    Returns:
        pandas.DataFrame: The transformed dataframe with the added 'difficulty' column.
    """
    df_cleaned = sanitize_dataframe(df)
    df_filtered = filter_recipes(df_cleaned)
    df_total_time = calculate_total_time(df_filtered)
    df_difficulty = df_total_time.assign(new_column='difficulty')
    df_difficulty['difficulty'] = set_difficulty(df_difficulty)
    return df_difficulty

def generate_average_times_csv(df):
    """
    Generate a CSV file containing the average total times by difficulty calculated from the given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame containing the data.

    Returns:
        None
    """
    df_average_times = calculate_average_times(df)
    dump_to_csv(df_average_times, 'Results.csv')

def generate_difficulty_csv(df):
    """
    Generate a recipe CSV file including difficulty data.

    Args:
        df (pandas.DataFrame): The input DataFrame containing difficulty data.

    Returns:
        None
    """
    df_final = clean_difficulty_data(df)
    dump_to_csv(df_final, 'Chilies.csv')

def execute_etl_pipeline(bi_recipes_url):
    """
    Executes the ETL (Extract, Transform, Load) pipeline for processing recipes data.
    Generates two CSV files: 'Chilies.csv' and 'Results.csv'.

    Args:
        bi_recipes_url (str): The URL of the BI recipes data.

    Raises:
        KeyError: If a key error occurs during the execution.
        ValueError: If a value error occurs during the execution.
        Exception: If an unexpected error occurs during the execution.

    Returns:
        None
    """
    try:
        df = download_json_to_dataframe(bi_recipes_url)
        check_empty_dataframe(df)
        df_difficulty = transform_recipes(df)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule each function to run in a separate thread
            thread1 = executor.submit(generate_average_times_csv, df_difficulty)
            thread2 = executor.submit(generate_difficulty_csv, df_difficulty)
    except KeyError as ke:
        print(f"KeyError: {ke}")
        traceback.print_exc()
    except ValueError as ve:
        print(f"ValueError: {ve}")
        traceback.print_exc()
    except Exception as e:
        print(f"Unexpected error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    execute_etl_pipeline(bi_recipes_url)




# recipes-etl

[`Recipes-ETL`](command:_github.copilot.openRelativePath?%5B%7B%22scheme%22%3A%22file%22%2C%22authority%22%3A%22%22%2C%22path%22%3A%22%2FUsers%2Fibrahimnasser%2FDesktop%2FPersonals%2FHelloFresh%2Frecipes-etl%2Fhf_bi_python_exercise%2Fmain.py%22%2C%22query%22%3A%22%22%2C%22fragment%22%3A%22%22%7D%5D "/Users/ibrahimnasser/Desktop/Personals/HelloFresh/recipes-etl/hf_bi_python_exercise/main.py") is a Python script that performs an Extract, Transform, Load (ETL) operation on a dataset of recipes. The script downloads a JSON file from a specified URL, processes the data, and outputs the results to two CSV files.

## Features

1. **Data Extraction**: The script downloads a JSON file from a specified URL and converts it into a pandas DataFrame.

2. **Data Transformation**: The script performs several transformations on the data:

    - Sanitizes the data by removing escape characters, replacing ampersands, and replacing semicolons with commas.
    - Filters the recipes based on a partial phrase match in the 'ingredients' column.
    - Calculates the total time for each recipe by parsing the 'prepTime' and 'cookTime' fields.
    - Assigns a difficulty level to each recipe based on the total time required.

3. **Data Loading**: The script generates two CSV files:

    - 'Chilies.csv': Contains the recipe data with an additional 'difficulty' column.
    - 'Results.csv': Contains the average total time for each difficulty level.

4. **Error Handling**: The script includes error handling for KeyErrors, ValueErrors, and other unexpected errors. If an error occurs, the script prints the error message and a traceback.

5. **Concurrency**: The script uses the [`concurrent.futures`](command:_github.copilot.openSymbolFromReferences?%5B%7B%22%24mid%22%3A1%2C%22path%22%3A%22%2FLibrary%2FDeveloper%2FCommandLineTools%2FLibrary%2FFrameworks%2FPython3.framework%2FVersions%2F3.9%2Flib%2Fpython3.9%2Fconcurrent%2F__init__.py%22%2C%22scheme%22%3A%22file%22%7D%2C%7B%22line%22%3A0%2C%22character%22%3A0%7D%5D "../../../../../../Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/lib/python3.9/concurrent/__init__.py") module to generate the two CSV files concurrently, improving the script's efficiency.

## Usage

To run the script, use the following command:

```bash
pip install -r requirements.txt
python /hf_bi_python_excercise/main.py 
```

The script will download the JSON file, process the data, and generate the two CSV files in the current directory.

## Scheduler

scheduler.py is a Python script that schedules the execution of the main.py script. It uses the schedule library to run the script at specified intervals.

To run the script, use the following command:

```bash
python /hf_bi_python_excercise/scheduler.py 
```

The script will start running and will execute main.py at the specified intervals. The script will continue running until it is stopped manually.
import pandas as pd
import re
import yaml
import string

# This is intended to be run inside Jupyter Notebooks

def read_csv(file_name):
    # Set the max_colwidth option to prevent text wrapping
    pd.set_option('display.max_colwidth', None)

    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_name, na_values=[''])
    except FileNotFoundError:
        print(f"File {file_name} not found.")
        return None
    except pd.errors.ParserError:
        print(f"Error reading file {file_name}.")
        return None

    # Replace missing values in all columns with empty strings
    df = df.fillna('')

    # Convert DataFrame columns to strings
    df = df.astype(str)

    # Check if DataFrame is empty
    if df.empty:
        print(f"File {file_name} is empty or doesn't contain valid data.")
        return None
    return df

def read_patterns_file(filename):
    try:
        patterns = read_yaml_file(filename)
        if patterns is None:
            return []
        validation_errors = validate_patterns(patterns)
        if validation_errors:
            print("Pattern validation errors:")
            for error in validation_errors:
                print(f"  {error}")
            return []
        return patterns
    except (FileNotFoundError, PermissionError, yaml.YAMLError, IOError) as e:
        print(f"Error occurred when reading patterns file: {e}")
        return []

def read_yaml_file(filename):
    try:
        with open(filename, 'r') as file:
            patterns = yaml.safe_load(file)
        return patterns
    except FileNotFoundError:
        print(f"File {filename} not found.")
    except PermissionError:
        print(f"Permission denied when accessing file {filename}.")
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file {filename}: {e}")
    except IOError as e:
        print(f"IOError when reading file {filename}: {e}")
    except Exception as e:
        print(f"Unexpected error occurred when reading file {filename}: {e}")
    return None


def validate_patterns(patterns):
    validation_errors = []
    compiled_patterns = {}

    for i, pattern in enumerate(patterns):
        if 'column' not in pattern or 'patterns' not in pattern:
            validation_errors.append(f"Pattern {i+1}: Each pattern must have 'column' and 'patterns' fields.")
            continue

        for column_pattern in pattern.get('patterns', []):
            if not is_valid_column_pattern(column_pattern):
                validation_errors.append(f"Pattern {i+1}: Each column pattern must have 'find', 'replace', and 'type' fields.")
                continue

            if not compile_regex_patterns(column_pattern.get('find', []), compiled_patterns, i, pattern):
                validation_errors.append(f"Pattern {i+1}, Column '{pattern['column']}': Invalid regular expression.")

    return validation_errors


def is_valid_column_pattern(column_pattern):
    return all(field in column_pattern for field in ['find', 'replace', 'type'])


def compile_regex_patterns(find_patterns, compiled_patterns, pattern_index, pattern):
    for find_pattern in find_patterns:
        pattern_type = pattern.get('type')
        if pattern_type == 'regex' and find_pattern not in compiled_patterns:
            try:
                compiled_pattern = re.compile(find_pattern)
                compiled_patterns[find_pattern] = compiled_pattern
            except re.error as e:
                print(f"Error compiling pattern: {find_pattern}")
                print(f"Pattern: {pattern}")
                print(f"Pattern Index: {pattern_index}")
                print(f"Error: {e}")
                return False
    return True

def apply_substitution_pattern(column_data, find, replace):
    # Ensure that the find and replace patterns are lists
    if not isinstance(find, list):
        find = [find]
    if not isinstance(replace, list):
        replace = [replace]
    column_data = column_data.astype(str)  # Convert column to string type
    
    return column_data.apply(
        lambda x: ''  # Replace with an empty string if the value matches the find pattern
        if x.strip() == find[0]  # Check if the value stripped of leading/trailing whitespace matches the find pattern
        else x  # Otherwise, keep the original value
    )

def apply_wildcard_pattern(column_data, find, replace):
    # Validate and sanitize the find and replace values
    find = sanitize_input(find)
    replace = sanitize_input(replace)

    # Apply the wildcard pattern using a lambda function
    return column_data.apply(
        lambda x: replace
        if any(re.search(f.replace('*', '.*'), x, flags=re.IGNORECASE) for f in find)
        else x
    )

def sanitize_input(input_value):
    if isinstance(input_value, list):
        # Handle lists of values
        return [re.sub(f'[{re.escape(string.punctuation)}]', '', value) for value in input_value]
    
    # Handle individual values
    return re.sub(f'[{re.escape(string.punctuation)}]', '', input_value)


def apply_regex_pattern(column_data, find, replace):
    # Compile the regex pattern outside the loop
    compiled_patterns = [re.compile(pattern, flags=re.IGNORECASE) for pattern in find]

    # Apply the regex pattern using a lambda function
    for compiled_pattern in compiled_patterns:
        try:
            column_data = column_data.apply(
                lambda x, pattern=compiled_pattern: re.sub(
                    pattern,  # Regular expression pattern to search for
                    lambda match: replace.format(
                        text=match.group(1) if match.group(1) else ""
                    ),
                    str(x),  # Original value
                )
            )
        except re.error:
            print(f"Error in regular expression: {compiled_pattern.pattern}")
            continue
    return column_data

def replace_with_patterns(dataframe, patterns):
    # Mapping of pattern types to corresponding replace functions
    pattern_mapping = {
        'substitution': apply_substitution_pattern,
        'wildcard': apply_wildcard_pattern,
        'regex': apply_regex_pattern
    }

    # Apply patterns to each column in the DataFrame
    for pattern in patterns:
        column = pattern['column']
        column_patterns = pattern['patterns']
        
        # Check if the column exists in the DataFrame
        if column in dataframe.columns:
            for replace_pattern in column_patterns:
                find = replace_pattern['find']
                replace = replace_pattern['replace']
                pattern_type = replace_pattern.get('type')

                # Check if the pattern type is supported
                if pattern_type in pattern_mapping:
                    # Retrieve the corresponding replace function
                    replace_function = pattern_mapping[pattern_type]

                    # Apply the replace function to the column
                    dataframe[column] = replace_function(dataframe[column], find, replace)
    
    return dataframe


def main(csv_file, pattern_file):
    df = read_csv(csv_file)
    if df is None:
        return None, None
    patterns = read_patterns_file(pattern_file)
    if not patterns:
        return None, None
    # Apply the patterns to the DataFrame
    df = replace_with_patterns(df, patterns)
    return df, patterns



if __name__ == "__main__":
    df, patterns = main('data.csv', 'patterns.yml')
    if df is not None and patterns is not None:
        # Print the modified DataFrame
        print(df.head(5))

"""
Misc functions for "Project: Capstone Project"
"""

def _simple_validation(df, name, expected_columns, min_no_rows):
    """Performs a simple validation on a DataFrame"""
    
    if bool(set(expected_columns) - set(df.columns)):
         raise ValueError(f"Invalid schema on {name}")

    if df.count() < min_no_rows:
        raise ValueError(f"Suspiciously low number of rows on {name}")

def fix_nan_values_for_numbers(df, numeric_columns):
    """
    Replace NaN values in specified numeric columns with 0.

    Parameters:
    df (pandas.DataFrame): The input DataFrame.
    numeric_columns (list): List of column names to check for NaN values.

    Returns:
    pandas.DataFrame: DataFrame with NaN values replaced by 0 in specified columns.
    """
    for column in numeric_columns:
        if column in df.columns:
            df[column] = df[column].fillna(0.0)
    return df

def fix_nan_value_for_season(df, period_column):
    """
    Replace NaN values in the specified period column based on 'Hooaeg' column.

    Parameters:
    df (pandas.DataFrame): The input DataFrame.
    period_column (str): The name of the period column.

    Returns:
    pandas.DataFrame: DataFrame with NaN values replaced in the period column.
    """
    if period_column in df.columns and 'Hooaeg' in df.columns:
        # Create a boolean mask for rows where 'Hooaeg' is 'Ei ole hooajaline'
        seasonal_mask = df['Hooaeg'] == 'Ei ole hooajaline'

        # Fill NaN values in the period_column based on the mask
        df.loc[seasonal_mask, period_column] = df.loc[seasonal_mask, period_column].fillna('1 Jaanuar - 31 Detsember')
        df.loc[~seasonal_mask, period_column] = df.loc[~seasonal_mask, period_column].fillna('')

    return df

def fix_opening_times(df):
  opening_colum = 'Avatud ajad'
  if opening_colum in df.columns and 'Hooaeg' in df.columns:
      # Create a boolean mask for rows where 'Lahtiolekuaeg' is 'Ööpäevaringselt'
      opening_mask = df['Lahtiolekuaeg'] == 'Ööpäevaringselt'

      # Fill NaN values in the period_column based on the mask
      df.loc[opening_mask, opening_colum] = df.loc[opening_mask, opening_colum].fillna('00:00-24:00')
      df.loc[~opening_mask, opening_colum] = df.loc[~opening_mask, opening_colum].fillna('')

  return df

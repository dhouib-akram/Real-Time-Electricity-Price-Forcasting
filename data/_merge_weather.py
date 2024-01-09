import pandas as pd
import numpy as np

df_weather = pd.read_csv('./data/weather_features.csv', parse_dates=["dt_iso"])
df_weather.set_index('dt_iso', inplace=True)
# df_weather = df_weather.drop(['weather_main', 'weather_id',
#                               'weather_description', 'weather_icon'], axis=1)
# df_weather.loc[df_weather.pressure > 1051, 'pressure'] = np.nan
# df_weather.loc[df_weather.pressure < 931, 'pressure'] = np.nan
# df_weather.loc[df_weather.wind_speed > 50, 'wind_speed'] = np.nan
df_weather['city_name'] = df_weather['city_name'].str.replace(' ', '')


df_1, df_2, df_3, df_4, df_5 = [x for _, x in df_weather.groupby('city_name')]
dfs = [df_1, df_2, df_3, df_4, df_5]

df_final = dfs[0].add_suffix('_{}'.format(df_1['city_name'].iloc[0]))

for df in dfs[1:]:
    city = df['city_name'].iloc[0]  # Assuming there's only one unique city in each DataFrame
   
    # Remove spaces from 'city_name'
    city = city.replace(' ', '')

    df = df.add_suffix('_{}'.format(city))
    df_final = df_final.merge(df, left_index=True, right_index=True, how='outer')

# Construct the city_str and drop the 'city_name' column for all datasets
for df in dfs:
    city_str = str(df['city_name'].iloc[0]).replace("'", "").replace('[', '').replace(']', '').replace(' ', '')
   
    # Remove spaces from 'city_name'
    city_str = city_str.replace(' ', '')
   
    # Check if the column exists before dropping
    column_to_drop = 'city_name_{}'.format(city_str)
    if column_to_drop in df_final.columns:
        df_final = df_final.drop(column_to_drop, axis=1)

# Drop duplicate columns (columns with the same name)
df_final = df_final.loc[:, ~df_final.columns.duplicated()]

df_final.to_csv('./data/weather_data.csv')
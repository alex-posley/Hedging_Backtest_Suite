def find_closest_earlier(row, rowCol, lookup_df, targetCol, lookupCol):
    earlier_times = lookup_df[lookup_df[lookupCol] <= row[rowCol]]
    if not earlier_times.empty:
        closest_time = earlier_times[lookupCol].max()
        return earlier_times.loc[earlier_times[lookupCol] == closest_time, targetCol].values[0]
    return None

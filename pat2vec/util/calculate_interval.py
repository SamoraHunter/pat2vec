def calculate_interval(start_date, time_delta, m=1):
    # adjust for time interval width
    end_date = start_date + time_delta
    interval_days = (end_date - start_date).days

    n_intervals = interval_days // m
    return n_intervals

import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    months = [10 , 11, 12]

    df = pd.DataFrame()

    for month in months:
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz"
        

        taxi_dtypes = {
            'VendorID': pd.Int64Dtype(), 
            'store_and_fwd_flag':str, 
            'RatecodeID':pd.Int64Dtype(), 
            'PULocationID':pd.Int64Dtype(), 
            'DOLocationID':pd.Int64Dtype(), 
            'passenger_count':pd.Int64Dtype(), 
            'trip_distance':float, 
            'fare_amount':float, 
            'extra':float, 
            'mta_tax':float, 
            'tip_amount':float, 
            'tolls_amount':float, 
            'ehail_fee':float, 
            'improvement_surcharge':float, 
            'total_amount':float, 
            'payment_type':pd.Int64Dtype(), 
            'trip_type':float, 
            'congestion_surcharge':float
        }

        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        month_df = pd.read_csv(url, 
                               sep=',', 
                               compression='gzip',
                               dtype=taxi_dtypes,
                               parse_dates=parse_dates)

        df = pd.concat([df, month_df])

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'



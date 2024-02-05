import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_datetime']).dt.date

    data.rename({"VendorID":"vendor_id", 
                 "RatecodeID":"ratecode_id",
                 "PULocationID":"pu_location_id",
                 "DOLocationID":"do_location_id"},
                 axis=1,
                 inplace=True)

    return data

# @transformer
# def transform_df(data, *args, **kwargs):
#     """
#     Extract unique values of the vendor_id column.
#     """
#     unique_vendor_ids = data['vendor_id'].unique()
#     return unique_vendor_ids


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert len(output[output["vendor_id"].isin([1,2])==False])==0 , 'The vendor_id column has a value of either 1 or 2'
    assert output['passenger_count'].isin([0]).sum() == 0, 'Each passenger count is greater than 0'
    assert output['trip_distance'].isin([0]).sum() == 0, 'Each trip distance is greater than 0'

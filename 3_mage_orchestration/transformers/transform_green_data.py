if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    print("Preprocessing: rows with 0 passengers:", data['passenger_count'].isin([0]).sum())
    print("Preprocessing: rows with 0 trip distance:", data['trip_distance'].isin([0]).sum())
    data = data[data['passenger_count'] > 0]
    data = data[data['trip_distance'] > 0]    

    for col in data.columns:
        col_name = col[0].lower()
        for idx, c in enumerate(col[1:]):
            if c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ') and col[idx].isupper():
                col_name = ''.join([col_name, c.lower()])
            elif c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
                col_name = '_'.join([col_name, c.lower()])
            else:
                col_name = ''.join([col_name, c])
        
        data.rename(columns={col: col_name}, inplace=True)
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    return data


@test
def test_output(output, *args):
    
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
    assert 'vendor_id' in output.columns, 'vendor_id column does not exist'

import pandas

import beetsplug.rkbeets as rkb

def test_default_ddb():
    """Make sure it is roughly functional without inspecting it too much."""
    # I don't understand why pytest can't see this resource.
    ddb = rkb.DimensionsDB('src/beetsplug/rkbeets-fields.csv')

    for row in ddb.get_beets_cols():
        assert pandas.Series(data=None, dtype=row.dtype) is not None

    for row in ddb.get_rkb_cols():
        assert pandas.Series(data=None, dtype=row.dtype) is not None

    ei = ddb.get_export_conversion_info()
    assert ei is not None
    assert ei.drop_fields is not None
    assert ei.export_fields is not None

    for ef in ei.export_fields:
        assert ef.beets is not None
        assert ef.rkb is not None

    count = 0
    for pair in ddb.get_sync_pairs():
        count += 1
        assert pair.beets is not None
        assert pair.rkb is not None
    assert count > 0
    
def test_ddb_cols():
    ddb = rkb.DimensionsDB('test/ddb1.csv')
    assert ddb.num_beets_cols() == 18
    assert ddb.num_rkb_cols() == 18

    for row in ddb.get_beets_cols():
        assert row.field.startswith('b')
        assert pandas.Series(data=None, dtype=row.dtype) is not None

    for row in ddb.get_rkb_cols():
        assert row.field.startswith('r')
        assert pandas.Series(data=None, dtype=row.dtype) is not None

def test_ddb_export():
    ddb = rkb.DimensionsDB('test/ddb1.csv')

    ei = ddb.get_export_conversion_info()
    assert list(ei._asdict().keys()) == ['drop_fields', 'export_fields']
    assert ei.drop_fields == ['b90', 'b92']
    for ef in ei.export_fields:
        assert list(ef._asdict().keys()) == ['beets', 'rkb', 'func']
        assert ef.func is None
        assert ef.beets[1:] == ef.rkb[1:]

def test_ddb_sync():
    ddb = rkb.DimensionsDB('test/ddb1.csv')

    count = 0
    for pair in ddb.get_sync_pairs():
        assert list(pair._asdict().keys()) == ['beets', 'rkb']
        count += 1
        assert pair.beets[1:] == pair.rkb[1:]
    assert count == 5

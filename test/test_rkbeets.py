import beetsplug.rkbeets as rkb

# just making sure tests work...
def test_convert_format():
    value = 'AAC'
    assert rkb.format_to_kind(value) == 'M4A File'

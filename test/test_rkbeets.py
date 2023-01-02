import beetsplug.rkbeets as rkb

def test_get_samplerate_none():
    item = {
        'samplerate': None
    }
    assert rkb.get_samplerate(item) == None

def test_get_samplerate_44():
    item = {
        'samplerate': 44
    }
    assert rkb.get_samplerate(item) == 44000.0

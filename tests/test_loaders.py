def test_load_all_publishers(schema_loader):
    pubs = schema_loader.get_all_publishers()
    assert pubs == {
        "GLEBZ": {
            "id": "GLEBZ",
            "name": "Datateam Glebz",
            "shortname": "braft",
            "tags": {"costcenter": "12345.6789"},
        },
        "HARRY": {
            "id": "HARRY",
            "name": "Datateam Harry",
            "shortname": "harhar",
            "tags": {"costcenter": "123456789.4321.13519", "team": "taggy"},
        },
        "NOTTHESAMEASFILENAME": {
            "id": "NOTTHESAMEASFILENAME",
            "name": "Datateam incorrect",
            "shortname": "nono",
            "tags": {"costcenter": "1236789.4321.13519", "team": "taggy"},
        },
    }

import config


def test_aws_config_has_bucket():
    cfg = config.load("aws")
    assert cfg["bucket"] == "vn-rental-dsp"
    assert cfg["region"] == "ap-southeast-1"


def test_sources_config_lists_four_sources():
    cfg = config.load("sources")
    assert set(cfg["sources"]) == {"batdongsan", "phongtro123", "mogi", "nhatot"}

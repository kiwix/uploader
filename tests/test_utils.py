from kiwix_uploader.utils import parse_url


def test_parse_url():
    print("yolo")
    pu = parse_url(
        "s3://s3.us-west-1.wasabisys.com/?bucketName=org-kiwix-hotspot-cardshop-warehouse"
    )
    assert pu.scheme == "s3"

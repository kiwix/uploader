from kiwix_uploader.context import Context

Context.setup()

from kiwix_uploader.upload import upload_file, watched_upload, check_and_upload_file  # noqa: F401, E402

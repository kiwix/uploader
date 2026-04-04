from kiwix_uploader.context import Context

Context.setup()

from kiwix_uploader.api import (  # noqa: F401, E402
    check_and_upload_file,
    multi_file_upload,
    upload_file,
    watched_upload,
)

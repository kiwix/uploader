import datetime
import urllib.parse
from pathlib import Path

from kiwixstorage import FileTransferHook, KiwixStorage

from kiwix_uploader.context import Context
from kiwix_uploader.utils import display_stats, now, rebuild_uri, remove_source_file

context = Context.get()
logger = context.logger


def get_url_scheme(url: urllib.parse.ParseResult) -> str:
    if url.scheme.startswith("s3+http"):
        return "http"
    # covers both "s3" and "s3+https"
    elif url.scheme.startswith("s3") or url.scheme.startswith("s3+https"):
        return "https"
    else:
        raise ValueError(f"Unsupported URL scheme in: {url}")


def s3_upload_file(
    src_path: Path,
    upload_url: str,
    filesize: int,
    delete: bool = context.delete,
    delete_after: int = context.delete_after,  # nb of days to mark file for deletion (marker file only)
    wasabi_delete_after: int = context.wasabi_delete_after,  # nb of days to expire upload file after
) -> int:
    started_on = now()
    upload_uri = urllib.parse.urlparse(upload_url)
    s3_storage = KiwixStorage(
        str(rebuild_uri(upload_uri, scheme=get_url_scheme(upload_uri)).geturl())
    )
    logger.debug(f"S3 initialized for {s3_storage.url.netloc}/{s3_storage.bucket_name}")

    key = upload_uri.path[1:]
    if upload_uri.path.endswith("/"):
        key += src_path.name

    try:
        logger.info(f"Uploading to {key}")
        hook = FileTransferHook(filename=src_path)
        s3_storage.upload_file(fpath=src_path, key=key, Callback=hook)
        print("", flush=True)
    except Exception as exc:
        # as there is no resume, uploading to existing URL will result in DELETE+UPLOAD
        # if credentials doesn't allow DELETE or if there is an unsatisfied
        # retention, will raise PermissionError
        logger.error(f"uploader failed: {exc}")
        logger.exception(exc)
        return 1
    ended_on = now()
    logger.info("uploader ran successfuly.")

    # setting autodelete
    if delete_after > 0:
        try:
            # set expiration after bucket's min retention.
            # bucket retention is 1d minumum.
            # can be configured to loger value.
            # if expiration before bucket min retention, raises 400 Bad Request
            # on compliance
            expire_on = (
                datetime.datetime.now()
                + datetime.timedelta(days=max(delete_after, 0) or 1)
                # adding 1mn to prevent clash with bucket's equivalent min retention
                + datetime.timedelta(seconds=60)
            )
            logger.info(f"Setting autodelete to {expire_on}")
            s3_storage.set_object_autodelete_on(key=key, on=expire_on)
        except Exception as exc:
            logger.error(f"Failed to set autodelete: {exc}")
            logger.exception(exc)

    if delete:
        remove_source_file(src_path)
    display_stats(filesize, started_on, ended_on)

    return 0


def s3_remove_file(upload_url: str, private_key: Path | None = None) -> int:
    upload_uri = urllib.parse.urlparse(upload_url)
    s3_storage = KiwixStorage(
        str(rebuild_uri(upload_uri, scheme=get_url_scheme(upload_uri)).geturl())
    )
    logger.debug(f"S3 initialized for {s3_storage.url.netloc}/{s3_storage.bucket_name}")

    key = upload_uri.path[1:]

    try:
        logger.info(f"Removing {key}")
        s3_storage.delete_object(key=key)
    except Exception as exc:
        # if credentials doesn't allow DELETE or if there is an unsatisfied
        # retention, will raise PermissionError
        logger.error(f"uploader failed: {exc}")
        logger.exception(exc)
        return 1
    logger.info("uploader ran successfuly.")
    return 0

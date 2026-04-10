import os
import shutil
import tempfile
import time
import urllib.parse
from pathlib import Path
from typing import Callable

import humanfriendly

from kiwix_uploader.context import Context
from kiwix_uploader.s3 import s3_remove_file, s3_upload_file
from kiwix_uploader.scp import scp_remove_file, scp_upload_file
from kiwix_uploader.sftp import sftp_remove_file, sftp_upload_file
from kiwix_uploader.upload import UploadResults, UploadsManager
from kiwix_uploader.utils import (
    ack_host_fingerprint,
    get_expiration_for,
    rebuild_uri,
    remove_source_file,
    watched_upload,
)

context = Context.get()
logger = context.logger
marker_suffix = ".delete_on"


def _get_upload_method(scheme: str) -> Callable | None:
    return {
        "scp": scp_upload_file,
        "sftp": sftp_upload_file,
        "s3": s3_upload_file,
        "s3+http": s3_upload_file,
        "s3+https": s3_upload_file,
    }.get(scheme)


def _get_remove_method(scheme: str) -> Callable | None:
    return {
        "scp": scp_remove_file,
        "sftp": sftp_remove_file,
        "s3": s3_remove_file,
        "s3+http": s3_remove_file,
        "s3+https": s3_remove_file,
    }.get(scheme)


def upload_file(
    src_path: Path,
    upload_url: str,
    private_key: Path | None = None,
    username: str = context.username,
    resume: bool = context.resume,
    watch_for: str = context.watch_for,
    move: bool = context.move,
    delete: bool = context.delete,
    compress: bool = context.compress,
    bandwidth: int | None = context.bandwidth,
    cipher: str | None = context.cipher,
    delete_after: int = context.delete_after,
    wasabi_delete_after: int = context.wasabi_delete_after,
) -> int:
    """Uploads single file to single destination. 0 upon success"""
    try:
        upload_uri = urllib.parse.urlparse(upload_url)
        Path(upload_uri.path)
    except Exception as exc:
        logger.error(f"invalid upload URI: `{upload_uri}` ({exc}).")
        return 1

    # set username in URI if provided and URI has none
    if upload_uri.scheme in ("scp", "sftp") and username and not upload_uri.username:
        upload_uri = rebuild_uri(upload_uri, username=username)

    if upload_uri.scheme in context.s3_schemes and upload_uri.query:
        params = urllib.parse.parse_qs(str(upload_uri.query))
        if "secretAccessKey" in params.keys():
            params["secretAccessKey"] = ["xxxxx"]
        safe_upload_uri = rebuild_uri(
            upload_uri, query=urllib.parse.urlencode(params, doseq=True)
        ).geturl()
    else:
        safe_upload_uri = upload_uri.geturl()

    logger.info(f"Starting upload of {src_path} to {safe_upload_uri}")

    method = _get_upload_method(str(upload_uri.scheme))
    if not method:
        logger.critical(f"URI scheme not supported: {upload_uri.scheme}")
        return 1

    if upload_uri.scheme in ("scp",) + context.s3_schemes and resume:
        logger.warning("--resume not supported via SCP/S3. Will upload from scratch.")

    if upload_uri.scheme not in context.s3_schemes and wasabi_delete_after > 0:
        logger.warning("--wasabi-delete-after only supported on S3/Wasabi.")

    try:
        filesize = src_path.stat().st_size
    except Exception as exc:
        logger.critical(f"Unable to retrieve file size: {exc!s}")
        return 1

    kwargs = {
        "src_path": src_path,
        "upload_url": upload_url,
        "filesize": filesize,
        "private_key": private_key,
        "resume": resume,
        "move": move,
        "delete": delete,
        "compress": compress,
        "bandwidth": bandwidth,
        "cipher": cipher,
        "delete_after": delete_after,
        "wasabi_delete_after": wasabi_delete_after,
    }

    # upload marker first
    if delete_after > 0:
        rc = _upload_marker_file(
            upload_url=rebuild_uri(
                upload_uri, path=f"{upload_uri.path}{src_path.name}"
            ).geturl(),
            private_key=private_key,
            username=username,
            delete_after=delete_after,
        )
        if rc != 0:
            return rc
        logger.debug("> marker file uploaded")

    if watch_for:
        try:
            watch = int(humanfriendly.parse_timespan(watch_for))
        except Exception as exc:
            logger.critical(f"--watch delay ({watch_for}) not correct: {exc}")
            return 1
        return watched_upload(watch, method, **kwargs)
    return method(**kwargs)


def check_and_upload_file(
    src_path: Path,
    upload_urls: list[str],
    private_key: Path | None = None,
    username: str = context.username,
    resume: bool = context.resume,
    watch_for: str = context.watch_for,
    move: bool = context.move,
    delete: bool = context.delete,
    compress: bool = context.compress,
    bandwidth: int | None = context.bandwidth,
    cipher: str | None = context.cipher,
    delete_after: int = context.delete_after,
    wasabi_delete_after: int = context.wasabi_delete_after,
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
) -> UploadResults:
    """Checks inputs and uploads single file to multiple destinations"""

    # fail early if source file is not readable
    src_path = Path(src_path).expanduser().resolve()
    if (
        not src_path.exists()
        or not src_path.is_file()
        or not os.access(src_path, os.R_OK)
    ):
        logger.error(f"source file ({src_path}) doesn't exist or is not readable.")
        return UploadResults.failure(1)

    # make sure we're not getting a single string in upload_urls
    if isinstance(upload_urls, str):
        raise ValueError("`upload_urls` should be an iterable but not a string")

    # trim-off any empty url
    upload_urls = [url.strip() for url in upload_urls]

    # ensure we dont have duplicates
    if len(set(upload_urls)) != len(upload_urls):
        raise ValueError("Upload URLs contains duplicates")

    for upload_url in upload_urls:
        # make sur upload-uri is correct (trailing slash)
        try:
            upload_uri = urllib.parse.urlparse(upload_url)
            if not upload_uri.scheme or not upload_uri.netloc:
                raise ValueError("missing URL component")
        except Exception as exc:
            logger.error(f"invalid upload URI: `{upload_url}` ({exc}).")
            return UploadResults.failure(1)
        else:
            if not upload_uri.path.endswith("/") and not Path(upload_uri.path).suffix:
                logger.error(
                    f"/!\\ your upload_url doesn't end with a slash "
                    f"and has no file extension: `{upload_url}`."
                )
                return UploadResults.failure(1)

        if upload_uri.scheme in ("scp", "sftp"):
            if private_key is None:
                raise IOError("Missing private_key for scp/sftp")
            # fail early if private key is not readable
            private_key = Path(private_key).expanduser().resolve()
            if (
                not private_key.exists()
                or not private_key.is_file()
                or not os.access(private_key, os.R_OK)
            ):
                logger.error(
                    f"private RSA key file ({private_key}) doesn't exist "
                    f"or is not readable."
                )
                return UploadResults.failure(1)

            ack_host_fingerprint(upload_uri.hostname, upload_uri.port)

    return multi_file_upload(
        src_path=src_path,
        upload_urls=upload_urls,
        private_key=private_key,
        username=username,
        resume=resume,
        watch_for=watch_for,
        move=move,
        delete=delete,
        compress=compress,
        bandwidth=bandwidth,
        cipher=cipher,
        delete_after=delete_after,
        wasabi_delete_after=wasabi_delete_after,
        attempts=attempts,
        attempts_delay=attempts_delay,
    )


def upload_file_retrying(
    src_path: Path,
    upload_url: str,
    private_key: Path | None = None,
    username: str = context.username,
    resume: bool = context.resume,
    watch_for: str = context.watch_for,
    move: bool = context.move,
    delete: bool = context.delete,
    compress: bool = context.compress,
    bandwidth: int | None = context.bandwidth,
    cipher: str | None = context.cipher,
    delete_after: int = context.delete_after,
    wasabi_delete_after: int = context.wasabi_delete_after,
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
) -> int:
    """Uploads single file to single destination, retrying upon errors: 0 upon success"""
    attempts = attempts or 1
    attempts_delay = attempts_delay or 0
    rc = 1

    while attempts and rc != 0:
        attempts -= 1
        rc = upload_file(
            src_path=src_path,
            upload_url=upload_url,
            private_key=private_key,
            username=username,
            resume=resume,
            watch_for=watch_for,
            move=move,
            delete=delete,
            compress=compress,
            bandwidth=bandwidth,
            cipher=cipher,
            delete_after=delete_after,
            wasabi_delete_after=wasabi_delete_after,
        )
        if rc != 0:
            if not attempts:
                return rc
            logger.warning(f"Upload failed: {attempts} attempts remaining.")
            if attempts_delay:
                logger.info(f"Pausing for {attempts_delay}s")
                time.sleep(attempts_delay)
            continue
    return rc


def multi_file_upload(
    src_path: Path,
    upload_urls: list[str],
    private_key: Path | None = None,
    username: str = context.username,
    resume: bool = context.resume,
    watch_for: str = context.watch_for,
    move: bool = context.move,
    delete: bool = context.delete,
    compress: bool = context.compress,
    bandwidth: int | None = context.bandwidth,
    cipher: str | None = context.cipher,
    delete_after: int = context.delete_after,
    wasabi_delete_after: int = context.wasabi_delete_after,
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
) -> UploadResults:
    """Uploads single file multiple destinations"""

    # at the moment, we only support parallel uploads
    manager = UploadsManager(
        target=upload_file_retrying,
        src_path=src_path,
        upload_urls=upload_urls,
        private_key=private_key,
        username=username,
        resume=resume,
        watch_for=watch_for,
        move=move,
        delete=False,  # cant delete there if mutliple uploads
        compress=compress,
        bandwidth=bandwidth,
        cipher=cipher,
        delete_after=delete_after,
        wasabi_delete_after=wasabi_delete_after,
        attempts=attempts,
        attempts_delay=attempts_delay,
    )
    manager.start()
    manager.wait()

    if manager.succeeded and delete:
        remove_source_file(src_path)

    return manager.results


def remove_file(
    upload_url: str, private_key: Path | None = None, username: str = context.username
) -> int:
    """Removes single file from single location. 0 upon success"""
    try:
        upload_uri = urllib.parse.urlparse(upload_url)
        Path(upload_uri.path)
    except Exception as exc:
        logger.error(f"invalid upload URL: `{upload_uri}` ({exc}).")
        return 1

    # set username in URI if provided and URI has none
    if upload_uri.scheme in ("scp", "sftp") and username and not upload_uri.username:
        upload_uri = rebuild_uri(upload_uri, username=username)

    if upload_uri.scheme in context.s3_schemes and upload_uri.query:
        params = urllib.parse.parse_qs(str(upload_uri.query))
        if "secretAccessKey" in params.keys():
            params["secretAccessKey"] = ["xxxxx"]
        safe_upload_uri = rebuild_uri(
            upload_uri, query=urllib.parse.urlencode(params, doseq=True), path=""
        ).geturl()
    else:
        safe_upload_uri = upload_uri.geturl()

    logger.info(
        f"Starting removal of {Path(upload_uri.path).name} from {safe_upload_uri}"
    )

    method = _get_remove_method(str(upload_uri.scheme))
    if not method:
        logger.critical(f"URL scheme not supported: {upload_uri.scheme}")
        return 1

    return method(upload_url=upload_uri.geturl(), private_key=private_key)


def remove_file_retrying(
    upload_url: str,
    private_key: Path | None = None,
    username: str = context.username,
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
) -> int:
    """Removes single file from single destination, retrying upon errors: 0 upon success"""
    logger.info("remove_file_retrying")
    attempts = attempts or 1
    attempts_delay = attempts_delay or 0
    rc = 1

    while attempts and rc != 0:
        attempts -= 1
        rc = remove_file(
            upload_url=upload_url, private_key=private_key, username=username
        )
        if rc != 0:
            if not attempts:
                return rc
            logger.warning(f"Removal failed: {attempts} attempts remaining.")
            if attempts_delay:
                logger.info(f"Pausing for {attempts_delay}s")
                time.sleep(attempts_delay)
            continue
    return rc


def _upload_marker_file(
    upload_url: str,
    delete_after: int,
    private_key: Path | None = None,
    username: str = context.username,
) -> int:
    """Actual upload of a marker file for single destination file. 0 upon success"""
    upload_uri = urllib.parse.urlparse(upload_url)

    # store marker on disk
    marker_dir = Path(
        tempfile.TemporaryDirectory(
            prefix="uploader_",
            suffix=".marker",
            ignore_cleanup_errors=True,
            delete=False,
        ).name
    )
    fname = Path(upload_uri.path).name
    marker_fpath = marker_dir.joinpath(f"{fname}{marker_suffix}")
    marker_fpath.write_text(get_expiration_for(delete_after).isoformat())
    try:
        marker_filesize = marker_fpath.stat().st_size
    except Exception as exc:
        logger.critical(f"Unable to retrieve marker file size: {exc!s}")
        return 1
    logger.info(marker_fpath.read_text())
    logger.debug(f"Uploading marker file {marker_fpath.name}…")
    method = _get_upload_method(str(upload_uri.scheme))
    if not method:
        logger.critical(f"URI scheme not supported: {upload_uri.scheme}")
        return 1
    rc = method(
        upload_url=rebuild_uri(
            upload_uri, path=str(Path(upload_uri.path).parent)
        ).geturl(),
        private_key=private_key,
        username=username,
        src_path=marker_fpath,
        filesize=marker_filesize,
        resume=False,
    )
    shutil.rmtree(marker_dir, ignore_errors=True)
    return rc


def update_marker(
    upload_url: str,
    private_key: Path | None = None,
    username: str = context.username,
    delete_after: int = context.delete_after,
) -> int:
    """Sets/updates delete marker file for a single destination file. 0 upon success"""
    try:
        upload_uri = urllib.parse.urlparse(upload_url)
        Path(upload_uri.path)
    except Exception as exc:
        logger.error(f"invalid upload URL: `{upload_uri}` ({exc}).")
        return 1

    # set username in URI if provided and URI has none
    if upload_uri.scheme in ("scp", "sftp") and username and not upload_uri.username:
        upload_uri = rebuild_uri(upload_uri, username=username)

    if upload_uri.scheme in context.s3_schemes and upload_uri.query:
        params = urllib.parse.parse_qs(str(upload_uri.query))
        if "secretAccessKey" in params.keys():
            params["secretAccessKey"] = ["xxxxx"]
        safe_upload_uri = rebuild_uri(
            upload_uri, query=urllib.parse.urlencode(params, doseq=True), path=""
        ).geturl()
    else:
        safe_upload_uri = upload_uri.geturl()

    logger.info(
        f"Updating marker of {Path(upload_uri.path).name} from {safe_upload_uri}"
    )

    # setting or updating expiration
    if delete_after > 0:
        return _upload_marker_file(
            upload_url=upload_uri.geturl(),
            private_key=private_key,
            username=username,
            delete_after=delete_after,
        )
    # removing expiration
    else:
        rpath = Path(upload_uri.path)
        return remove_file(
            upload_url=rebuild_uri(
                upload_uri, path=str(rpath.with_name(f"{rpath.name}{marker_suffix}"))
            ).geturl(),
            private_key=private_key,
            username=username,
        )


def set_marker_retrying(
    upload_url: str,
    private_key: Path | None = None,
    username: str = context.username,
    delete_after: int = context.delete_after,
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
) -> int:
    """Sets marker file for single destination, retrying on errors. 0 upon success"""
    attempts = attempts or 1
    attempts_delay = attempts_delay or 0
    rc = 1

    while attempts and rc != 0:
        attempts -= 1
        rc = update_marker(
            upload_url=upload_url,
            private_key=private_key,
            username=username,
            delete_after=delete_after,
        )
        if rc != 0:
            if not attempts:
                return rc
            logger.warning(f"Removal failed: {attempts} attempts remaining.")
            if attempts_delay:
                logger.info(f"Pausing for {attempts_delay}s")
                time.sleep(attempts_delay)
            continue
    return rc

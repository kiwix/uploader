import os
import time
import urllib.parse
from pathlib import Path

from kiwix_uploader.context import Context, humanfriendly
from kiwix_uploader.s3 import s3_upload_file
from kiwix_uploader.scp import scp_upload_file
from kiwix_uploader.sftp import sftp_upload_file
from kiwix_uploader.upload import UploadResults, UploadsManager
from kiwix_uploader.utils import (
    ack_host_fingerprint,
    rebuild_uri,
    watched_upload, remove_source_file,
)

context = Context.get()
logger = context.logger


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
):
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

    method = {
        "scp": scp_upload_file,
        "sftp": sftp_upload_file,
        "s3": s3_upload_file,
        "s3+http": s3_upload_file,
        "s3+https": s3_upload_file,
    }.get(str(upload_uri.scheme))

    if not method:
        logger.critical(f"URI scheme not supported: {upload_uri.scheme}")
        return 1

    if upload_uri.scheme in ("scp",) + context.s3_schemes and resume:
        logger.warning("--resume not supported via SCP/S3. Will upload from scratch.")

    if upload_uri.scheme not in context.s3_schemes and delete_after > 0:
        logger.warning("--delete-after only supported on S3/Wasabi.")

    kwargs = {
        "src_path": src_path,
        "upload_url": upload_url,
        "filesize": src_path.stat().st_size,
        "private_key": private_key,
        "resume": resume,
        "move": move,
        "delete": delete,
        "compress": compress,
        "bandwidth": bandwidth,
        "cipher": cipher,
        "delete_after": delete_after,
    }

    if watch_for:
        try:
            # without humanfriendly, watch is considered to be in seconds
            watch = int(
                humanfriendly.parse_timespan(watch_for) if humanfriendly else watch_for
            )
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
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
) -> UploadResults:
    """checks inputs and uploads file, returning 0 on success"""

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
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
):
    attempts = attempts or 1
    attempts_delay = attempts_delay or 0
    rc = None

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
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
) -> UploadResults:
    """ Upload a single file to one or more destinations"""

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
        attempts=attempts,
        attempts_delay=attempts_delay,
    )
    manager.start()
    manager.wait()

    if delete:
        remove_source_file(src_path)

    return manager.results

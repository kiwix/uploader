import subprocess
from pathlib import Path

from kiwix_uploader.context import Context
from kiwix_uploader.utils import (
    display_stats,
    get_batch_file,
    now,
    parse_url,
    rebuild_uri,
    remove_source_file,
)

context = Context.get()
logger = context.logger


def sftp_remote_filesize(private_key, sftp_uri, fname) -> int:
    args = [
        str(context.sftp_bin_path),
        "-i",
        str(private_key),
        "-b",
        get_batch_file([f"ls -n {fname}"]),
        "-o",
        f"GlobalKnownHostsFile={context.host_know_file}",
        sftp_uri.geturl(),
    ]

    logger.info("Executing: {args}".format(args=" ".join(args)))

    sftp = subprocess.run(
        args=args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    last_line = sftp.stdout.strip().split("\n")[-1]
    if not last_line.endswith(fname) or last_line.startswith("sftp"):
        return 0

    try:
        remote_size = int(last_line.split()[4])
    except Exception:
        return 0

    return remote_size or 0


def sftp_actual_upload(
    private_key, source_path, sftp_uri, commands, cipher, compress, bandwidth
) -> subprocess.CompletedProcess:
    args = [
        str(context.sftp_bin_path),
        "-i",
        str(private_key),
        "-b",
        get_batch_file(commands),
        "-o",
        f"GlobalKnownHostsFile={context.host_know_file}",
    ]

    if cipher:
        args += ["-c", cipher]

    if compress:
        args += ["-C"]

    if bandwidth > 0:
        args += ["-l", str(bandwidth)]

    args += [sftp_uri.geturl()]

    logger.info("Executing: {args}".format(args=" ".join(args)))

    return subprocess.run(
        args=args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def sftp_upload_file(
    src_path: Path,
    upload_url: str,
    filesize: int,
    private_key: Path | None = None,
    username: str = context.username,
    resume: bool = context.resume,
    watch_for: str = context.watch_for,
    move: bool = context.move,
    delete: bool = context.delete,
    compress: bool = context.compress,
    bandwidth: int = context.bandwidth,
    cipher: str = context.cipher,
    delete_after: int = context.delete_after,
    wasabi_delete_after: int = context.wasabi_delete_after,  # not supported
    attempts: int = context.attempts,
    attempts_delay: int = context.attempts_delay,
) -> int:
    # we need to reconstruct the url but without an ending filename
    upload_uri = parse_url(upload_url)
    if not upload_uri.path.endswith("/"):
        uri_path = Path(upload_uri.path)
        final_fname = uri_path.name
        sftp_uri = rebuild_uri(upload_uri, path=f"{uri_path.parent}/")
    else:
        final_fname = src_path.name
        sftp_uri = upload_uri

    put_cmd = "put"  # default to overwritting
    if resume:
        # check if there's already a matching file on the remte
        existing_size = sftp_remote_filesize(private_key, sftp_uri, final_fname)
        # if source and destination filesizes match, return as sftp would fail
        if existing_size >= filesize:
            logger.info(
                "Nothing to upload "
                "(destination file bigger or same size as source file)"
            )
            return 0
        # there's a different size file on destination, let's overwrite
        if existing_size:
            put_cmd = "reput"  # change to APPEND mode
            filesize = filesize - existing_size  # used for stats

    if move:
        temp_fname = f"{final_fname}.tmp"
        commands = [
            f'{put_cmd} "{src_path}" "{temp_fname}"',
            f'rename "{temp_fname}" "{final_fname}"',
            "bye",
        ]
    else:
        commands = [f'{put_cmd} "{src_path}" "{final_fname}"', "bye"]

    started_on = now()
    sftp = sftp_actual_upload(
        private_key, src_path, sftp_uri, commands, cipher, compress, bandwidth
    )
    ended_on = now()

    if sftp.returncode == 0:
        logger.info("Uploader ran successfuly.")
        display_stats(filesize, started_on, ended_on)
        if delete:
            remove_source_file(src_path)
    else:
        logger.error(f"sftp failed returning {sftp.returncode}:: {sftp.stdout}")

    return sftp.returncode


def sftp_remove_file(upload_url: str, private_key: Path | None = None) -> int:
    upload_uri = parse_url(upload_url)
    if upload_uri.path.endswith("/"):
        raise NotImplementedError("Does not support removing folders")
    sftp_uri = rebuild_uri(upload_uri, path="/").geturl()

    commands = [f'rm "{upload_uri.path!s}"', "bye"]
    args = [
        str(context.sftp_bin_path),
        "-i",
        str(private_key),
        "-b",
        get_batch_file(commands),
        "-o",
        f"GlobalKnownHostsFile={context.host_know_file}",
    ]
    args += [sftp_uri]

    logger.info("Executing: {args}".format(args=" ".join(args)))

    sftp = subprocess.run(
        args=args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if sftp.returncode == 0:
        logger.info("Removal succeeded")
    return sftp.returncode

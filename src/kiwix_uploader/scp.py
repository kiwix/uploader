import subprocess
from pathlib import Path

from kiwix_uploader.context import Context
from kiwix_uploader.utils import (
    display_stats,
    now,
    parse_url,
    rebuild_uri,
    remove_source_file,
)

context = Context.get()
logger = context.logger


def scp_actual_upload(
    private_key, source_path, dest_uri, cipher, compress, bandwidth
) -> subprocess.CompletedProcess:
    """transfer a file via SCP and return subprocess"""

    args = [
        str(context.scp_bin_path),
        "-i",
        str(private_key),
        "-B",  # batch mode
        "-q",  # quiet mode
        "-o",
        f"GlobalKnownHostsFile={context.host_know_file}",
    ]

    if cipher:
        args += ["-c", cipher]

    if compress:
        args += ["-C"]

    if bandwidth > 0:
        args += ["-l", str(bandwidth)]

    args += [str(source_path), dest_uri.geturl()]

    logger.info("Executing: {args}".format(args=" ".join(args)))

    return subprocess.run(
        args=args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )


def scp_upload_file(
    src_path: Path,
    upload_url: str,
    filesize: int,
    private_key: Path | None = None,
    username: str = context.username,
    resume: bool = context.resume,
    move: bool = context.move,
    delete: bool = context.delete,
    compress: bool = context.compress,
    bandwidth: int = context.bandwidth,
    cipher: str = context.cipher,
) -> int:
    # directly uploading final file to final destination
    if not move:
        started_on = now()
        scp = scp_actual_upload(
            private_key, src_path, parse_url(upload_url), cipher, compress, bandwidth
        )
        ended_on = now()

        if scp.returncode == 0:
            logger.info("Uploader ran successfuly.")
            if delete:
                remove_source_file(src_path)
            display_stats(filesize, started_on, ended_on)
        else:
            logger.error(f"scp failed returning {scp.returncode}:: {scp.stdout}")

        return scp.returncode

    # uploading file in two steps
    # - uploading to temporary name
    # - uploading an upload-complete marker aside
    upload_uri = parse_url(upload_url)
    if upload_uri.path.endswith("/"):
        real_fname = src_path.name
        dest_folder = upload_uri.path
    else:
        uri_path = Path(upload_uri.path)
        real_fname = uri_path.name
        dest_folder = f"{uri_path.parent}/"

    temp_fname = f"{real_fname}.tmp"
    dest_path = f"{dest_folder}{temp_fname}"
    marker_dest_path = f"{dest_folder}{real_fname}.complete"

    started_on = now()
    scp = scp_actual_upload(
        private_key,
        src_path,
        rebuild_uri(upload_uri, path=dest_path),
        cipher,
        compress,
        bandwidth,
    )
    ended_on = now()

    if scp.returncode != 0:
        logger.critical(f"scp failed returning {scp.returncode}:: {scp.stdout}")
        return scp.returncode

    logger.info(
        f"[WIP] uploaded to temp file `{temp_fname}` successfuly. "
        f"uploading complete marker..."
    )
    if delete:
        remove_source_file(src_path)

    scp = scp_actual_upload(
        private_key,
        context.marker_file,
        rebuild_uri(parse_url(upload_url), path=marker_dest_path),
        cipher,
        compress,
        bandwidth,
    )

    if scp.returncode == 0:
        logger.info("Uploader ran successfuly.")
    else:
        logger.warning(
            f"scp failed to transfer upload marker "
            f"returning {scp.returncode}:: {scp.stdout}"
        )
        logger.warning(
            "actual file transferred properly though. You'd need to move it manually."
        )
    display_stats(filesize, started_on, ended_on)

    return scp.returncode


def scp_remove_file(upload_url: str, private_key: Path | None = None) -> int:

    upload_uri = parse_url(upload_url)
    if upload_uri.path.endswith("/"):
        raise NotImplementedError("Does not support removing folders")
    ssh_uri = upload_uri.netloc.rsplit(":", 1)[0]
    ssh_port = upload_uri.port or 22

    args = [
        str(context.ssh_bin_path),
        "-i",
        str(private_key),
        "-q",  # quiet mode
        "-o",
        f"GlobalKnownHostsFile={context.host_know_file}",
        "-p",
        str(ssh_port),
        ssh_uri,
    ]

    args += ["rm", "-f", str(upload_uri.path)]

    logger.info("Executing: {args}".format(args=" ".join(args)))

    ssh = subprocess.run(
        args=args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    if ssh.returncode == 0:
        logger.info("Removal succeeded")
    return ssh.returncode

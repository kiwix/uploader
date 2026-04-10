import datetime
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path
from typing import Callable

from kiwix_uploader.context import Context

import humanfriendly

context = Context.get()
logger = context.logger


def now() -> datetime.datetime:
    return datetime.datetime.now()


def ack_host_fingerprint(host, port):
    """run/store ssh-keyscan to prevent need to manually confirm host fingerprint"""
    keyscan = subprocess.run(
        ["/usr/bin/ssh-keyscan", "-p", str(port), host],
        capture_output=True,
        text=True,
    )
    if keyscan.returncode != 0:
        logger.error(f"unable to get remote host ({host}:{port}) public key")
        sys.exit(1)

    with open(context.host_know_file, "w") as keyscan_output:
        keyscan_output.write(keyscan.stdout)
        keyscan_output.seek(0)


def remove_source_file(src_path: Path):
    logger.info("removing source file…")
    try:
        src_path.unlink()
    except Exception as exc:
        logger.error(f":: failed to remove ZIM file: {exc}")
    else:
        logger.info(":: success.")


def parse_url(url: str) -> urllib.parse.ParseResult:
    return urllib.parse.urlparse(url, allow_fragments=False)


def rebuild_uri(
    uri: urllib.parse.ParseResult,
    scheme: str | None = None,
    username: str | None = None,
    password: str | None = None,
    hostname: str | None = None,
    port: int | None = None,
    path: str | None = None,
    params: str | None = None,
    query: str | None = None,
    fragment: str | None = None,
) -> urllib.parse.ParseResult:
    scheme = scheme or uri.scheme
    username = username or uri.username
    password = password or uri.password
    hostname = hostname or uri.hostname
    port = port or uri.port
    path = path or uri.path
    netloc: str = ""
    if username:
        netloc += str(username)
    if password:
        netloc += f":{password}"
    if username or password:
        netloc += "@"
    netloc += str(hostname)
    if port:
        netloc += f":{port}"
    params = params or uri.params
    query = query or uri.query
    fragment = fragment or uri.fragment
    return urllib.parse.urlparse(
        urllib.parse.urlunparse([scheme, netloc, path, fragment, query, fragment])
    )


def get_batch_file(commands: list[str]) -> str:
    command_content = "\n".join(commands)
    logger.debug(f"SFTP commands:\n{command_content}---")
    batch_file = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    batch_file.write(command_content)
    batch_file.close()
    return batch_file.name


def display_stats(
    filesize: int,
    started_on: datetime.datetime,
    ended_on: datetime.datetime | None = None,
):
    ended_on = ended_on or now()
    duration = (ended_on - started_on).total_seconds()
    hfilesize = humanfriendly.format_size(filesize, binary=True)
    hduration = humanfriendly.format_timespan(duration, max_units=2)
    speed = humanfriendly.format_size(filesize / duration)
    msg = f"uploaded {hfilesize} in {hduration} ({speed}/s)"
    logger.info(f"[stats] {msg}")


def watched_upload(delay: int, method: Callable, **kwargs) -> int:
    str_delay = humanfriendly.format_timespan(delay) if humanfriendly else f"{delay}s"
    logger.info(f"... watching file until {str_delay} after last modification")

    class ExitCatcher:
        def __init__(self):
            self.requested = False
            for name in ["TERM", "INT", "QUIT"]:
                signal.signal(getattr(signal, f"SIG{name}"), self.on_exit)

        def on_exit(self, signum, frame):
            self.requested = True
            logger.info(f"received signal {signal.strsignal(signum)}, graceful exit.")

    exit_catcher = ExitCatcher()
    last_change = datetime.datetime.fromtimestamp(kwargs["src_path"].stat().st_mtime)
    last_upload, retries = None, 10

    while (
        # make sure we upload it at least once
        not last_upload
        # delay without change has not expired
        or datetime.datetime.now() - datetime.timedelta(seconds=delay) < last_change
    ):
        # file has changed (or initial), we need to upload
        if not last_upload or last_upload < last_change:
            started_on = datetime.datetime.now()
            kwargs["filesize"] = kwargs["src_path"].stat().st_size
            returncode = method(**kwargs)
            if returncode != 0:
                retries -= 1
                if retries <= 0:
                    return returncode
            else:
                if not last_upload:  # this was first run
                    kwargs["resume"] = True
                last_upload = started_on

        if exit_catcher.requested:
            break

        # nb of seconds to sleep between modtime checks
        time.sleep(1)

        # refresh modification time
        last_change = datetime.datetime.fromtimestamp(
            kwargs["src_path"].stat().st_mtime
        )
    if not exit_catcher.requested:
        logger.info(f"File last modified on {last_change}. Delay expired.")

    return 0


def get_expiration_for(delete_after: int) -> datetime.datetime:
    # set expiration after bucket's min retention.
    # bucket retention is 1d minumum.
    # can be configured to loger value.
    # if expiration before bucket min retention, raises 400 Bad Request
    # on compliance
    return (
        datetime.datetime.now()
        + datetime.timedelta(days=max(delete_after, 0) or 1)
        # adding 1mn to prevent clash with bucket's equivalent min retention
        + datetime.timedelta(seconds=60)
    )

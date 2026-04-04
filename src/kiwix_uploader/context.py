import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import humanfriendly
except ImportError:
    humanfriendly = None


@dataclass(kw_only=True)
class Context:
    # singleton instance
    _instance: "Context | None" = None
    debug: bool = False

    host_know_file: Path = (
        Path(os.getenv("HOST_KNOW_FILE", "~/.ssh/known_hosts")).expanduser().resolve()
    )
    marker_file = Path(os.getenv("MARKER_FILE", "/usr/share/marker"))
    scp_bin_path = Path(os.getenv("SCP_BIN_PATH", "/usr/bin/scp"))
    sftp_bin_path = Path(os.getenv("SFTP_BIN_PATH", "/usr/bin/sftp"))
    s3_schemes = ("s3", "s3+http", "s3+https")

    username: str = ""
    resume: bool = False
    watch_for: str = ""
    move: bool = False
    delete: bool = False
    compress: bool = False
    bandwidth: int = -1
    cipher: str = "aes128-ctr"
    delete_after: int = -1
    attempts: int = 3
    attempts_delay: int = 3 * 60  # seconds

    logger: logging.Logger = logging.getLogger("uploader")  # noqa: RUF009

    def __post_init__(self):
        self.debug = bool(os.getenv("debug", ""))

    @classmethod
    def setup(cls, **kwargs: Any):
        cls._instance = cls(**kwargs)
        cls._instance.__post_init__()
        cls.setup_logger()

    @classmethod
    def setup_logger(cls):
        debug = cls._instance.debug if cls._instance else cls.debug
        if cls._instance:
            cls._instance.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        else:
            cls.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        logging.basicConfig(
            level=logging.DEBUG if debug else logging.INFO,
            format="%(asctime)s [%(threadName)40s] %(levelname)8s | %(message)s",
        )

        def handle_exc(msg, *args, **kwargs):
            cls.logger.debug(msg, *args, exc_info=True, **kwargs)

        cls.logger.exception = handle_exc

    @classmethod
    def get(cls) -> "Context":
        if not cls._instance:
            raise OSError("Uninitialized context")  # pragma: no cover
        return cls._instance

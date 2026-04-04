import datetime
import threading
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from kiwix_uploader.context import Context
from kiwix_uploader.utils import parse_url, rebuild_uri

context = Context.get()
logger = context.logger


def excepthook(args: threading.ExceptHookArgs, /):
    """record exception in the thread it was emited from"""
    logger.error(
        f"Upload thread {args.thread} raised {args.exc_type}: {args.exc_value}"
    )
    logger.debug(args.exc_traceback)
    if isinstance(args.thread, UploadThread):
        args.thread.record_exc(args.exc_type, args.exc_value)


threading.excepthook = excepthook


@dataclass
class UploadResult:
    """Single destination upload result"""

    upload_url: str
    returncode: int
    fname: Path
    started_on: datetime.datetime
    ended_on: datetime.datetime

    @property
    def succeeded(self) -> bool:
        return self.returncode == 0

    @property
    def upload_url_repr(self) -> str:
        """credentials-removed upload URL"""
        uri = parse_url(self.upload_url)
        qs = urllib.parse.parse_qs(uri.query)
        for key in ("keyId", "secretAccessKey"):
            if key in qs:
                del qs[key]
        uri = rebuild_uri(
            uri, password=None, query=urllib.parse.urlencode(qs, doseq=True)
        )
        return uri.geturl()

    @property
    def duration(self) -> datetime.timedelta:
        return self.ended_on - self.started_on

    @classmethod
    def from_error(
        cls, upload_url: str, fname: Path, started_on: datetime.datetime | None = None
    ) -> "UploadResult":
        now = datetime.datetime.now()
        return cls(
            upload_url=upload_url,
            returncode=1,
            fname=fname,
            started_on=started_on or now,
            ended_on=started_on or now,
        )


@dataclass
class UploadResults:
    """Multi-destination upload results collection"""

    results: list[UploadResult]
    exo_returncode: int = 1

    @property
    def returncode(self) -> int:
        # dont silently succeed if there's no result
        if not self.results:
            return self.exo_returncode
        return sum([res.returncode for res in self.results])

    @classmethod
    def failure(cls, returncode: int = 1) -> "UploadResults":
        return cls(results=[], exo_returncode=returncode)


class UploadThread(threading.Thread):
    def __init__(self, target: Callable, upload_url: str, /, **kwargs):
        self.upload_url = upload_url
        self.fname = Path(kwargs["src_path"].name)
        kwargs["upload_url"] = self.upload_url  # sent as target arg

        upload_url_ = parse_url(upload_url)
        super().__init__(
            group=None,
            target=target,
            name=f"T-{upload_url_.scheme.upper()}-{upload_url_.hostname}"[:40],
            args=(),
            kwargs=kwargs,
            daemon=True,
        )
        self.returncode: int
        self.started_on: datetime.datetime
        self.ended_on: datetime.datetime

    def run(self):
        try:
            self.started_on = datetime.datetime.now()
            self.returncode = self._target(**self._kwargs) or 0  # pyright:ignore[reportAttributeAccessIssue]  # ty:ignore[unresolved-attribute]
        finally:
            self.ended_on = datetime.datetime.now()
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs  # pyright:ignore[reportAttributeAccessIssue]  # ty:ignore[unresolved-attribute]

    def record_exc(self, exc_type, exc_value):
        self.returncode = 20
        self.ended_on = datetime.datetime.now()
        self.exc_type = exc_type
        self.exc_value = exc_value

    @property
    def result(self) -> UploadResult:
        if self.is_alive():
            raise OSError(f"Thread {self.name} is still alive")
        try:
            return UploadResult(
                upload_url=self.upload_url,
                returncode=self.returncode,
                fname=self.fname,
                started_on=self.started_on,
                ended_on=self.ended_on,
            )
        except Exception:
            return UploadResult.from_error(
                upload_url=self.upload_url,
                fname=self.fname,
                started_on=getattr(self, "started_on", None),
            )


class UploadsManager:
    """Upload threads management"""

    def __init__(self, target: Callable, **kwargs) -> None:
        self.threads: list[UploadThread] = []
        self.upload_urls: list[str] = kwargs.pop("upload_urls")
        self.kwargs = kwargs
        self.started_on = datetime.datetime.now()

        for upload_url in self.upload_urls:
            self.threads.append(UploadThread(target, upload_url, **self.kwargs))

    def start(self):
        for thread in self.threads:
            thread.start()

    def wait(self, timeout: float | None = None):
        for thread in self.threads:
            thread.join(timeout=timeout)
        return self.returncode

    @property
    def results(self) -> UploadResults:
        for thread in self.threads:
            if thread.is_alive():
                raise OSError(f"Upload thread {thread.name} is still alive")
        return UploadResults(results=[thread.result for thread in self.threads])

    @property
    def returncode(self) -> int | None:
        for thread in self.threads:
            if thread.is_alive():
                return None
        return sum([t.returncode or 0 for t in self.threads])

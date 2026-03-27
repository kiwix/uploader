import argparse
import logging
import os
import sys

from kiwix_uploader.upload import check_and_upload_file
from kiwix_uploader.context import Context

logger = Context.logger


def main():
    parser = argparse.ArgumentParser(prog="uploader")

    parser.add_argument(
        "--file",
        help="absolute path to source file to upload",
        required=True,
        dest="src_path",
    )

    parser.add_argument(
        "--upload-url",
        help="upload URL to upload to (folder, trailing-slash)",
        required=True,
        dest="upload_url",
    )

    parser.add_argument(
        "--key",
        help="path to RSA private key",
        dest="private_key",
        required=False,
        default=os.getenv("RSA_KEY", "/etc/ssh/keys/id_rsa"),
    )

    parser.add_argument(
        "--username",
        help="username to authenticate to warehouse (if not in URI)",
    )

    parser.add_argument(
        "--resume",
        help="whether to continue uploading existing remote file instead "
        "of overriding (SFTP only)",
        action="store_true",
        default=False,
    )

    # format: https://humanfriendly.readthedocs.io/en/latest/api.html
    # humanfriendly.parse_timespan
    parser.add_argument(
        "--watch",
        help="Keep uploading until file has not been changed "
        "for that period of time (ex. 10s 1m 2h 3d)",
        dest="watch_for",
        action="store",
    )

    parser.add_argument(
        "--move",
        help="whether to upload to a temp location and move to final one on success",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "--delete",
        help="whether to delete source file upon success",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "--compress",
        help="whether to enable ssh compression on transfer (good for text)",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "--bandwidth",
        help="limit bandwidth used for transfer. In Kbit/s.",
        type=int,
    )

    parser.add_argument(
        "--cipher", help="Cipher to use with SSH.", default="aes128-ctr"
    )

    parser.add_argument(
        "--delete-after",
        help="nb of days after which to autodelete "
        "(Wasabi/S3-only, bucket must support it)",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--attempts",
        help="Number of upload attempts before giving up should it fail",
        default=3,
        type=int,
    )

    parser.add_argument(
        "--attempts-delay",
        help="Delay (seconds) between attempts should the upload fail.",
        default=3 * 60,
        type=int,
    )

    parser.add_argument(
        "--debug",
        help="change logging level to DEBUG",
        action="store_true",
        default=False,
    )

    args = parser.parse_args()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)

    Context.setup(
        debug=args.debug,
        username=args.username,
        resume=args.resume,
        watch_for=args.watch_for,
        move=args.move,
        delete=args.delete,
        compress=args.compress,
        bandwidth=args.bandwidth,
        cipher=args.cipher,
        delete_after=args.delete_after,
        attempts=args.attempts,
        attempts_delay=args.attempts_delay,
    )

    sys.exit(
        check_and_upload_file(
            src_path=args.src_path,
            upload_url=args.upload_url,
            private_key=args.private_key,
        )
    )


if __name__ == "__main__":
    main()

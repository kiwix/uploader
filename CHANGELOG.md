# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.7.1]

### Fixed

- S3 upload (was receiving unexpected arguments)

### Changed

- Simplified marker upload by leveraging `upload_file()` (transparent change)

## [1.7.0] - 2026-04-10

### Added

- `set_marker_retrying()` to update a marker file

### Changed

- Renamed `sftp.sftp_remote_file_exists` to `sftp.sftp_remote_filesize`
- > it always returns an int. 0 if the file doesnt exists.

### Fixed

- SFTP uploads of files with space in filename
- SCP(SSH) removal of files

## [1.6.0] - 2026-04-08

### Added

- `remove_file()` and `remove_file_retrying()` to delete files (not exposed over CLI)

## [1.5.1] - 2026-04-06

### Added

- `UploadManager.succeeded` shortcut

### Fixed

- `multi_file_upload()` would delete source file if `--delete` regardless of success
- `upload_file()` would fail on missing source path (testing filesize) instead of returning error

## [1.5] - 2026-04-05

Adds multiple destination support to uploader (uses threads)

### Added

- `multi_file_upload()`
- `UploadResult` and `UploadResults` types, returned by `multi_file_upload()`
- `UploadThread` and `UploadManager` for additional control
- `delete_after` creating a `.delete_on` marker file containing an UTC date to delete file on.

### Changed

- `check_and_upload_file`, `upload_file`, `watched_upload`, `upload_file_retrying` moved from `upload` module to `api` module.
- Log messages now includes thread name
- `delete_after` renamed to `wasabi_delete_after` as it was a wasabi-specific feature.
- ⚠️ `delete_after` param reused for marker file feature

## [1.4] - 2026-03-27

### Added

- Python 3.14 support

### Changed

- Renamed python package from `openzim-uploader` to `kiwix-uploader`
- Changed source repository from `openzim/zimfarm` to `kiwix/uploader`
- Using humandfriendly 10 and kiwixstorage 0.10.1
- Docker image moved to ghcr.io/kiwix/uploader
- Docker image now based on 3.14-alpine
- Exploded single source file into multiple ones.

## [1.3] - 2025-10-23

### Removed

- Python 3.12 support

### Added

- Python 3.14 support
- CONTRIBUTING

### Changed

- S3 upload accepts URL schemes `s3+http` and `s3+https`
- Using kiwixstorage 0.9.0

## [1.2] - 2022-09-24

### Added

- Simple retry mechanism via `--attempts` and `--attempts-delay`

## [1.0] - 2022-09-01

- Initial version

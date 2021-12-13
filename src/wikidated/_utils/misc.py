#
# Copyright 2021 Lukas Schmelzeisen
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from contextlib import contextmanager
from datetime import date, timedelta
from logging import getLogger
from pathlib import Path
from subprocess import PIPE, Popen, TimeoutExpired
from typing import IO, TYPE_CHECKING, Iterator, Optional, Sequence, Union

import requests
from tqdm import tqdm  # type: ignore
from typing_extensions import Protocol

_LOGGER = getLogger(__name__)


def next_month(day: date) -> date:
    if day.month == 12:
        return date(year=day.year + 1, month=1, day=1)
    else:
        return date(year=day.year, month=day.month + 1, day=1)


def days_between_dates(start: date, stop: date) -> Sequence[date]:
    results = []
    cur_date = start
    while cur_date <= stop:
        results.append(cur_date)
        cur_date += timedelta(days=1)
    return results


def month_between_dates(start: date, stop: date) -> Sequence[date]:
    results = []
    cur_date = start if start.day == 1 else next_month(start)
    while cur_date <= stop:
        results.append(cur_date)
        cur_date = next_month(cur_date)
    return results


# Adapted from: https://stackoverflow.com/a/37573701/211404
def download_file_with_progressbar(
    url: str, dest: Union[Path, IO[bytes]], *, description: Optional[str] = None
) -> None:
    if isinstance(dest, Path):
        _LOGGER.debug(f"Downloading '{url}' to file '{dest}'.")
    else:
        _LOGGER.debug(f"Downloading '{url}'.")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    chunk_size = 4 * 1024  # 4 KiB

    fd: IO[bytes]
    if isinstance(dest, Path):
        fd = dest.open("wb")
    else:
        fd = dest

    try:
        bytes_written = 0
        with tqdm(
            desc=description or "",
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            dynamic_ncols=True,
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size):
                bytes_written += fd.write(chunk)
                progress_bar.update(len(chunk))
    finally:
        if isinstance(dest, Path):
            fd.close()

    _LOGGER.debug(f"Done downloading '{url}'.")
    if total_size != 0 and total_size != bytes_written:  # pragma: no cover
        _LOGGER.warning(
            f"Size mismatch for file downloaded from '{url}', expected {total_size} "
            f"bytes got {bytes_written} bytes."
        )


class Hash(Protocol):
    @property
    def name(self) -> str:
        ...

    def update(self, buffer: bytes) -> None:
        ...

    def hexdigest(self) -> str:
        ...


def hashsum(file: Union[Path, IO[bytes]], h: Hash) -> str:
    fd: IO[bytes]
    if isinstance(file, Path):
        fd = file.open("rb")
    else:
        fd = file

    try:
        for buffer in iter(lambda: fd.read(128 * 1024), b""):
            h.update(buffer)
    finally:
        if isinstance(file, Path):
            fd.close()

    return h.hexdigest()


def hashcheck(file: Union[Path, IO[bytes]], h: Hash, expected: str) -> None:
    actual = hashsum(file, h)
    if actual != expected:
        file_name = f"File '{file}'" if isinstance(file, Path) else "File"
        raise Exception(
            f"{file_name} has {h.name} hash '{actual}' but '{expected}' was expected."
        )


# Python 3.7 does not allow indexing Popen yet, but mypy requires it.
if TYPE_CHECKING:
    Popen_str = Popen[str]
else:
    Popen_str = Popen


@contextmanager
def external_process(
    args: Sequence[str],
    *,
    stdin: Optional[int],
    stdout: Optional[int],
    stderr: Optional[int],
    cwd: Optional[Path] = None,
    name: Optional[str] = None,
    exhaust_stdout_to_log: bool = False,
    exhaust_stderr_to_log: bool = False,
    terminate_timeout: Optional[float] = 1,
    check_return_code_zero: bool = False,
) -> Iterator[Popen_str]:
    if name is None:
        name = args[0]
        _LOGGER.debug(f"Starting external process '{' '.join(args)}'")
    else:
        _LOGGER.debug(f"Starting external process {name}: '{' '.join(args)}'")

    process = Popen(
        args, stdin=stdin, stdout=stdout, stderr=stderr, cwd=cwd, encoding="UTF-8"
    )

    try:
        yield process

    finally:
        if stdin == PIPE:
            assert process.stdin is not None
            process.stdin.close()

        if exhaust_stdout_to_log:
            assert stdout == PIPE
            assert process.stdout is not None
            for line in process.stdout:
                _LOGGER.debug(f"{name}: {line.rstrip()}")

        if exhaust_stderr_to_log:
            assert stderr == PIPE
            assert process.stderr is not None
            for line in process.stderr:
                _LOGGER.error(f"{name}: {line.rstrip()}")

        process.terminate()
        try:
            process.wait(timeout=terminate_timeout)
        except TimeoutExpired as e:
            _LOGGER.exception(
                f"External process {name} did not terminate, killing...", e
            )
            process.kill()
        _LOGGER.debug(f"Ended external process {name}")

        if check_return_code_zero and process.returncode != 0:
            raise Exception(
                f"External process {name} had non-zero return code: "
                f"{process.returncode}"
            )

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

from logging import getLogger
from pathlib import Path
from typing import IO, Optional, Union

import requests
from tqdm import tqdm  # type: ignore
from typing_extensions import Protocol

_LOGGER = getLogger(__name__)


# Adapted from: https://stackoverflow.com/a/37573701/211404
def download_file_with_progressbar(
    url: str, dest: Union[Path, IO[bytes]], description: Optional[str] = None
) -> None:
    if isinstance(dest, Path):
        _LOGGER.debug(f"Downloading url '{url}' to file '{dest}'...")
    else:
        _LOGGER.debug(f"Downloading url '{url}'...")

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

    if total_size != 0 and total_size != bytes_written:  # pragma: no cover
        _LOGGER.warning(
            f"  Downloaded file size mismatch, expected {total_size} bytes got "
            f"{bytes_written} bytes."
        )


class Hash(Protocol):
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
        raise Exception(f"File has hash '{actual}' but '{expected}' was expected.")

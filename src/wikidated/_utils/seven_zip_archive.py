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

from __future__ import annotations

from contextlib import contextmanager
from logging import getLogger
from os.path import relpath
from pathlib import Path
from subprocess import DEVNULL, PIPE
from typing import IO, Iterator, Optional

from wikidated._utils.misc import external_process

_LOGGER = getLogger(__name__)


class SevenZipArchive:
    def __init__(self, path: Path) -> None:
        self._path = path

    @classmethod
    def from_dir(cls, dir_: Path, path: Path) -> SevenZipArchive:
        with external_process(
            ("7z", "a", "-ms=off", relpath(path, dir_), "."),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            cwd=dir_,
            exhaust_stdout_to_log=True,
            exhaust_stderr_to_log=True,
            check_return_code_zero=True,
        ) as _:
            pass
        return SevenZipArchive(path)

    @contextmanager
    def write(self, file_name: Optional[Path] = None) -> Iterator[IO[str]]:
        # This method seems to take longer the more file already exist in the archive.
        # If you plan want to create archives with many files, it is better to just
        # create a directory with all files in it as you need them, and than to convert
        # that to an archive using SevenZipArchive.from_dir().
        file_name_str = str(file_name) if file_name else ""
        with external_process(
            ("7z", "a", "-bd", "-bso0", f"-si{file_name_str}", str(self._path)),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            exhaust_stdout_to_log=True,
            exhaust_stderr_to_log=True,
            check_return_code_zero=True,
        ) as seven_zip_process:
            assert seven_zip_process.stdin is not None
            yield seven_zip_process.stdin

    @contextmanager
    def read(self, file_name: Optional[Path] = None) -> Iterator[IO[str]]:
        file_name_str = str(file_name) if file_name else ""
        # Not sure how to check for errors here (particularly, if one wants to end
        # processing output from stdout, before the full archive if depleted). Waiting
        # for output on stderr stalls the process. Terminating while output in stdout is
        # still being generated results in a -15 return code.
        with external_process(
            ("7z", "x", "-so", str(self._path), file_name_str),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
        ) as seven_zip_process:
            assert seven_zip_process.stdout is not None
            yield seven_zip_process.stdout

    def iter_file_names(self) -> Iterator[Path]:
        with external_process(
            ("7z", "l", "-ba", "-slt", str(self._path)),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
        ) as seven_zip_process:
            assert seven_zip_process.stdout is not None
            for line in seven_zip_process.stdout:
                if line.startswith("Path = "):
                    yield Path(line[len("Path = ") : -len("\n")])

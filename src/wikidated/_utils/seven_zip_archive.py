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
from logging import getLogger
from pathlib import Path
from subprocess import DEVNULL, PIPE
from typing import IO, Iterator, Optional

from wikidated._utils.misc import external_process

_LOGGER = getLogger(__name__)


class SevenZipArchive:
    def __init__(self, path: Path) -> None:
        self._path = path

    @contextmanager
    def write(self, file_name: Optional[Path] = None) -> Iterator[IO[str]]:
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
        with external_process(
            ("7z", "x", "-so", str(self._path), file_name_str),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            exhaust_stderr_to_log=True,
            check_return_code_zero=True,
        ) as seven_zip_process:
            assert seven_zip_process.stdout is not None
            yield seven_zip_process.stdout

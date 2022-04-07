#
# Copyright 2021-2022 Lukas Schmelzeisen
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
from shutil import rmtree
from subprocess import DEVNULL, PIPE
from typing import IO, Any, Callable, Iterator, Optional

from typing_extensions import Final, Protocol

from wikidated._utils.misc import external_process

_LOGGER = getLogger(__name__)


class _SupportsLessThan(Protocol):
    def __lt__(self, __other: Any) -> bool:
        ...


class SevenZipArchive:
    def __init__(self, path: Path) -> None:
        self.path: Final = path

    @classmethod
    def from_dir(cls, dir_: Path, path: Path) -> SevenZipArchive:
        tmp_path = path.parent / (".tmp." + path.name)
        _LOGGER.debug(f"Creating 7z archive {path} from directory {dir_}.")
        with external_process(
            ("7z", "a", "-ms=off", relpath(tmp_path, dir_), "."),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            cwd=dir_,
            exhaust_stdout_to_log=True,
            exhaust_stderr_to_log=True,
            check_return_code_zero=True,
        ) as _:
            pass
        tmp_path.rename(path)
        return SevenZipArchive(path)

    @classmethod
    def from_dir_with_order(
        cls, dir_: Path, path: Path, key: Callable[[Path], _SupportsLessThan]
    ) -> SevenZipArchive:
        tmp_path = path.parent / f".tmp.{path.name}"
        tmp_dir = path.parent / f".tmp.{path.name}.contents"
        listfile_rename = path.parent / f".tmp.{path.name}.listfile-rename"
        _LOGGER.debug(f"Creating ordered 7z archive {path} from directory {dir_}.")

        files = list(dir_.iterdir())
        files.sort(key=key)
        ordered_filename_num_digits = len(str(len(files) - 1))

        tmp_dir.mkdir(exist_ok=False, parents=True)
        with listfile_rename.open("w", encoding="UTF-8") as fout:
            for i, file in enumerate(files):
                ordered_filename = f"{i:0{ordered_filename_num_digits}d}"
                fout.write(f"{ordered_filename}\n{file.name}\n")
                (tmp_dir / ordered_filename).symlink_to(file.resolve())

        with external_process(
            ("7z", "a", "-l", "-ms=off", relpath(tmp_path, tmp_dir), "."),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            cwd=tmp_dir,
            exhaust_stdout_to_log=True,
            exhaust_stderr_to_log=True,
            check_return_code_zero=True,
        ) as _:
            pass

        rmtree(tmp_dir)

        with external_process(
            ("7z", "rn", f"{tmp_path}", f"@{listfile_rename}"),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            exhaust_stdout_to_log=True,
            exhaust_stderr_to_log=True,
            check_return_code_zero=True,
        ) as _:
            pass

        listfile_rename.unlink()

        tmp_path.rename(path)
        return SevenZipArchive(path)

    @contextmanager
    def write(self, file_name: Optional[Path] = None) -> Iterator[IO[str]]:
        # This method seems to take longer the more file already exist in the archive.
        # If you plan want to create archives with many files, it is better to just
        # create a directory with all files in it as you need them, and then to convert
        # that to an archive using SevenZipArchive.from_dir().
        if file_name:
            _LOGGER.debug(f"Writing file {file_name} to 7z archive {self.path}.")
        else:
            _LOGGER.debug(f"Writing to 7z archive {self.path}.")
        file_name_str = str(file_name) if file_name else ""
        with external_process(
            ("7z", "a", "-bd", "-bso0", f"-si{file_name_str}", str(self.path)),
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
        if file_name:
            _LOGGER.debug(f"Reading file {file_name} from 7z archive {self.path}.")
        else:
            _LOGGER.debug(f"Reading from 7z archive {self.path}.")
        file_name_str = str(file_name) if file_name else ""
        # Not sure how to check for errors here (particularly, if one wants to end
        # processing output from stdout, before the full archive if depleted). Waiting
        # for output on stderr stalls the process. Terminating while output in stdout is
        # still being generated results in a -15 return code.
        with external_process(
            ("7z", "x", "-so", str(self.path), file_name_str),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
        ) as seven_zip_process:
            assert seven_zip_process.stdout is not None
            yield seven_zip_process.stdout

    def iter_file_names(self) -> Iterator[Path]:
        _LOGGER.debug(f"Iterating file names in 7z archive {self.path}.")
        with external_process(
            ("7z", "l", "-ba", "-slt", str(self.path)),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
        ) as seven_zip_process:
            assert seven_zip_process.stdout is not None
            for line in seven_zip_process.stdout:
                if line.startswith("Path = "):
                    yield Path(line[len("Path = ") : -len("\n")])

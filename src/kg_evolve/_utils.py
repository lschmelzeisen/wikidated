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
from subprocess import PIPE, Popen, TimeoutExpired
from typing import IO, ContextManager, Iterator, Optional, Union, overload

from nasty_utils import ColoredBraceStyleAdapter

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


@overload
def p7z_open(file: Path) -> ContextManager[IO[bytes]]:
    ...


@overload
def p7z_open(file: Path, encoding: str) -> ContextManager[IO[str]]:
    ...


@contextmanager  # type: ignore
def p7z_open(
    file: Path, encoding: Optional[str] = None
) -> Iterator[Union[IO[str], IO[bytes]]]:
    p7z = Popen(
        ["7z", "x", "-so", str(file)],
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        encoding=encoding,
    )
    assert p7z.stdout is not None

    # TODO: I have no idea how to check for errors in 7z. Calling
    #  seven_zip.stderr.read() will block the thread indefinitely, if there is no
    #  error. It seems the correct way to do this, would be to create the process
    #  using asyncio, see https://stackoverflow.com/a/34114767/211404

    try:
        yield p7z.stdout
    finally:
        p7z.terminate()
        try:
            p7z.wait(timeout=1)
        except TimeoutExpired as e:
            _LOGGER.exception("7z Process did not terminate, killing...", e)
            p7z.kill()

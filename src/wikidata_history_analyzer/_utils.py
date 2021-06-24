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

import atexit
import hashlib
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from contextlib import contextmanager
from io import BytesIO
from logging import getLogger
from multiprocessing import Manager
from pathlib import Path
from subprocess import PIPE, Popen, TimeoutExpired
from typing import (
    IO,
    BinaryIO,
    ContextManager,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from nasty_utils import ColoredBraceStyleAdapter
from tqdm import tqdm
from typing_extensions import Protocol

from wikidata_history_analyzer.jvm_manager import JvmManager

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


def sha1sum(file: Union[Path, BinaryIO, BytesIO]) -> str:
    if isinstance(file, Path):
        fd = file.open("rb")
    else:
        fd = file

    try:
        h = hashlib.sha1()
        for buffer in iter(lambda: fd.read(128 * 1024), b""):
            h.update(buffer)
    finally:
        if isinstance(file, Path):
            fd.close()

    return h.hexdigest()


_T_Argument = TypeVar("_T_Argument", contravariant=True)
_T_Return = TypeVar("_T_Return", covariant=True)


class ParallelizeCallback(Protocol[_T_Argument, _T_Return]):
    def __call__(self, argument: _T_Argument, **kwargs: object) -> _T_Return:
        ...


class ParallelizeProgressCallback(Protocol):
    def __call__(
        self, name: str, n: Union[int, float], total: Union[int, float]
    ) -> None:
        ...


def parallelize(
    func: ParallelizeCallback[_T_Argument, _T_Return],
    arguments: Iterable[_T_Argument],
    *,
    extra_arguments: Mapping[str, object],
    total: Optional[int] = None,
    max_workers: int,
    jars_dir: Optional[Path] = None,
    update_frequency: float = 5.0,
) -> Iterator[_T_Return]:
    num_workers = min(max_workers, total) if total is not None else max_workers
    with ProcessPoolExecutor(
        max_workers=num_workers, initializer=_init_worker, initargs=(jars_dir,)
    ) as pool, tqdm(
        total=total, dynamic_ncols=True, position=-num_workers
    ) as pbar_overall, Manager() as manager:
        pbar_args: MutableMapping[
            str, Tuple[Union[int, float], Union[int, float]]
        ] = manager.dict()  # type: ignore
        pbars: MutableMapping[str, tqdm[None]] = {}

        futures_not_done = {
            pool.submit(_func_wrapper, func, argument, extra_arguments, pbar_args)
            for argument in arguments
        }

        while True:
            futures_done, futures_not_done = wait(
                futures_not_done, timeout=update_frequency, return_when=FIRST_COMPLETED
            )

            for pbar_name, (pbar_n, pbar_total) in pbar_args.items():
                pbar = pbars.get(pbar_name)
                if not pbar:
                    pbar = tqdm(desc=pbar_name, dynamic_ncols=True)
                    pbars[pbar_name] = pbar
                pbar.n = pbar_n
                pbar.total = pbar_total
                pbar.refresh()
                if pbar_n == pbar_total:
                    pbar.close()
                pbar_overall.refresh()

            if futures_done:
                for future in futures_done:
                    try:
                        yield future.result()
                        pbar_overall.update(1)
                    except BaseException as exception:
                        _LOGGER.exception(
                            "Exception during parallelize: {} {}",
                            type(exception).__name__,
                            str(exception),
                        )

            if not futures_not_done:
                break

        for pbar in pbars.values():
            pbar.close()


_JVM_MANAGER: Optional[JvmManager] = None


def _init_worker(jars_dir: Optional[Path]) -> None:
    global _JVM_MANAGER
    if jars_dir is not None:
        _JVM_MANAGER = JvmManager(jars_dir)
    atexit.register(_exit_worker)


def _exit_worker() -> None:
    global _JVM_MANAGER
    if _JVM_MANAGER is not None:
        _JVM_MANAGER.close()
        _JVM_MANAGER = None


def _func_wrapper(
    func: ParallelizeCallback[_T_Argument, _T_Return],
    argument: _T_Argument,
    extra_arguments: Mapping[str, object],
    pbar_args: MutableMapping[str, Tuple[Union[int, float], Union[int, float]]],
) -> _T_Return:
    def progress_callback(
        name: str, n: Union[int, float], total: Union[int, float]
    ) -> None:
        pbar_args[name] = (n, total)

    return func(
        argument,
        **extra_arguments,
        progress_callback=progress_callback,
        jvm_manager=_JVM_MANAGER,
    )

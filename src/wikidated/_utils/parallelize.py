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

import atexit
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from logging import getLogger
from multiprocessing import Manager, cpu_count
from typing import (
    Collection,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from tqdm import tqdm  # type: ignore
from typing_extensions import Protocol

_LOGGER = getLogger(__name__)

_T_Argument = TypeVar("_T_Argument", contravariant=True)
_T_Return = TypeVar("_T_Return", covariant=True)


class ParallelizeUpdateProgressFunc(Protocol):
    def __call__(
        self, name: str, n: Union[int, float], total: Union[int, float]
    ) -> None:
        ...


class ParallelizeFunc(Protocol[_T_Argument, _T_Return]):
    def __call__(
        self,
        argument: _T_Argument,
        update_progress: ParallelizeUpdateProgressFunc,
        **extra_arguments: object,
    ) -> _T_Return:
        ...


class ParallelizeInitWorkerFunc(Protocol):
    def __call__(self) -> Optional[Mapping[str, object]]:
        ...


class ParallelizeExitWorkerFunc(Protocol):
    def __call__(self) -> None:
        pass


_EXTRA_ARGUMENTS_FROM_INIT_FUNC: Optional[Mapping[str, object]] = None


def parallelize(
    func: ParallelizeFunc[_T_Argument, _T_Return],
    arguments: Union[Iterator[_T_Argument], Iterable[_T_Argument]],
    *,
    num_arguments: Optional[int] = None,
    extra_arguments: Optional[Mapping[str, object]] = None,
    init_worker_func: Optional[ParallelizeInitWorkerFunc] = None,
    exit_worker_func: Optional[ParallelizeExitWorkerFunc] = None,
    max_workers: Optional[int] = None,
    update_frequency: float = 5.0,
    reraise_exceptions: bool = True,
) -> Iterator[_T_Return]:
    if max_workers is None:
        max_workers = cpu_count()
    if num_arguments is not None and max_workers > num_arguments:
        max_workers = num_arguments

    if extra_arguments is None:
        extra_arguments = {}

    with ProcessPoolExecutor(
        max_workers=max_workers,
        initializer=_init_worker_wrapper,
        initargs=(init_worker_func, exit_worker_func),
    ) as pool, tqdm(
        total=num_arguments, dynamic_ncols=True, position=-max_workers
    ) as progress_bar_overall, Manager() as shared_manager:
        progress_bar_state: MutableMapping[
            str, Tuple[Union[int, float], Union[int, float]]
        ] = shared_manager.dict()  # type: ignore
        progress_bars_for_workers: MutableMapping[str, tqdm] = {}

        for result in _process_futures(
            {
                pool.submit(
                    _func_wrapper, func, argument, extra_arguments, progress_bar_state
                )
                for argument in arguments
            },
            update_frequency=update_frequency,
            reraise_exceptions=reraise_exceptions,
            progress_bar_overall=progress_bar_overall,
            progress_bars_for_workers=progress_bars_for_workers,
            progress_bar_state=progress_bar_state,
        ):
            yield result

        for progress_bar in progress_bars_for_workers.values():
            progress_bar.close()


def _process_futures(
    futures: Collection[Future[_T_Return]],
    *,
    update_frequency: float,
    reraise_exceptions: bool,
    progress_bar_overall: tqdm,
    progress_bars_for_workers: MutableMapping[str, tqdm],
    progress_bar_state: MutableMapping[
        str, Tuple[Union[int, float], Union[int, float]]
    ],
) -> Iterator[_T_Return]:
    futures_not_done = futures

    while futures_not_done:
        futures_done, futures_not_done = wait(
            futures_not_done,
            timeout=update_frequency,
            return_when=FIRST_COMPLETED,
        )

        for name, (n, total) in progress_bar_state.items():
            progress_bar = progress_bars_for_workers.get(name)
            if not progress_bar:
                progress_bar = tqdm(desc=name, dynamic_ncols=True)
                progress_bars_for_workers[name] = progress_bar

            progress_bar.n = n
            progress_bar.total = total
            progress_bar.refresh()

            if n == total:
                progress_bar.close()

            progress_bar_overall.refresh()

        for future in futures_done:
            try:
                yield future.result()
                progress_bar_overall.update(1)
            except BaseException as e:
                _LOGGER.exception(
                    f"Exception during parallelize: {type(e).__name__} {e}"
                )
                if reraise_exceptions:
                    raise


def _init_worker_wrapper(
    init_worker_func: Optional[ParallelizeInitWorkerFunc],
    exit_worker_func: Optional[ParallelizeExitWorkerFunc],
) -> None:
    if init_worker_func is not None:
        global _EXTRA_ARGUMENTS_FROM_INIT_FUNC
        _EXTRA_ARGUMENTS_FROM_INIT_FUNC = init_worker_func()
    if exit_worker_func is not None:
        atexit.register(exit_worker_func)


def _func_wrapper(
    func: ParallelizeFunc[_T_Argument, _T_Return],
    argument: _T_Argument,
    extra_arguments: Mapping[str, object],
    progress_bar_state: MutableMapping[
        str, Tuple[Union[int, float], Union[int, float]]
    ],
) -> _T_Return:
    def update_progress_func(
        name: str, n: Union[int, float], total: Union[int, float]
    ) -> None:
        progress_bar_state[name] = (n, total)

    return func(
        argument,
        **extra_arguments,
        **(_EXTRA_ARGUMENTS_FROM_INIT_FUNC or {}),
        update_progress=update_progress_func,
    )

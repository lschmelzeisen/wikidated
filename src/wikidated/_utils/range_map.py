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

from typing import (
    AbstractSet,
    Any,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

_T_Value = TypeVar("_T_Value")


class RangeMap(
    Generic[_T_Value],
    MutableMapping[Union[int, range], _T_Value],
):
    def __init__(
        self,
        iterable: Optional[
            Union[Mapping[range, _T_Value], Iterable[Tuple[range, _T_Value]]]
        ] = None,
    ) -> None:
        self._data: MutableSequence[Tuple[range, _T_Value]] = []
        if iterable is not None:
            if isinstance(iterable, Mapping):
                for key, value in iterable.items():
                    self[key] = value
            else:
                for key, value in iterable:
                    self[key] = value

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return bool(self._data)

    @overload
    def __contains__(self, key: int) -> bool:
        ...

    @overload
    def __contains__(self, key: range) -> bool:
        ...

    @overload
    def __contains__(self, key: object) -> Any:  # NoReturn doesn't work.
        ...

    def __contains__(self, key: object) -> bool:
        try:
            _ = self[key]
            return True
        except KeyError:
            return False

    def _index(self, key: int) -> int:
        # Binary search for last item with start of range lesser or equal to given key.
        # Returns -1 if all items have a larger start of range.
        left = 0  # Inclusive.
        right = len(self._data)  # Exclusive.
        while left != right:
            i = left + (right - left) // 2
            if key < self._data[i][0].start:
                right = i
            else:
                left = i + 1
        # assert key < self._data[left][0].start # noqa: E800
        return left - 1

    def __setitem__(self, key: object, value: _T_Value) -> None:
        # O(1) for keys inserted in increasing order, O(n) otherwise.

        if not isinstance(key, range) or key.step != 1:
            raise TypeError("key must be a range with step = 1.")
        if not key:
            raise TypeError("key must not be an empty range.")

        if not self._data or key.start >= self._data[-1][0].stop:
            self._data.append((key, value))
            return

        i = self._index(key.start)
        if key == self._data[i][0]:
            # Overwrite existing item.
            self._data[i] = (key, value)
            return

        if key.stop > self._data[i + 1][0].start or (
            i != -1 and key.start < self._data[i][0].stop
        ):
            raise TypeError("Overlapping ranges.")

        self._data.insert(i + 1, (key, value))

    @overload
    def __getitem__(self, key: int) -> _T_Value:
        ...

    @overload
    def __getitem__(self, key: range) -> _T_Value:
        ...

    @overload
    def __getitem__(self, key: slice) -> Iterable[_T_Value]:
        ...

    @overload
    def __getitem__(self, key: object) -> Any:  # NoReturn doesn't work.
        ...

    def __getitem__(self, key: object) -> Union[_T_Value, Iterable[_T_Value]]:
        # O(log n) for non-slice lookup.

        if isinstance(key, int):
            i = self._index(key)
            if i != -1 and key in self._data[i][0]:
                return self._data[i][1]
        elif isinstance(key, range):
            i = self._index(key.start)
            if i != -1 and key == self._data[i][0]:
                return self._data[i][1]
        elif isinstance(key, slice):
            if key.step is not None and key.step != 1:
                raise ValueError("Slices with steps != 1 are not supported.")
            start = key.start or self._data[0][0].start
            stop = key.stop or self._data[-1][0].stop
            results = []
            for i in range(max(self._index(start), 0), len(self)):
                if self._data[i][0].start >= stop:
                    break
                results.append(self._data[i][1])
            return results
        else:
            raise TypeError("key must either be an int, a range, or a slice.")
        raise KeyError(key)

    def __delitem__(self, key: object) -> None:
        # O(n).

        if not isinstance(key, range):
            raise TypeError("key must be a range.")

        i = self._index(key.start)
        if i == -1 or key != self._data[i][0]:
            raise KeyError(key)
        del self._data[i]

    def __iter__(self) -> Iterator[range]:
        for item_key, _item_value in self._data:
            yield item_key

    def keys(self) -> AbstractSet[range]:
        return cast(AbstractSet[range], super().keys())

    def clear(self) -> None:
        self._data.clear()

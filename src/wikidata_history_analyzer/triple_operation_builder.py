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

from datetime import datetime
from enum import Enum
from logging import getLogger
from pathlib import Path
from types import TracebackType
from typing import IO, NamedTuple, Optional, Sequence, Set, Type

from nasty_utils import ColoredBraceStyleAdapter

from wikidata_history_analyzer.wikidata_rdf_serializer import RdfTriple

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class TripleOperationType(Enum):
    ADD = 1
    DELETE = 2


class TripleOperation(NamedTuple):
    timestamp: datetime
    type: TripleOperationType
    subject: str
    predicate: str
    object: str

    def __str__(self) -> str:
        if self.type == TripleOperationType.ADD:
            op = "+"
        elif self.type == TripleOperationType.DELETE:
            op = "-"
        else:
            raise NotImplementedError()
        return " ".join(
            (self.timestamp.isoformat(), op, self.subject, self.predicate, self.object)
        )

    @classmethod
    def from_str(cls, s: str) -> TripleOperation:
        timestamp, op, subject, predicate, object_ = s.split(" ", 4)
        if op == "+":
            op_ = TripleOperationType.ADD
        elif op == "-":
            op_ = TripleOperationType.DELETE
        else:
            raise ValueError()
        return TripleOperation(
            datetime.fromisoformat(timestamp), op_, subject, predicate, object_
        )


class TripleOperationBuilder:
    def __init__(self, file: Path):
        self._file = file
        self._file_tmp = self._file.parent / (self._file.name + ".tmp")
        self._file_handle: Optional[IO[str]] = None
        self._state: Set[RdfTriple] = set()

    def process_triples(
        self, triples: Sequence[RdfTriple], timestamp: datetime
    ) -> None:

        triples_set = set(triples)
        deleted_triples = self._state - triples_set
        added_triples = triples_set - self._state
        self._state.difference_update(deleted_triples)
        self._state.update(added_triples)

        if deleted_triples or added_triples:
            if self._file_handle is None:
                self._file.parent.mkdir(parents=True, exist_ok=True)
                self._file_handle = self._file_tmp.open("w", encoding="UTF-8")

            for triple in sorted(deleted_triples):
                self._file_handle.write(
                    str(TripleOperation(timestamp, TripleOperationType.DELETE, *triple))
                    + "\n"
                )
            for triple in sorted(added_triples):
                self._file_handle.write(
                    str(TripleOperation(timestamp, TripleOperationType.ADD, *triple))
                    + "\n"
                )

    def __enter__(self) -> TripleOperationBuilder:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self._file_handle:
            self._file_handle.close()
            if not (exc_type or exc_val or exc_tb):
                self._file_tmp.rename(self._file)

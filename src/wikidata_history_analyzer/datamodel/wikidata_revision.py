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

import gzip
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, Type, TypeVar

from pydantic import BaseModel as PydanticModel

_T_WikidataRevision = TypeVar("_T_WikidataRevision", bound="WikidataRevision")


class WikidataRevisionProcessingException(Exception):
    def __init__(
        self,
        reason: str,
        revision: WikidataRevision,
        exception: Optional[Exception] = None,
    ) -> None:
        self.reason = reason
        self.revision = revision
        self.exception = exception

    def __str__(self) -> str:
        return (
            f"{self.reason} ({self.revision.prefixed_title}, "
            f"page: {self.revision.page_id}, revision: {self.revision.revision_id})"
        )


class WikidataRevision(PydanticModel, ABC):
    prefixed_title: str
    namespace: int
    page_id: int
    redirect: Optional[str]
    revision_id: int
    parent_revision_id: Optional[int]
    timestamp: datetime
    contributor: Optional[str]
    contributor_id: Optional[int]
    is_minor: bool
    comment: Optional[str]
    content_model: str
    format: str
    sha1: Optional[str]

    @abstractmethod
    @classmethod
    def _base_dir(cls, data_dir: Path) -> Path:
        pass

    @classmethod
    def path(
        cls, data_dir: Path, dump_name: str, page_id: int, revision_id: int
    ) -> Path:
        return (
            cls._base_dir(data_dir)
            / dump_name
            / str(page_id)
            / (str(revision_id) + ".json.gz")
        )

    def save_to_file(self, data_dir: Path, dump_name: str) -> None:
        path = self.path(data_dir, dump_name, self.page_id, self.revision_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="UTF-8") as fout:
            fout.write(self.json(indent=2) + "\n")

    @classmethod
    def load_from_file(
        cls: Type[_T_WikidataRevision],
        data_dir: Path,
        dump_name: str,
        page_id: int,
        revision_id: int,
    ) -> _T_WikidataRevision:
        path = cls.path(data_dir, dump_name, page_id, revision_id)
        with gzip.open(path, "rt", encoding="UTF-8") as fin:
            return cls.parse_raw(fin.read())

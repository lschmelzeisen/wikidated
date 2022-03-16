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

import re
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from sys import maxsize
from typing import Any, Iterable, Iterator, Optional, Tuple, Union, overload

from tqdm import tqdm  # type: ignore
from typing_extensions import Final

from wikidated._utils import RangeMap, SevenZipArchive
from wikidated.wikidated_entity_streams import (
    WikidatedEntityStreams,
    WikidatedEntityStreamsFile,
)
from wikidated.wikidated_revision import WikidatedRevision

_LOGGER = getLogger(__name__)


class WikidatedSortedEntityStreamsFile:
    def __init__(self, archive_path: Path, page_ids: range) -> None:
        if not archive_path.exists():
            raise FileNotFoundError(archive_path)
        self.archive_path: Final = archive_path
        self.page_ids: Final = page_ids

    def iter_revisions(
        self,
        *,
        min_revision_id: Optional[int] = None,
        max_revision_id: Optional[int] = None,
        min_timestamp: Optional[datetime] = None,
        max_timestamp: Optional[datetime] = None,
    ) -> Iterator[WikidatedRevision]:
        archive = SevenZipArchive(self.archive_path)
        min_revision_id_ = min_revision_id or -maxsize
        max_revision_id_ = max_revision_id or maxsize
        min_timestamp_ = min_timestamp or datetime.min
        max_timestamp_ = max_timestamp or datetime.max
        if not min_timestamp_.tzinfo:
            min_timestamp_ = min_timestamp_.replace(tzinfo=timezone.utc)
        if not max_timestamp_.tzinfo:
            max_timestamp_ = max_timestamp_.replace(tzinfo=timezone.utc)
        with archive.read() as fd:
            for line in fd:
                revision = WikidatedRevision.parse_raw(line)
                if (
                    revision.revision_id < min_revision_id_
                    or revision.timestamp < min_timestamp_
                ):
                    continue
                if (
                    revision.revision_id > max_revision_id_
                    or revision.timestamp > max_timestamp_
                ):
                    break
                yield revision

    @classmethod
    def archive_path_glob(cls, dataset_dir: Path) -> str:
        return f"{dataset_dir.name}-sorted-entity-streams-p*-p*.7z"

    @classmethod
    def _make_archive_path(cls, dataset_dir: Path, page_ids: range) -> Path:
        return dataset_dir / (
            f"{dataset_dir.name}-sorted-entity-streams"
            f"-p{page_ids.start}-p{page_ids.stop - 1}.7z"
        )

    @classmethod
    def _parse_archive_path(cls, path: Path) -> Tuple[Path, range]:
        match = re.match(
            r"^(?P<dataset_dir_name>.+)-sorted-entity-streams"
            r"-p(?P<min_page_id>\d+)-p(?P<max_page_id>\d+).7z$",
            path.name,
        )
        assert match

        dataset_dir = path.parent.resolve()
        assert dataset_dir.name == match["dataset_dir_name"]
        page_ids = range(int(match["min_page_id"]), int(match["max_page_id"]) + 1)
        return dataset_dir, page_ids

    @classmethod
    def load(cls, path: Path) -> WikidatedSortedEntityStreamsFile:
        _, page_ids = cls._parse_archive_path(path)
        return WikidatedSortedEntityStreamsFile(path, page_ids)

    @classmethod
    def build(
        cls, dataset_dir: Path, entity_streams_file: WikidatedEntityStreamsFile
    ) -> WikidatedSortedEntityStreamsFile:
        archive_path = cls._make_archive_path(dataset_dir, entity_streams_file.page_ids)
        if archive_path.exists():
            _LOGGER.debug(
                f"Sorted entity streams file '{archive_path.name}' already exists, "
                f"skipping building."
            )
        else:
            _LOGGER.debug(f"Building sorted entity streams file {archive_path.name}.")
            tmp_path = archive_path.parent / ("tmp." + archive_path.name)
            revisions = list(entity_streams_file.iter_revisions())
            revisions.sort(key=lambda rev: rev.revision_id)
            with SevenZipArchive(tmp_path).write() as fd:
                for revision in revisions:
                    fd.write(revision.json() + "\n")
            tmp_path.rename(archive_path)
            _LOGGER.debug(
                f"Done building sorted entity streams file {archive_path.name}."
            )

        return WikidatedSortedEntityStreamsFile(
            archive_path, entity_streams_file.page_ids
        )


class WikidatedSortedEntityStreams:
    def __init__(self, files_by_page_ids: RangeMap[WikidatedSortedEntityStreamsFile]):
        self._files_by_page_ids = files_by_page_ids

    def __len__(self) -> int:
        return len(self._files_by_page_ids)

    def __iter__(self) -> Iterator[WikidatedSortedEntityStreamsFile]:
        return iter(self._files_by_page_ids.values())

    @overload
    def __getitem__(self, key: int) -> WikidatedSortedEntityStreamsFile:
        ...

    @overload
    def __getitem__(self, key: slice) -> Iterable[WikidatedSortedEntityStreamsFile]:
        ...

    @overload
    def __getitem__(self, key: object) -> Any:  # NoReturn doesn't work here.
        ...

    def __getitem__(
        self, key: object
    ) -> Union[
        WikidatedSortedEntityStreamsFile, Iterable[WikidatedSortedEntityStreamsFile]
    ]:
        if isinstance(key, int) or isinstance(key, slice):
            return self._files_by_page_ids[key]
        else:
            raise TypeError("key needs to be of type int or slice.")

    @classmethod
    def load(cls, dataset_dir: Path) -> WikidatedSortedEntityStreams:
        _LOGGER.debug(f"Loading sorted entity streams for dataset {dataset_dir.name}.")
        files_by_page_ids = RangeMap[WikidatedSortedEntityStreamsFile]()
        for path in dataset_dir.glob(
            WikidatedSortedEntityStreamsFile.archive_path_glob(dataset_dir)
        ):
            file = WikidatedSortedEntityStreamsFile.load(path)
            files_by_page_ids[file.page_ids] = file
        _LOGGER.debug(
            f"Done loading sorted entity streams for dataset {dataset_dir.name}."
        )
        return WikidatedSortedEntityStreams(files_by_page_ids)

    @classmethod
    def build(
        cls, dataset_dir: Path, entity_streams: WikidatedEntityStreams
    ) -> WikidatedSortedEntityStreams:
        _LOGGER.debug(f"Building sorted entity streams for dataset {dataset_dir.name}.")
        files_by_page_ids = RangeMap[WikidatedSortedEntityStreamsFile]()
        for entity_streams_file in tqdm(entity_streams, desc="Sorted Entity Streams"):
            file = WikidatedSortedEntityStreamsFile.build(
                dataset_dir, entity_streams_file
            )
            files_by_page_ids[file.page_ids] = file
        _LOGGER.debug(
            f"Done building sorted entity streams for dataset {dataset_dir.name}."
        )
        return WikidatedSortedEntityStreams(files_by_page_ids)

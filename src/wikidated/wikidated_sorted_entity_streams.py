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

from logging import getLogger
from pathlib import Path
from typing import Iterable, Iterator, Union

from tqdm import tqdm

from wikidated._utils import RangeMap, SevenZipArchive
from wikidated.wikidated_entity_streams import (
    WikidatedEntityStreamsManager,
    WikidatedEntityStreamsPart,
)
from wikidated.wikidated_revision import WikidatedRevision

_LOGGER = getLogger(__name__)


class WikidatedSortedEntityStreamsPart:
    def __init__(self, dataset_dir: Path, page_id_range: range) -> None:
        self._path = dataset_dir / (
            f"{dataset_dir.name}-sorted-entity-streams"
            f"-p{page_id_range.start}-p{page_id_range.stop - 1}.7z"
        )
        self._page_id_range = page_id_range

    @property
    def path(self) -> Path:
        return self._path

    @property
    def page_id_range(self) -> range:
        return self._page_id_range

    def iter_revisions(self) -> Iterator[WikidatedRevision]:
        assert self._path.exists()
        archive = SevenZipArchive(self._path)
        with archive.read() as fd:
            for line in fd:
                yield WikidatedRevision.parse_raw(line)

    def build(self, entity_streams_part: WikidatedEntityStreamsPart) -> None:
        if self._path.exists():
            _LOGGER.debug(f"File '{self._path}' already exists, skipping building.")

        assert self.page_id_range == entity_streams_part.page_id_range

        revisions = list(entity_streams_part.iter_revisions())
        revisions.sort(key=lambda revision: revision.revision.revision_id)
        with SevenZipArchive(self._path).write() as fd:
            for revision in revisions:
                fd.write(revision.json() + "\n")


class WikidatedSortedEntityStreamsManager:
    def __init__(
        self, dataset_dir: Path, page_id_ranges: Union[Iterator[range], Iterable[range]]
    ):
        self._parts = RangeMap[WikidatedSortedEntityStreamsPart]()
        for page_id_range in page_id_ranges:
            self._parts[page_id_range] = WikidatedSortedEntityStreamsPart(
                dataset_dir, page_id_range
            )

    def build(self, entity_streams_manager: WikidatedEntityStreamsManager) -> None:
        for page_id_range, part in tqdm(
            self._parts.items(), desc="Sorted Entity Streams"
        ):
            part.build(entity_streams_manager._parts[page_id_range])

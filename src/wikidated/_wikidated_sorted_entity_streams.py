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

from tqdm import tqdm  # type: ignore

from wikidated._utils import RangeMap, SevenZipArchive
from wikidated.wikidated_entity_streams import (
    WikidatedEntityStreamsFile,
    WikidatedEntityStreamsManager,
)
from wikidated.wikidated_revision import WikidatedRevision

_LOGGER = getLogger(__name__)


class WikidatedSortedEntityStreamsFile:
    def __init__(self, dataset_dir: Path, page_ids: range) -> None:
        self.path = dataset_dir / (
            f"{dataset_dir.name}-sorted-entity-streams"
            f"-p{page_ids.start}-p{page_ids.stop - 1}.7z"
        )
        self.page_ids = page_ids

    def iter_revisions(self) -> Iterator[WikidatedRevision]:
        assert self.path.exists()
        archive = SevenZipArchive(self.path)
        with archive.read() as fd:
            for line in fd:
                yield WikidatedRevision.parse_raw(line)

    def build(self, entity_streams_part: WikidatedEntityStreamsFile) -> None:
        if self.path.exists():
            _LOGGER.debug(f"File '{self.path}' already exists, skipping building.")
            return

        assert self.page_ids == entity_streams_part.page_ids

        revisions = list(entity_streams_part.iter_revisions())
        revisions.sort(key=lambda revision: revision.revision.revision_id)
        with SevenZipArchive(self.path).write() as fd:
            for revision in revisions:
                fd.write(revision.json() + "\n")


class WikidatedSortedEntityStreamsManager:
    def __init__(
        self,
        dataset_dir: Path,
        page_ids_stream: Union[Iterator[range], Iterable[range]],
    ):
        self._files = RangeMap[WikidatedSortedEntityStreamsFile]()
        for page_ids in page_ids_stream:
            self._files[page_ids] = WikidatedSortedEntityStreamsFile(
                dataset_dir, page_ids
            )

    def build(self, entity_streams_manager: WikidatedEntityStreamsManager) -> None:
        for page_ids, part in tqdm(self._files.items(), desc="Sorted Entity Streams"):
            part.build(entity_streams_manager._files[page_ids])

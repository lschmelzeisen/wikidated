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

from pathlib import Path
from typing import Iterator, Optional

from wikidated.wikidata import WikidataDump
from wikidated.wikidated_entity_streams import WikidatedEntityStreamsManager
from wikidated.wikidated_global_stream import WikidatedGlobalStreamManager
from wikidated.wikidated_revision import WikidatedRevision
from wikidated.wikidated_sorted_entity_streams import (
    WikidatedSortedEntityStreamsManager,
)


class WikidatedDataset:
    def __init__(
        self,
        dataset_dir: Path,
        jars_dir: Path,
        wikidata_dump: WikidataDump,
    ) -> None:
        self._wikidata_dump = wikidata_dump
        self._entity_streams_manager = WikidatedEntityStreamsManager(
            dataset_dir, jars_dir
        )
        self._sorted_entity_streams_manager = WikidatedSortedEntityStreamsManager(
            dataset_dir
        )
        self._global_stream_manager = WikidatedGlobalStreamManager(dataset_dir)

    def download(self) -> None:
        raise NotImplementedError()  # TODO

    def build(self, *, max_workers: Optional[int] = 4) -> None:
        self._entity_streams_manager.build(
            self._wikidata_dump.sites_table,
            self._wikidata_dump.pages_meta_history,
            max_workers=max_workers,
        )
        self._sorted_entity_streams_manager.build(self._entity_streams_manager)
        self._global_stream_manager.build(
            self._sorted_entity_streams_manager, self._wikidata_dump.version
        )

    # TODO: rethink what kind of accessor methods might be used here in practice.

    def iter_revisions(
        self, entity_page_id: Optional[int] = None
    ) -> Iterator[WikidatedRevision]:
        raise NotImplementedError()

    def iter_page_ids(self) -> Iterator[int]:
        raise NotImplementedError()

    def entity_streams(self) -> None:
        raise NotImplementedError()

    def global_stream(self) -> None:
        raise NotImplementedError()

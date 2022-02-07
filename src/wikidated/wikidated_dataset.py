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

from logging import getLogger
from pathlib import Path
from typing import Optional

from typing_extensions import Final

from wikidated.wikidata import WikidataDump
from wikidated.wikidated_entity_streams import WikidatedEntityStreams
from wikidated.wikidated_global_stream import WikidatedGlobalStream
from wikidated.wikidated_sorted_entity_streams import WikidatedSortedEntityStreams

_LOGGER = getLogger(__name__)


class WikidatedDataset:
    def __init__(
        self,
        entity_streams: Optional[WikidatedEntityStreams],
        sorted_entity_streams: Optional[WikidatedSortedEntityStreams],
        global_stream: Optional[WikidatedGlobalStream],
    ) -> None:
        self.entity_streams: Final = entity_streams
        self.sorted_entity_streams: Final = sorted_entity_streams
        self.global_stream: Final = global_stream
        pass

    @classmethod
    def download(cls) -> WikidatedDataset:
        raise NotImplementedError()  # TODO

    @classmethod
    def load(cls, dataset_dir: Path) -> WikidatedDataset:
        _LOGGER.debug(f"Loading dataset {dataset_dir.name}.")
        entity_streams = WikidatedEntityStreams.load(dataset_dir)
        sorted_entity_streams = WikidatedSortedEntityStreams.load(dataset_dir)
        global_stream = WikidatedGlobalStream.load(dataset_dir)
        _LOGGER.debug(f"Done loading dataset {dataset_dir.name}.")
        return WikidatedDataset(entity_streams, sorted_entity_streams, global_stream)

    @classmethod
    def build(
        cls,
        dataset_dir: Path,
        jars_dir: Path,
        wikidata_dump: WikidataDump,
        *,
        max_workers: Optional[int] = 4,
    ) -> WikidatedDataset:
        _LOGGER.info(f"Building dataset {dataset_dir.name} with {max_workers} workers.")
        entity_streams = WikidatedEntityStreams.build(
            dataset_dir,
            jars_dir,
            wikidata_dump.sites_table,
            wikidata_dump.pages_meta_history,
            max_workers=max_workers,
        )
        sorted_entity_streams = WikidatedSortedEntityStreams.build(
            dataset_dir, entity_streams
        )
        global_stream = WikidatedGlobalStream.build(
            dataset_dir, sorted_entity_streams, wikidata_dump.version
        )
        _LOGGER.info(f"Done building dataset {dataset_dir.name}.")
        return WikidatedDataset(entity_streams, sorted_entity_streams, global_stream)

    # TODO: rethink what kind of accessor methods might be used here in practice.

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
from datetime import date, datetime
from logging import getLogger
from pathlib import Path
from sys import maxsize
from typing import Iterator, Optional

from typing_extensions import Final

from wikidated.wikidata import WikidataDump
from wikidated.wikidated_entity_streams import WikidatedEntityStreams
from wikidated.wikidated_global_stream import WikidatedGlobalStream
from wikidated.wikidated_revision import WikidatedRevision
from wikidated.wikidated_sorted_entity_streams import WikidatedSortedEntityStreams

_LOGGER = getLogger(__name__)


class WikidatedDataset:
    def __init__(
        self,
        dataset_dir: Path,
        dump_version: Optional[date],
        entity_streams: WikidatedEntityStreams,
        sorted_entity_streams: WikidatedSortedEntityStreams,
        global_stream: WikidatedGlobalStream,
    ) -> None:
        self.dataset_dir: Final = dataset_dir
        self.dump_version: Final = dump_version
        self.entity_streams: Final = entity_streams
        self.sorted_entity_streams: Final = sorted_entity_streams
        self.global_stream: Final = global_stream

    @property
    def dataset_version(self) -> str:
        if self.dump_version:
            return f"custom-{self.dump_version:%4Y%2m%2d}"
        else:
            return "custom-unknown"

    @classmethod
    def download(cls) -> WikidatedDataset:
        raise NotImplementedError()  # TODO

    @classmethod
    def load(cls, dataset_dir: Path) -> WikidatedDataset:
        _LOGGER.debug(f"Loading dataset {dataset_dir.name}.")

        match = re.match(
            r".*(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2}).*", dataset_dir.name
        )
        dump_version = (
            date(
                year=int(match["year"]),
                month=int(match["month"]),
                day=int(match["day"]),
            )
            if match
            else None
        )

        entity_streams = WikidatedEntityStreams.load(dataset_dir)
        sorted_entity_streams = WikidatedSortedEntityStreams.load(dataset_dir)
        global_stream = WikidatedGlobalStream.load(dataset_dir)
        _LOGGER.debug(f"Done loading dataset {dataset_dir.name}.")
        return WikidatedDataset(
            dataset_dir,
            dump_version,
            entity_streams,
            sorted_entity_streams,
            global_stream,
        )

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
        return WikidatedDataset(
            dataset_dir,
            wikidata_dump.version,
            entity_streams,
            sorted_entity_streams,
            global_stream,
        )

    def iter_revisions(
        self,
        page_id: Optional[int] = None,
        *,
        min_page_id: Optional[int] = None,
        max_page_id: Optional[int] = None,
        min_revision_id: Optional[int] = None,
        max_revision_id: Optional[int] = None,
        min_timestamp: Optional[datetime] = None,
        max_timestamp: Optional[datetime] = None,
    ) -> Iterator[WikidatedRevision]:
        # Will not iterate pages with increasing page_id for entity streams.
        if page_id is not None:
            if min_page_id is not None or max_page_id is not None:
                raise ValueError(
                    "Dot not use page_id together with min_page_id or max_page_id."
                )
            try:
                entity_streams_file = self.entity_streams[page_id]
                yield from entity_streams_file.iter_revisions(
                    page_id,
                    min_revision_id=min_revision_id,
                    max_revision_id=max_revision_id,
                    min_timestamp=min_timestamp,
                    max_timestamp=max_timestamp,
                )
            except KeyError:
                yield from ()
        elif min_page_id is not None or max_page_id is not None:
            entity_streams_files = self.entity_streams[
                (min_page_id or 0) : (max_page_id or maxsize)
            ]
            for entity_streams_file in entity_streams_files:
                yield from entity_streams_file.iter_revisions(
                    min_revision_id=min_revision_id,
                    max_revision_id=max_revision_id,
                    min_timestamp=min_timestamp,
                    max_timestamp=max_timestamp,
                )
        else:
            global_stream_files_by_revision_ids = self.global_stream[
                min_revision_id:max_revision_id
            ]
            # We ignore typing here because for mypy slice indices must be integers.
            global_stream_files_by_timestamps = self.global_stream[
                (min_timestamp and min_timestamp.date()) : (  # type: ignore
                    max_timestamp and max_timestamp.date()  # type: ignore
                )
            ]
            global_stream_files = sorted(
                set(global_stream_files_by_revision_ids)
                & set(global_stream_files_by_timestamps),
                key=lambda f: f.month,
            )
            for global_stream_file in global_stream_files:
                yield from global_stream_file.iter_revisions(
                    min_revision_id=min_revision_id,
                    max_revision_id=max_revision_id,
                    min_timestamp=min_timestamp,
                    max_timestamp=max_timestamp,
                )

    def iter_page_ids(self) -> Iterator[int]:
        for entity_streams_file in self.entity_streams:
            yield from entity_streams_file.iter_page_ids()

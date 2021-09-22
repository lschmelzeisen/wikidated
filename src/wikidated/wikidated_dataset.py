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
from typing import Iterator, Optional, Sequence

from wikidated._utils import JvmManager
from wikidated.wikidata import (
    WikidataDump,
    WikidataDumpPagesMetaHistory,
    WikidataRdfTriple,
    WikidataRevisionBase,
)


class WikidatedRevision(WikidataRevisionBase):
    triple_deletions: Sequence[WikidataRdfTriple]
    triple_additions: Sequence[WikidataRdfTriple]
    triple_deletions_sample: Sequence[float]
    triple_additions_sample: Sequence[float]


class WikidatedDataset:
    def __init__(self, data_dir: Path, wikidata_dump: WikidataDump) -> None:
        self._data_dir = data_dir
        self._dataset_dir = data_dir / f"wikidated-custom-{wikidata_dump.version}"

        self._partial_entity_streams = []
        self._partial_global_stream = []
        for pages_meta_history in wikidata_dump.pages_meta_history():
            partial_entity_streams = _WikidatedEntityStreamsPartial(
                self._dataset_dir, pages_meta_history
            )
            partial_global_stream = _WikidatedGlobalStreamPartial(
                self._dataset_dir, partial_entity_streams
            )
            self._partial_entity_streams.append(partial_entity_streams)
            self._partial_global_stream.append(partial_global_stream)
        self._merged_entity_streams = _WikidataEntityStreamsMerged(
            self._dataset_dir, self._partial_entity_streams
        )
        self._merged_global_stream = _WikidatedGlobalStreamMerged(
            self._dataset_dir, self._partial_global_stream
        )

    def download(
        self, *, entity_streams: bool = True, global_stream: bool = True
    ) -> None:
        raise NotImplementedError()  # TODO

    def build(self, *, entity_streams: bool = True, global_stream: bool = True) -> None:
        raise NotImplementedError()  # TODO

    def iter_revisions(
        self, page_id: Optional[int] = None, sample_rate: Optional[float] = None
    ) -> Iterator[WikidatedRevision]:
        if page_id is not None:
            return self._merged_entity_streams.iter_revisions(
                page_id=page_id, sample_rate=sample_rate
            )
        else:
            return self._merged_global_stream.iter_revisions(sample_rate=sample_rate)

    def iter_page_ids(self) -> Iterator[int]:
        return self._merged_entity_streams.iter_page_ids()

    # TODO: rethink what kind of accessor methods might be used here in pracitce.

    def entity_streams(self) -> WikidatedEntityStreams:
        return self._merged_entity_streams

    def global_stream(self) -> WikidatedGlobalStream:
        return self._merged_global_stream


class _WikidatedStreamFile:
    def __init__(self, path: Path, page_id_range: Optional[range]) -> None:
        self._path = path
        self._page_id_range = page_id_range

    @property
    def path(self) -> Path:
        return self._path

    @property
    def page_id_range(self) -> Optional[range]:
        return self._page_id_range


class WikidatedEntityStreams(_WikidatedStreamFile):
    def build(self, jvm_manager: JvmManager) -> None:
        raise NotImplementedError()

    def iter_revisions(
        self, page_id: int, sample_rate: Optional[float] = None
    ) -> Iterator[WikidatedRevision]:
        raise NotImplementedError()  # TODO

    def iter_page_ids(self) -> Iterator[int]:
        raise NotImplementedError()  # TODO


class _WikidatedEntityStreamsPartial(WikidatedEntityStreams):
    def __init__(
        self,
        dataset_dir: Path,
        pages_meta_history: WikidataDumpPagesMetaHistory,
    ) -> None:
        super().__init__(
            dataset_dir
            / (
                f"entity-streams"
                f"-p{pages_meta_history.page_id_range.start}"
                f"-p{pages_meta_history.page_id_range.stop-1}.7z"
            ),
            pages_meta_history.page_id_range,
        )

    def build(self, jvm_manager: JvmManager) -> None:
        raise NotImplementedError()  # TODO


class _WikidataEntityStreamsMerged(WikidatedEntityStreams):
    def __init__(
        self,
        dataset_dir: Path,
        entity_streams_partials: Sequence[_WikidatedEntityStreamsPartial],
    ) -> None:
        super().__init__(dataset_dir / "entity-streams.7z", None)

    def build(self, jvm_manager: JvmManager) -> None:
        raise NotImplementedError()  # TODO


class WikidatedGlobalStream(_WikidatedStreamFile):
    def build(self) -> None:
        raise NotImplementedError()

    def iter_revisions(
        self, sample_rate: Optional[float] = None
    ) -> Iterator[WikidatedRevision]:
        raise NotImplementedError()  # TODO


class _WikidatedGlobalStreamPartial(WikidatedGlobalStream):
    def __init__(
        self, dataset_dir: Path, entity_stream: WikidatedEntityStreams
    ) -> None:
        assert entity_stream.page_id_range is not None
        super().__init__(
            dataset_dir
            / (
                "global-stream"
                f"-p{entity_stream.page_id_range.start}"
                f"-p{entity_stream.page_id_range.stop - 1}.7z"
            ),
            entity_stream.page_id_range,
        )

    def build(self) -> None:
        raise NotImplementedError()  # TODO


class _WikidatedGlobalStreamMerged(WikidatedGlobalStream):
    def __init__(
        self,
        dataset_dir: Path,
        global_stream_partials: Sequence[_WikidatedGlobalStreamPartial],
    ) -> None:
        super().__init__(dataset_dir / "global-stream.7z", None)

    def build(self) -> None:
        raise NotImplementedError()  # TODO

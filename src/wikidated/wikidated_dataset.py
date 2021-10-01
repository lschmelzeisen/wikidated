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

from itertools import groupby
from logging import getLogger
from pathlib import Path
from typing import Iterator, MutableSet, Optional, Sequence, Tuple

from wikidated._utils import SevenZipArchive
from wikidated.wikidata import (
    WikidataDump,
    WikidataDumpPagesMetaHistory,
    WikidataEntityMeta,
    WikidataRawRevision,
    WikidataRdfConversionError,
    WikidataRdfConverter,
    WikidataRdfTriple,
    WikidataRevisionBase,
)

_LOGGER = getLogger(__name__)


class WikidatedRevision(WikidataRevisionBase):
    triple_deletions: Sequence[WikidataRdfTriple]
    triple_additions: Sequence[WikidataRdfTriple]


class WikidatedDataset:
    def __init__(self, data_dir: Path, wikidata_dump: WikidataDump) -> None:
        self._data_dir = data_dir
        self._dataset_dir = data_dir / f"wikidated-custom-{wikidata_dump.version}"

        self._entity_streams_partials = []
        self._global_stream_partials = []
        for pages_meta_history in wikidata_dump.pages_meta_history()[:4]:
            entity_streams_partial = _WikidatedEntityStreamsPartial(
                self._dataset_dir, pages_meta_history
            )
            self._entity_streams_partials.append(entity_streams_partial)

            global_stream_partial = _WikidatedGlobalStreamPartial(
                self._dataset_dir, entity_streams_partial
            )
            self._global_stream_partials.append(global_stream_partial)

        self._entity_streams_merged = _WikidataEntityStreamsMerged(
            self._dataset_dir, self._entity_streams_partials
        )
        self._global_stream_merged = _WikidatedGlobalStreamMerged(
            self._dataset_dir, self._global_stream_partials
        )

    def download(
        self, *, entity_streams: bool = True, global_stream: bool = True
    ) -> None:
        raise NotImplementedError()  # TODO

    def build(self, *, entity_streams: bool = True, global_stream: bool = True) -> None:
        raise NotImplementedError()  # TODO

    def iter_revisions(
        self, entity_page_id: Optional[int] = None
    ) -> Iterator[WikidatedRevision]:
        if entity_page_id is not None:
            return self._entity_streams_merged.iter_revisions(
                entity_page_id=entity_page_id
            )
        else:
            return self._global_stream_merged.iter_revisions()

    def iter_page_ids(self) -> Iterator[int]:
        return self._entity_streams_merged.iter_page_ids()

    # TODO: rethink what kind of accessor methods might be used here in practice.

    def entity_streams(self) -> WikidatedEntityStreams:
        return self._entity_streams_merged

    def global_stream(self) -> WikidatedGlobalStream:
        return self._global_stream_merged


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
    def build(self, rdf_converter: WikidataRdfConverter) -> None:
        raise NotImplementedError()

    def iter_revisions(self, entity_page_id: int) -> Iterator[WikidatedRevision]:
        entity_streams_archive = SevenZipArchive(self._path)
        with entity_streams_archive.read(
            self._entity_file_name_from_page_id(entity_page_id)
        ) as fd:
            for line in fd:
                yield WikidatedRevision.parse_raw(line)

    def iter_page_ids(self) -> Iterator[int]:
        assert self._path.exists()
        entity_streams_archive = SevenZipArchive(self._path)
        for entity_file_name in entity_streams_archive.iter_file_names():
            yield self._entity_page_id_from_file_name(entity_file_name)

    @classmethod
    def _entity_file_name_from_page_id(cls, entity_page_id: int) -> Path:
        return Path(f"{entity_page_id}.jsonl")

    @classmethod
    def _entity_page_id_from_file_name(cls, entity_file_name: Path) -> int:
        return int(entity_file_name.name[: -len(".jsonl")])


class _WikidatedEntityStreamsPartial(WikidatedEntityStreams):
    def __init__(
        self,
        dataset_dir: Path,
        pages_meta_history: WikidataDumpPagesMetaHistory,
    ) -> None:
        super().__init__(
            dataset_dir
            / (
                f"{dataset_dir.name}-entity-streams"
                f"-p{pages_meta_history.page_id_range.start}"
                f"-p{pages_meta_history.page_id_range.stop-1}.7z"
            ),
            pages_meta_history.page_id_range,
        )
        self._pages_meta_history = pages_meta_history

    def build(self, rdf_converter: WikidataRdfConverter) -> None:
        if self._path.exists():
            _LOGGER.debug(f"File '{self._path}' already exists, skipping building.")
            return

        tmp_path = self._path.parent / ("tmp." + self._path.name)
        tmp_path.parent.mkdir(exist_ok=True, parents=True)
        entity_streams_archive = SevenZipArchive(tmp_path)

        for entity_meta, revisions in self._iter_revisions_grouped_per_entity():
            with entity_streams_archive.write(
                self._entity_file_name_from_page_id(entity_meta.page_id)
            ) as fd:
                for revision in self._iter_wikidated_revisions(
                    revisions, rdf_converter
                ):
                    fd.write(revision.json() + "\n")

        tmp_path.rename(self._path)

    def _iter_revisions_grouped_per_entity(
        self,
    ) -> Iterator[Tuple[WikidataEntityMeta, Iterator[WikidataRawRevision]]]:
        return groupby(self._pages_meta_history, lambda revision: revision.entity)

    @classmethod
    def _iter_wikidated_revisions(
        cls,
        revisions: Iterator[WikidataRawRevision],
        rdf_converter: WikidataRdfConverter,
    ) -> Iterator[WikidatedRevision]:
        state: MutableSet[WikidataRdfTriple] = set()

        for revision in revisions:
            try:
                rdf_revision = rdf_converter(revision)
            except WikidataRdfConversionError:
                _LOGGER.exception("RDF conversion error.")
                continue

            triples_set = set(rdf_revision.triples)
            triple_deletions = sorted(state - triples_set)
            triple_additions = sorted(triples_set - state)
            state = triples_set

            yield WikidatedRevision(
                entity=revision.entity,
                revision=revision.revision,
                triple_deletions=triple_deletions,
                triple_additions=triple_additions,
            )


class _WikidataEntityStreamsMerged(WikidatedEntityStreams):
    def __init__(
        self,
        dataset_dir: Path,
        entity_streams_partials: Sequence[_WikidatedEntityStreamsPartial],
    ) -> None:
        super().__init__(dataset_dir / f"{dataset_dir.name}-entity-streams.7z", None)
        self._entity_streams_partials = entity_streams_partials

    def build(self, rdf_converter: WikidataRdfConverter) -> None:
        raise NotImplementedError()  # TODO


class WikidatedGlobalStream(_WikidatedStreamFile):
    def build(self) -> None:
        raise NotImplementedError()

    def iter_revisions(self) -> Iterator[WikidatedRevision]:
        raise NotImplementedError()  # TODO


class _WikidatedGlobalStreamPartial(WikidatedGlobalStream):
    def __init__(
        self, dataset_dir: Path, entity_stream: WikidatedEntityStreams
    ) -> None:
        assert entity_stream.page_id_range is not None
        super().__init__(
            dataset_dir
            / (
                f"{dataset_dir.name}-global-stream"
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
        super().__init__(dataset_dir / f"{dataset_dir.name}-global-stream.7z", None)

    def build(self) -> None:
        raise NotImplementedError()  # TODO

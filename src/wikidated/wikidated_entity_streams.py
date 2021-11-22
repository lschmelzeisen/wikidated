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

from itertools import chain, groupby
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from typing import Iterable, Iterator, Mapping, MutableSet, Optional, Tuple, Union, cast

from wikidated._utils import (
    JvmManager,
    ParallelizeUpdateProgressFunc,
    RangeMap,
    SevenZipArchive,
    parallelize,
)
from wikidated.wikidata import (
    WikidataDumpPagesMetaHistory,
    WikidataDumpSitesTable,
    WikidataEntityMeta,
    WikidataRawRevision,
    WikidataRdfConversionError,
    WikidataRdfConverter,
    WikidataRdfTriple,
)
from wikidated.wikidated_revision import WikidatedRevision

_LOGGER = getLogger(__name__)


class WikidatedEntityStreamsFile:
    def __init__(self, dataset_dir: Path, page_ids: range) -> None:
        self.path = dataset_dir / (
            f"{dataset_dir.name}-entity-streams"
            f"-p{page_ids.start}-p{page_ids.stop - 1}.7z"
        )
        self.page_ids = page_ids

    def iter_revisions(
        self, page_id: Optional[int] = None
    ) -> Iterator[WikidatedRevision]:
        assert self.path.exists()
        archive = SevenZipArchive(self.path)
        with archive.read(
            self._file_name_from_page_id(page_id) if page_id else None
        ) as fd:
            for line in fd:
                yield WikidatedRevision.parse_raw(line)

    def iter_page_ids(self) -> Iterator[int]:
        assert self.path.exists()
        entity_streams_archive = SevenZipArchive(self.path)
        for entity_file_name in entity_streams_archive.iter_file_names():
            yield self._page_id_from_file_name(entity_file_name)

    @classmethod
    def _file_name_from_page_id(cls, page_id: int) -> Path:
        return Path(f"{page_id}.jsonl")

    @classmethod
    def _page_id_from_file_name(cls, file_name: Path) -> int:
        return int(file_name.name[: -len(".jsonl")])

    def build(
        self,
        pages_meta_history: WikidataDumpPagesMetaHistory,
        rdf_converter: WikidataRdfConverter,
    ) -> Iterator[WikidatedRevision]:
        if self.path.exists():
            _LOGGER.debug(f"File '{self.path}' already exists, skipping building.")
            return iter([])

        assert self.page_ids == pages_meta_history.page_ids

        tmp_dir = self.path.parent / ("tmp." + self.path.name)
        if tmp_dir.exists():
            # TODO: If desired, one could rewrite this for better recovery from
            #  crashes. That is, we could reuse entities already processed in tmp_dir
            #  and skip those when iterating.
            rmtree(tmp_dir)
        tmp_dir.mkdir(exist_ok=True, parents=True)

        for entity_meta, revisions in self._iter_revisions_grouped_per_entity(
            pages_meta_history
        ):
            wikidated_revisions = self._iter_wikidated_revisions(
                revisions, rdf_converter
            )

            # In the following we check if we can access the first element in the
            # iterable. If it does not exist, the current page  does not describe a
            # Wikidata entity (e.g., it could be a wikitext page). Only if it exists, we
            # add a file to the output archive.

            try:
                first_wikidated_revision = next(wikidated_revisions)
            except StopIteration:
                continue

            with (tmp_dir / self._file_name_from_page_id(entity_meta.page_id)).open(
                "w", encoding="UTF-8"
            ) as fd:
                for wikidated_revision in chain(
                    (first_wikidated_revision,), wikidated_revisions
                ):
                    fd.write(wikidated_revision.json() + "\n")
                    yield wikidated_revision

        SevenZipArchive.from_dir(tmp_dir, self.path)
        rmtree(tmp_dir)

    @classmethod
    def _iter_revisions_grouped_per_entity(
        cls, pages_meta_history: WikidataDumpPagesMetaHistory
    ) -> Iterator[Tuple[WikidataEntityMeta, Iterator[WikidataRawRevision]]]:
        return groupby(
            pages_meta_history.iter_revisions(display_progress_bar=False),
            lambda revision: revision.entity,
        )

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


class WikidatedEntityStreamsManager:
    def __init__(
        self,
        dataset_dir: Path,
        jars_dir: Path,
        page_ids_stream: Union[Iterator[range], Iterable[range]],
    ) -> None:
        self._files = RangeMap[WikidatedEntityStreamsFile]()
        for page_ids in page_ids_stream:
            self._files[page_ids] = WikidatedEntityStreamsFile(dataset_dir, page_ids)
        self._jars_dir = jars_dir
        self._jvm_manager: Optional[JvmManager] = None
        self._sites_table: Optional[WikidataDumpSitesTable] = None

    def build(
        self,
        sites_table: WikidataDumpSitesTable,
        pages_meta_history: RangeMap[WikidataDumpPagesMetaHistory],
        max_workers: Optional[int] = 4,
    ) -> None:
        self._sites_table = sites_table
        for _ in parallelize(
            self._build_part,
            self._files.values(),
            num_arguments=len(self._files),
            extra_arguments={"pages_meta_history": pages_meta_history},
            init_worker_func=self._init_worker_with_rdf_converter,
            exit_worker_func=self._exit_worker_with_rdf_converter,
            max_workers=max_workers,
            progress_bar_desc="Entity Streams",
        ):
            pass

    @classmethod
    def _build_part(
        cls,
        argument: WikidatedEntityStreamsFile,
        update_progress: ParallelizeUpdateProgressFunc,
        **extra_arguments: object,
    ) -> None:
        name = argument.path.name
        page_ids = argument.page_ids
        pages_meta_history = cast(
            RangeMap[WikidataDumpPagesMetaHistory],
            extra_arguments["pages_meta_history"],
        )
        assert isinstance(pages_meta_history, RangeMap)
        rdf_converter = extra_arguments["rdf_converter"]
        assert isinstance(rdf_converter, WikidataRdfConverter)

        progress_current = 0
        progress_total = len(page_ids)
        update_progress(name, progress_current, progress_total)
        for revision in argument.build(pages_meta_history[page_ids], rdf_converter):
            # TODO: count number of processed revisions and exceptions?
            if progress_current != revision.entity.page_id - page_ids.start:
                progress_current = revision.entity.page_id - page_ids.start
                update_progress(name, progress_current, progress_total)
        update_progress(name, progress_total, progress_total)

    def _init_worker_with_rdf_converter(self) -> Mapping[str, object]:
        self._jvm_manager = JvmManager(jars_dir=self._jars_dir)
        # TODO: log jvm errors?
        assert self._sites_table is not None
        return {
            "rdf_converter": WikidataRdfConverter(self._sites_table, self._jvm_manager)
        }

    def _exit_worker_with_rdf_converter(self) -> None:
        if self._jvm_manager is not None:
            self._jvm_manager.close()
            self._jvm_manager = None

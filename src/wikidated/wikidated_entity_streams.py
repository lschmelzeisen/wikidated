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

import re
from itertools import chain, groupby
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from typing import Iterator, Mapping, MutableSet, Optional, Tuple

from typing_extensions import Final

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
    WikidataRawRevision,
    WikidataRdfConversionError,
    WikidataRdfConverter,
    WikidataRdfTriple,
)
from wikidated.wikidated_revision import WikidatedRevision

_LOGGER = getLogger(__name__)


class WikidatedEntityStreamsFile:
    def __init__(self, archive_path: Path, page_ids: range) -> None:
        self.archive_path: Final = archive_path
        self.page_ids: Final = page_ids

    def iter_revisions(
        self, page_id: Optional[int] = None
    ) -> Iterator[WikidatedRevision]:
        assert self.archive_path.exists()
        archive = SevenZipArchive(self.archive_path)
        with archive.read(
            self._make_archive_component_path(page_id) if page_id else None
        ) as fd:
            for line in fd:
                yield WikidatedRevision.parse_raw(line)

    def iter_page_ids(self) -> Iterator[int]:
        assert self.archive_path.exists()
        archive = SevenZipArchive(self.archive_path)
        for component_file_name in archive.iter_file_names():
            yield self._parse_archive_component_path(component_file_name)

    @classmethod
    def archive_path_glob(cls, dataset_dir: Path) -> str:
        return f"{dataset_dir.name}-entity-streams-p*-p*.7z"

    @classmethod
    def _make_archive_path(cls, dataset_dir: Path, page_ids: range) -> Path:
        return dataset_dir / (
            f"{dataset_dir.name}-entity-streams"
            f"-p{page_ids.start}-p{page_ids.stop - 1}.7z"
        )

    @classmethod
    def _parse_archive_path(cls, path: Path) -> Tuple[Path, range]:
        match = re.match(
            r"^(?P<dataset_dir_name>.+)-entity-streams"
            r"-p(?P<min_page_id>\d+)-p(?P<max_page_id>\d+).7z$",
            path.name,
        )
        assert match

        dataset_dir = path.parent.resolve()
        assert dataset_dir.name == match["dataset_dir_name"]
        page_ids = range(int(match["min_page_id"]), int(match["max_page_id"]) + 1)
        return dataset_dir, page_ids

    @classmethod
    def _make_archive_component_path(cls, page_id: int) -> Path:
        return Path(f"p{page_id}.jsonl")

    @classmethod
    def _parse_archive_component_path(cls, path: Path) -> int:
        match = re.match(r"p(?P<page_id>\d+).jsonl", path.name)
        assert match

        page_id = int(match["page_id"])
        return page_id

    @classmethod
    def load(cls, path: Path) -> WikidatedEntityStreamsFile:
        assert path.exists()
        _, page_ids = cls._parse_archive_path(path)
        return WikidatedEntityStreamsFile(path, page_ids)

    @classmethod
    def build(
        cls,
        dataset_dir: Path,
        pages_meta_history: WikidataDumpPagesMetaHistory,
        rdf_converter: WikidataRdfConverter,
    ) -> Tuple[WikidatedEntityStreamsFile, Iterator[WikidatedRevision]]:
        archive_path = cls._make_archive_path(dataset_dir, pages_meta_history.page_ids)
        revisions_iter: Iterator[WikidatedRevision] = iter([])
        if archive_path.exists():
            _LOGGER.debug(f"File '{archive_path}' already exists, skipping building.")
        else:
            revisions_iter = cls._build_archive(
                archive_path, pages_meta_history, rdf_converter
            )
        return (
            WikidatedEntityStreamsFile(archive_path, pages_meta_history.page_ids),
            revisions_iter,
        )

    @classmethod
    def _build_archive(
        cls,
        archive_path: Path,
        pages_meta_history: WikidataDumpPagesMetaHistory,
        rdf_converter: WikidataRdfConverter,
    ) -> Iterator[WikidatedRevision]:
        tmp_dir = archive_path.parent / ("tmp." + archive_path.name)
        if tmp_dir.exists():
            # TODO: If desired, one could rewrite this for better recovery from
            #  crashes. That is, we could reuse entities already processed in tmp_dir
            #  and skip those when iterating.
            rmtree(tmp_dir)
        tmp_dir.mkdir(exist_ok=True, parents=True)

        for entity_meta, revisions in groupby(
            pages_meta_history.iter_revisions(display_progress_bar=False),
            lambda revision: revision.entity,
        ):
            wikidated_revisions = cls._iter_wikidated_revisions(
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

            with (tmp_dir / cls._make_archive_component_path(entity_meta.page_id)).open(
                "w", encoding="UTF-8"
            ) as fd:
                for wikidated_revision in chain(
                    (first_wikidated_revision,), wikidated_revisions
                ):
                    fd.write(wikidated_revision.json() + "\n")
                    yield wikidated_revision

        SevenZipArchive.from_dir(tmp_dir, archive_path)
        rmtree(tmp_dir)

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
    ) -> None:
        self._dataset_dir = dataset_dir
        self._files_by_page_ids = RangeMap[WikidatedEntityStreamsFile]()
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
        for file in parallelize(
            self._build_part,
            pages_meta_history.values(),
            num_arguments=len(pages_meta_history),
            extra_arguments={"dataset_dir": self._dataset_dir},
            init_worker_func=self._init_worker_with_rdf_converter,
            exit_worker_func=self._exit_worker_with_rdf_converter,
            max_workers=max_workers,
            progress_bar_desc="Entity Streams",
        ):
            self._files_by_page_ids[file.page_ids] = file

    @classmethod
    def _build_part(
        cls,
        argument: WikidataDumpPagesMetaHistory,
        update_progress: ParallelizeUpdateProgressFunc,
        **extra_arguments: object,
    ) -> WikidatedEntityStreamsFile:
        dataset_dir = extra_arguments["dataset_dir"]
        assert isinstance(dataset_dir, Path)
        rdf_converter = extra_arguments["rdf_converter"]
        assert isinstance(rdf_converter, WikidataRdfConverter)

        file, revisions_builder = WikidatedEntityStreamsFile.build(
            dataset_dir, argument, rdf_converter
        )
        progress_name = file.archive_path.name
        progress_current = 0
        progress_total = len(argument.page_ids)
        update_progress(progress_name, progress_current, progress_total)
        for revision in revisions_builder:
            # TODO: count number of processed revisions and exceptions?
            if progress_current != revision.entity.page_id - file.page_ids.start:
                progress_current = revision.entity.page_id - file.page_ids.start
                update_progress(progress_name, progress_current, progress_total)
        update_progress(progress_name, progress_total, progress_total)
        return file

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

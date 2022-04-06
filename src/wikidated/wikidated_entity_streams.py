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
from itertools import chain, groupby
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from sys import maxsize
from typing import (
    Any,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    MutableSet,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

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
        self,
        page_id: Optional[int] = None,
        *,
        min_revision_id: Optional[int] = None,
        max_revision_id: Optional[int] = None,
        min_timestamp: Optional[datetime] = None,
        max_timestamp: Optional[datetime] = None,
    ) -> Iterator[WikidatedRevision]:
        if not self.archive_path.exists():
            raise FileNotFoundError(self.archive_path)
        archive = SevenZipArchive(self.archive_path)
        min_revision_id_ = min_revision_id or -maxsize
        max_revision_id_ = max_revision_id or maxsize
        min_timestamp_ = min_timestamp or datetime.min
        max_timestamp_ = max_timestamp or datetime.max
        if not min_timestamp_.tzinfo:
            min_timestamp_ = min_timestamp_.replace(tzinfo=timezone.utc)
        if not max_timestamp_.tzinfo:
            max_timestamp_ = max_timestamp_.replace(tzinfo=timezone.utc)
        with archive.read(
            self._make_archive_component_path(page_id) if page_id else None
        ) as fd:
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
                    if page_id is not None:
                        break
                    else:
                        # Can not break here, since the revisions of the next entity
                        # will likely have lower revision IDs and timestamps again.
                        continue
                yield revision

    def iter_page_ids(self) -> Iterator[int]:
        if not self.archive_path.exists():
            raise FileNotFoundError(self.archive_path)
        archive = SevenZipArchive(self.archive_path)
        for page_id, _component_file_name in sorted(
            (WikidatedEntityStreamsFile._parse_archive_component_path(Path(c)), c)
            for c in archive.iter_file_names()
        ):
            yield page_id

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
    def load_custom(cls, path: Path) -> WikidatedEntityStreamsFile:
        _, page_ids = cls._parse_archive_path(path)
        return WikidatedEntityStreamsFile(path, page_ids)

    @classmethod
    def build_custom(
        cls,
        dataset_dir: Path,
        pages_meta_history: WikidataDumpPagesMetaHistory,
        rdf_converter: WikidataRdfConverter,
    ) -> Tuple[WikidatedEntityStreamsFile, Iterator[WikidatedRevision]]:
        archive_path = cls._make_archive_path(dataset_dir, pages_meta_history.page_ids)
        revisions_iter: Iterator[WikidatedRevision] = iter([])
        if archive_path.exists():
            _LOGGER.debug(
                f"Entity streams file '{archive_path.name}' already exists, skipping "
                f"building."
            )
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
        _LOGGER.debug(f"Building entity streams file {archive_path.name}.")

        tmp_dir = archive_path.parent / ("tmp." + archive_path.name)
        if tmp_dir.exists():
            # TODO: If desired, one could rewrite this for better recovery from
            #  crashes. That is, we could reuse entities already processed in tmp_dir
            #  and skip those when iterating.
            rmtree(tmp_dir)
        tmp_dir.mkdir(exist_ok=True, parents=True)

        for page_id, revisions in groupby(
            pages_meta_history.iter_revisions(display_progress_bar=False),
            lambda revision: revision.page_id,
        ):
            wikidated_revisions = cls._iter_wikidated_revisions(
                revisions, rdf_converter
            )

            # In the following we check if we can access the first element in the
            # iterable. If it does not exist, the current page does not describe a
            # Wikidata entity (e.g., it could be a wikitext page). Only if it exists, we
            # add a file to the output archive.

            try:
                first_wikidated_revision = next(wikidated_revisions)
            except StopIteration:
                _LOGGER.debug(
                    f"Could not construct any Wikidated revisions for page {page_id}. "
                    "Most likely this page does not describe a Wikidata entity."
                )
                continue

            with (tmp_dir / cls._make_archive_component_path(page_id)).open(
                "w", encoding="UTF-8"
            ) as fd:
                for wikidated_revision in chain(
                    (first_wikidated_revision,), wikidated_revisions
                ):
                    fd.write(wikidated_revision.json() + "\n")
                    yield wikidated_revision

        SevenZipArchive.from_dir(tmp_dir, archive_path)
        rmtree(tmp_dir)

        _LOGGER.debug(f"Done building entity streams file {archive_path.name}.")

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
                _LOGGER.debug(
                    f"RDF conversion error for revision {revision.revision_id}.",
                    exc_info=True,
                )
                continue

            triples_set = set(rdf_revision.triples)
            triple_deletions = sorted(state - triples_set)
            triple_additions = sorted(triples_set - state)
            state = triples_set

            yield WikidatedRevision(
                entity_id=revision.entity_id,
                page_id=revision.page_id,
                namespace=revision.namespace,
                redirect=revision.redirect,
                revision_id=revision.revision_id,
                parent_revision_id=revision.parent_revision_id,
                timestamp=revision.timestamp,
                contributor=revision.contributor,
                contributor_id=revision.contributor_id,
                is_minor=revision.is_minor,
                comment=revision.comment,
                wikibase_model=revision.wikibase_model,
                wikibase_format=revision.wikibase_format,
                sha1=revision.sha1,
                triple_deletions=triple_deletions,
                triple_additions=triple_additions,
            )


# Variables used to communicate with child processes:
_JARS_DIR: Optional[Path] = None
_SITES_TABLE: Optional[WikidataDumpSitesTable] = None
_JVM_MANAGER: Optional[JvmManager] = None


_T_WikidatedEntityStreamsFile_co = TypeVar(
    "_T_WikidatedEntityStreamsFile_co",
    bound=WikidatedEntityStreamsFile,
    covariant=True,
)


class WikidatedGenericEntityStreams(Generic[_T_WikidatedEntityStreamsFile_co]):
    def __init__(
        self, files_by_page_ids: RangeMap[_T_WikidatedEntityStreamsFile_co]
    ) -> None:
        self._files_by_page_ids = files_by_page_ids

    def __len__(self) -> int:
        return len(self._files_by_page_ids)

    def __iter__(self) -> Iterator[_T_WikidatedEntityStreamsFile_co]:
        return iter(self._files_by_page_ids.values())

    @overload
    def __getitem__(self, key: int) -> _T_WikidatedEntityStreamsFile_co:
        ...

    @overload
    def __getitem__(self, key: slice) -> Iterable[_T_WikidatedEntityStreamsFile_co]:
        ...

    @overload
    def __getitem__(self, key: object) -> Any:  # NoReturn doesn't work here.
        ...

    def __getitem__(
        self, key: object
    ) -> Union[WikidatedEntityStreamsFile, Iterable[_T_WikidatedEntityStreamsFile_co]]:
        if isinstance(key, int) or isinstance(key, slice):
            return self._files_by_page_ids[key]
        else:
            raise TypeError("key needs to be of type int.")

    @classmethod
    def load_custom(cls, dataset_dir: Path) -> WikidatedEntityStreams:
        _LOGGER.debug(f"Loading entity streams for dataset {dataset_dir.name}.")
        files_by_page_ids = RangeMap[WikidatedEntityStreamsFile]()
        for path in dataset_dir.glob(
            WikidatedEntityStreamsFile.archive_path_glob(dataset_dir)
        ):
            file = WikidatedEntityStreamsFile.load_custom(path)
            files_by_page_ids[file.page_ids] = file
        _LOGGER.debug(f"Done loading entity streams for dataset {dataset_dir.name}.")
        return WikidatedEntityStreams(files_by_page_ids)

    @classmethod
    def build_custom(
        cls,
        dataset_dir: Path,
        jars_dir: Path,
        sites_table: WikidataDumpSitesTable,
        pages_meta_history: RangeMap[WikidataDumpPagesMetaHistory],
        max_workers: Optional[int] = 4,
    ) -> WikidatedEntityStreams:
        _LOGGER.debug(f"Building entity streams for dataset {dataset_dir.name}.")
        global _JARS_DIR, _SITES_TABLE
        _JARS_DIR = jars_dir
        _SITES_TABLE = sites_table
        files_by_page_ids = RangeMap[WikidatedEntityStreamsFile]()
        for file in parallelize(
            cls._build_part,
            pages_meta_history.values(),
            num_arguments=len(pages_meta_history),
            extra_arguments={"dataset_dir": dataset_dir},
            init_worker_func=cls._init_worker_with_rdf_converter,
            exit_worker_func=cls._exit_worker_with_rdf_converter,
            max_workers=max_workers,
            progress_bar_desc="Entity Streams",
        ):
            files_by_page_ids[file.page_ids] = file
        _LOGGER.debug(f"Done building entity streams for dataset {dataset_dir.name}.")
        return WikidatedEntityStreams(files_by_page_ids)

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

        file, revisions_builder = WikidatedEntityStreamsFile.build_custom(
            dataset_dir, argument, rdf_converter
        )
        progress_name = file.archive_path.name
        progress_current = 0
        progress_total = len(argument.page_ids)
        update_progress(progress_name, progress_current, progress_total)
        for revision in revisions_builder:
            if progress_current != revision.page_id - file.page_ids.start:
                progress_current = revision.page_id - file.page_ids.start
                update_progress(progress_name, progress_current, progress_total)
        update_progress(progress_name, progress_total, progress_total)
        return file

    @classmethod
    def _init_worker_with_rdf_converter(cls) -> Mapping[str, object]:
        assert _JARS_DIR is not None
        assert _SITES_TABLE is not None
        global _JVM_MANAGER
        _JVM_MANAGER = JvmManager(jars_dir=_JARS_DIR)
        return {"rdf_converter": WikidataRdfConverter(_SITES_TABLE, _JVM_MANAGER)}

    @classmethod
    def _exit_worker_with_rdf_converter(cls) -> None:
        global _JVM_MANAGER
        if _JVM_MANAGER is not None:
            _JVM_MANAGER.close()
            _JVM_MANAGER = None


WikidatedEntityStreams = WikidatedGenericEntityStreams[WikidatedEntityStreamsFile]

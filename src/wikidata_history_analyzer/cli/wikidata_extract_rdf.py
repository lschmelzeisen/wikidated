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
from sys import argv
from typing import AbstractSet, Counter, MutableSequence, Sequence, Tuple, cast

import requests
from nasty_utils import Argument, ColoredBraceStyleAdapter, Program, ProgramConfig
from overrides import overrides
from pydantic import validator

import wikidata_history_analyzer
from wikidata_history_analyzer._utils import (
    ParallelizeCallback,
    ParallelizeProgressCallback,
    parallelize,
)
from wikidata_history_analyzer.datamodel.wikidata_rdf_revision import (
    WikidataRdfRevision,
)
from wikidata_history_analyzer.datamodel.wikidata_revision import (
    WikidataRevisionProcessingException,
)
from wikidata_history_analyzer.dumpfiles.wikidata_dump_manager import (
    WikidataDumpManager,
)
from wikidata_history_analyzer.dumpfiles.wikidata_meta_history_dump import (
    WikidataMetaHistoryDump,
)
from wikidata_history_analyzer.dumpfiles.wikidata_sites_table import WikidataSitesTable
from wikidata_history_analyzer.jvm_manager import JvmManager
from wikidata_history_analyzer.settings_ import WikidataHistoryAnalyzerSettings

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataExtractRdf(Program):
    class Config(ProgramConfig):
        title = "wikidata-extract-rdf"
        version = wikidata_history_analyzer.__version__
        description = "Extract RDF statements about specific pages/revisions."

    settings: WikidataHistoryAnalyzerSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    file: Sequence[Path] = Argument(
        (),
        short_alias="f",
        description="Target dump file (separate multipel with commas).",
    )
    title_: Sequence[str] = Argument(
        (),
        short_alias="t",
        alias="title",
        description="Target title (prefixed, separate multiple with commas).",
    )
    page: Sequence[str] = Argument(
        (),
        short_alias="p",
        description="Target page ID (separate multiple with commas).",
    )
    revision: Sequence[str] = Argument(
        (),
        short_alias="r",
        description="Target revision ID (separate multiple with commas).",
    )

    @validator("file", "title_", "page", "revision", pre=True)
    def _split_at_comma(cls, value: object) -> Sequence[str]:  # noqa: N805
        return (
            value.split(",") if isinstance(value, str) else cast(Sequence[str], value)
        )

    @overrides
    def run(self) -> None:
        settings = self.settings.wikidata_history_analyzer

        dump_manager = WikidataDumpManager(
            settings.data_dir,
            settings.wikidata_dump_version,
            settings.wikidata_dump_mirror_base,
        )

        sites_table = dump_manager.sites_table()
        sites_table.download()

        target_page_ids, target_revision_ids = self._get_target_ids(dump_manager)
        target_dumps = self._get_target_dumps(dump_manager, target_page_ids)
        self._check_extra_page_ids(target_dumps, target_page_ids)

        exception_counter_overall = Counter[str]()
        num_processed_revisions_overall = 0

        num_workers = min(settings.num_workers, len(target_dumps))
        for num_processed_revisions, exception_counter in parallelize(
            cast(
                ParallelizeCallback[WikidataMetaHistoryDump, Tuple[int, Counter[str]]],
                self._process_dump,
            ),
            target_dumps,
            extra_arguments={
                "data_dir": settings.data_dir,
                "sites_table": sites_table,
                "target_page_ids": target_page_ids,
                "target_revision_ids": target_revision_ids,
            },
            total=len(target_dumps),
            max_workers=num_workers,
            jars_dir=settings.wikidata_toolkit_jars_dir,
        ):
            num_processed_revisions_overall += num_processed_revisions
            exception_counter_overall += exception_counter

        if exception_counter_overall:
            _LOGGER.warning(
                "Exception occurred when RDF-serializing (out of {} processed "
                "revisions):",
                num_processed_revisions_overall,
            )
            for reason, count in exception_counter_overall.items():
                _LOGGER.warning("  {} ({})", reason, count)

    def _get_target_ids(
        self, dump_manager: WikidataDumpManager
    ) -> Tuple[AbstractSet[str], AbstractSet[str]]:
        target_page_ids = {*self.page}
        target_revision_ids = {*self.revision}

        target_titles = set(self.title_)
        if target_titles:
            _LOGGER.info("Target page titles: {}", ", ".join(sorted(target_titles)))

            _LOGGER.info("Querying page IDs of given titles...")
            if len(target_titles) < 10_000:
                for title in target_titles:
                    response = requests.get(
                        "https://www.wikidata.org/w/api.php?action=query&format=json"
                        "&titles=" + title
                    ).json()
                    page_id = next(iter(response["query"]["pages"].keys()))
                    target_page_ids.add(page_id)
            else:
                # Iterating through the page table should be faster for many titles.
                for page_meta in dump_manager.page_table().iter_page_metas(
                    dump_manager.namespaces().load_namespace_titles()
                ):
                    if page_meta.prefixed_title in target_titles:
                        target_page_ids.add(page_meta.page_id)

        _LOGGER.info("Target page IDs: {}", ", ".join(sorted(target_page_ids)) or "all")
        _LOGGER.info(
            "Target revision IDs: {}", ", ".join(sorted(target_revision_ids)) or "all"
        )

        return target_page_ids, target_revision_ids

    def _get_target_dumps(
        self, dump_manager: WikidataDumpManager, target_page_ids: AbstractSet[str]
    ) -> Sequence[WikidataMetaHistoryDump]:
        files = set(self.file)

        target_dumps: MutableSequence[WikidataMetaHistoryDump] = []
        if files:
            for dump in dump_manager.meta_history_dumps():
                if Path(dump.path.name) in files:
                    target_dumps.append(dump)
        elif target_page_ids:
            for dump in dump_manager.meta_history_dumps():
                for page_id in target_page_ids:
                    if int(dump.min_page_id) <= int(page_id) <= int(dump.max_page_id):
                        target_dumps.append(dump)
                        break

        _LOGGER.info(
            "Target dumps: {}",
            ", ".join([dump.path.name for dump in target_dumps]) or "all",
        )
        if not target_dumps:
            _LOGGER.warning(
                "No target page/revision IDs, will export all pages and revisions!"
            )
        return target_dumps or dump_manager.meta_history_dumps()

    @classmethod
    def _check_extra_page_ids(
        cls,
        target_dumps: Sequence[WikidataMetaHistoryDump],
        target_page_ids: AbstractSet[str],
    ) -> None:
        extra_page_ids = set()
        for page_id in target_page_ids:
            for dump in target_dumps:
                if dump.min_page_id <= page_id <= dump.max_page_id:
                    break
            else:
                extra_page_ids.add(page_id)

        if extra_page_ids:
            _LOGGER.warning(
                "The following target page IDs are not included in any target dump: {}",
                sorted(extra_page_ids),
            )

    @classmethod
    def _process_dump(
        cls,
        dump: WikidataMetaHistoryDump,
        *,
        data_dir: Path,
        sites_table: WikidataSitesTable,
        target_page_ids: AbstractSet[str],
        target_revision_ids: AbstractSet[str],
        progress_callback: ParallelizeProgressCallback,
        jvm_manager: JvmManager,
        **kwargs: object,
    ) -> Tuple[int, Counter[str]]:
        num_pages = int(dump.max_page_id) - int(dump.min_page_id) + 1
        progress_callback(dump.path.name, 0, num_pages)

        dump.download()

        num_processed_revisions = 0
        exception_counter = Counter[str]()
        for revision in dump.iter_revisions(display_progress_bar=False):
            progress_callback(
                dump.path.name, int(revision.page_id) - int(dump.min_page_id), num_pages
            )

            if (target_page_ids and revision.page_id not in target_page_ids) or (
                target_revision_ids and revision.revision_id not in target_revision_ids
            ):
                continue

            num_processed_revisions += 1
            try:
                rdf_revision = WikidataRdfRevision.from_revision(
                    revision, sites_table, jvm_manager
                )
                rdf_revision.save_to_file(data_dir, dump.path.name)
            except WikidataRevisionProcessingException as exception:
                exception_counter[exception.reason] += 1
                continue

        progress_callback(dump.path.name, num_pages, num_pages)

        return num_processed_revisions, exception_counter


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataExtractRdf.init(*args).run()


if __name__ == "__main__":
    main()

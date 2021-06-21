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
from typing import AbstractSet, Counter, Iterator, Sequence, Tuple, cast

from nasty_utils import Argument, ColoredBraceStyleAdapter, Program
from pydantic import validator

from wikidata_history_analyzer._utils import ParallelizeProgressCallback
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
from wikidata_history_analyzer.misc import (
    check_page_ids_in_meta_history_dumps,
    meta_history_dumps_for_dump_names,
    meta_history_dumps_for_page_ids,
    page_ids_from_prefixed_titles,
)
from wikidata_history_analyzer.settings_ import WikidataHistoryAnalyzerSettings

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataRdfRevisionProgram(Program):
    settings: WikidataHistoryAnalyzerSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    title_: Sequence[str] = Argument(
        (),
        short_alias="t",
        alias="title",
        description="Target title (prefixed, separate multiple with commas).",
    )
    page: Sequence[int] = Argument(
        (),
        short_alias="p",
        description="Target page ID (separate multiple with commas).",
    )
    file: Sequence[str] = Argument(
        (),
        short_alias="f",
        description="Target dump file (separate multipel with commas).",
    )

    @validator("file", "title_", "page", pre=True)
    def _split_at_comma(cls, value: object) -> Sequence[str]:  # noqa: N805
        return (
            value.split(",") if isinstance(value, str) else cast(Sequence[str], value)
        )

    def _prepare_args(
        self, dump_manager: WikidataDumpManager
    ) -> Tuple[AbstractSet[int], Sequence[WikidataMetaHistoryDump]]:
        _LOGGER.info(
            "Page titles: {}",
            ", ".join(self.title_) if self.title_ else "all",
        )

        page_ids = set.union(
            set(page_ids_from_prefixed_titles(self.title_, dump_manager)),
            set(self.page),
        )
        _LOGGER.info(
            "Page IDs: {}",
            ", ".join(map(str, sorted(page_ids))) if page_ids else "all",
        )

        meta_history_dumps = (
            meta_history_dumps_for_dump_names(set(self.file), dump_manager)
            if self.file
            else meta_history_dumps_for_page_ids(page_ids, dump_manager)
        )
        _LOGGER.info(
            "Meta history dumps: {}",
            (
                ", ".join(dump.path.name for dump in meta_history_dumps)
                if len(meta_history_dumps) != len(dump_manager.meta_history_dumps())
                else "all"
            ),
        )

        check_page_ids_in_meta_history_dumps(page_ids, meta_history_dumps)

        return page_ids, meta_history_dumps

    @classmethod
    def _iter_rdf_revisions(
        cls,
        meta_history_dump: WikidataMetaHistoryDump,
        *,
        sites_table: WikidataSitesTable,
        page_ids: AbstractSet[int],
        progress_callback: ParallelizeProgressCallback,
        jvm_manager: JvmManager,
        log_dir: Path,
    ) -> Iterator[WikidataRdfRevision]:
        meta_history_dump.download()

        num_pages = meta_history_dump.max_page_id - meta_history_dump.min_page_id + 1
        progress_callback(meta_history_dump.path.name, 0, num_pages)

        log_dir.mkdir(parents=True, exist_ok=True)
        jvm_manager.set_java_logging_file_handler(log_dir / "rdf-serialization.log")

        num_processed_revisions = 0
        exception_counter = Counter[str]()
        for revision in meta_history_dump.iter_revisions(display_progress_bar=False):
            progress_callback(
                meta_history_dump.path.name,
                revision.page_id - meta_history_dump.min_page_id,
                num_pages,
            )

            if page_ids and revision.page_id not in page_ids:
                continue

            num_processed_revisions += 1
            try:
                yield WikidataRdfRevision.from_raw_revision(
                    revision, sites_table, jvm_manager
                )
            except WikidataRevisionProcessingException as exception:
                exception_counter[exception.reason] += 1
                continue

        jvm_manager.set_java_logging_file_handler(None)

        progress_callback(meta_history_dump.path.name, num_pages, num_pages)

        with (log_dir / "rdf-serialization.exceptions.log").open(
            "w", encoding="UTF-8"
        ) as fout:
            fout.write(
                "Exception occurred when RDF-serializing (out of "
                f"{num_processed_revisions} processed revisions):\n"
            )
            for reason, count in exception_counter.items():
                fout.write(f"  {reason} ({count})\n")

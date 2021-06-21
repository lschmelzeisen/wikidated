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


from itertools import groupby
from logging import getLogger
from pathlib import Path
from sys import argv
from typing import AbstractSet, cast

from nasty_utils import ColoredBraceStyleAdapter, ProgramConfig
from overrides import overrides

import wikidata_history_analyzer
from wikidata_history_analyzer._paths import wikidata_incremental_rdf_revision_dir
from wikidata_history_analyzer._utils import (
    ParallelizeCallback,
    ParallelizeProgressCallback,
    parallelize,
)
from wikidata_history_analyzer.cli._wikidata_rdf_revision_program import (
    WikidataRdfRevisionProgram,
)
from wikidata_history_analyzer.datamodel.wikidata_incremental_rdf_revision import (
    WikidataIncrementalRdfRevision,
)
from wikidata_history_analyzer.dumpfiles.wikidata_dump_manager import (
    WikidataDumpManager,
)
from wikidata_history_analyzer.dumpfiles.wikidata_meta_history_dump import (
    WikidataMetaHistoryDump,
)
from wikidata_history_analyzer.dumpfiles.wikidata_sites_table import WikidataSitesTable
from wikidata_history_analyzer.jvm_manager import JvmManager

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataExtractIncrementalRdf(WikidataRdfRevisionProgram):
    class Config(ProgramConfig):
        title = "wikidata-extract-rdf"
        version = wikidata_history_analyzer.__version__
        description = "Extract incremental RDF revisions."

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

        page_ids, meta_history_dumps = self._prepare_args(dump_manager)

        for _ in parallelize(
            cast(
                ParallelizeCallback[WikidataMetaHistoryDump, None],
                self._process_dump,
            ),
            meta_history_dumps,
            extra_arguments={
                "data_dir": settings.data_dir,
                "sites_table": sites_table,
                "page_ids": page_ids,
            },
            total=len(meta_history_dumps),
            max_workers=settings.num_workers,
            jars_dir=settings.wikidata_toolkit_jars_dir,
        ):
            pass

    @classmethod
    def _process_dump(
        cls,
        meta_history_dump: WikidataMetaHistoryDump,
        *,
        data_dir: Path,
        sites_table: WikidataSitesTable,
        page_ids: AbstractSet[int],
        progress_callback: ParallelizeProgressCallback,
        jvm_manager: JvmManager,
        **kwargs: object,
    ) -> None:
        out_dir = (
            wikidata_incremental_rdf_revision_dir(data_dir)
            / meta_history_dump.path.name
        )
        if out_dir.exists():
            _LOGGER.info("Directory {} already exists, skipping.", out_dir)
            return

        out_dir_tmp = out_dir.parent / (out_dir.name + ".tmp")

        for page_id, revisions in groupby(
            WikidataIncrementalRdfRevision.from_rdf_revisions(
                cls._iter_rdf_revisions(
                    meta_history_dump,
                    sites_table=sites_table,
                    page_ids=page_ids,
                    progress_callback=progress_callback,
                    jvm_manager=jvm_manager,
                    log_dir=out_dir_tmp,
                )
            ),
            lambda revision: revision.page_id,
        ):
            WikidataIncrementalRdfRevision.save_iter_to_file(
                revisions, out_dir_tmp, page_id
            )

        out_dir_tmp.rename(out_dir)


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataExtractIncrementalRdf.init(*args).run()


if __name__ == "__main__":
    main()

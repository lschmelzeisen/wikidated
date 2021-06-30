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


import gzip
import heapq
from logging import getLogger
from pathlib import Path
from sys import argv
from typing import AbstractSet, Iterator, Sequence, cast

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

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataMergeIncrementalRdfProgram(WikidataRdfRevisionProgram):
    class Config(ProgramConfig):
        title = "wikidata-merge-incremental-rdf"
        version = wikidata_history_analyzer.__version__
        description = "Merge incremental RDF revisions into one continuous stream."

    @overrides
    def run(self) -> None:
        settings = self.settings.wikidata_history_analyzer

        dump_manager = WikidataDumpManager(
            settings.data_dir,
            settings.wikidata_dump_version,
            settings.wikidata_dump_mirror_base,
        )

        page_ids, meta_history_dumps = self._prepare_args(dump_manager)

        _LOGGER.info("Merging incremental RDF revisions of each dump individually...")
        for _ in parallelize(
            cast(
                ParallelizeCallback[WikidataMetaHistoryDump, None], self._process_dump
            ),
            meta_history_dumps,
            extra_arguments={
                "data_dir": settings.data_dir,
                "page_ids": page_ids,
            },
            total=len(meta_history_dumps),
            max_workers=settings.num_workers,
        ):
            pass

        _LOGGER.info("Merging incremental RDF revisions across all dumps...")
        self._merge_dumps(meta_history_dumps, settings.data_dir)

    @classmethod
    def _process_dump(
        cls,
        meta_history_dump: WikidataMetaHistoryDump,
        *,
        data_dir: Path,
        page_ids: AbstractSet[int],
        progress_callback: ParallelizeProgressCallback,
        **kwargs: object,
    ) -> None:
        dump_dir = (
            wikidata_incremental_rdf_revision_dir(data_dir)
            / meta_history_dump.path.name
        )

        out_file = dump_dir.parent / (
            meta_history_dump.path.name + ".incremental-rdf-revisions.jsonl.gz"
        )
        if out_file.exists():
            _LOGGER.info("File {} already exists, skipping...", out_file)
            return

        dump_dir_files = sorted(dump_dir.iterdir())
        num_dump_dir_files = len(dump_dir_files)

        # Using 2x num_dump_dir_files so that the progress bar reaches 50% once all
        # files have been opened and 100% once the merging is done. Sadly, I do not know
        # any elegant way to monitor progress while merging, so we will see a hard jump
        # from 50% to 100%.
        progress_callback(meta_history_dump.path.name, 0, 2 * num_dump_dir_files)

        revision_iters = []
        for i, page_revisions_file in enumerate(dump_dir_files):
            progress_callback(meta_history_dump.path.name, i, 2 * num_dump_dir_files)

            if not page_revisions_file.name.endswith(".jsonl.gz"):
                continue

            page_id_str = page_revisions_file.name[: -len(".jsonl.gz")]
            if not page_id_str.isdigit():
                continue

            page_id = int(page_id_str)
            if page_ids and page_id not in page_ids:
                continue

            revision_iters.append(
                WikidataIncrementalRdfRevision.load_iter_from_file(dump_dir, page_id)
            )

        cls._merge_incremental_revision_iters(revision_iters, out_file)

        progress_callback(
            meta_history_dump.path.name, 2 * num_dump_dir_files, 2 * num_dump_dir_files
        )

    @classmethod
    def _merge_dumps(
        cls, meta_history_dumps: Sequence[WikidataMetaHistoryDump], data_dir: Path
    ) -> None:
        dumps_dir = wikidata_incremental_rdf_revision_dir(data_dir)
        out_file = dumps_dir / "incremental-rdf-revisions.jsonl.gz"

        revision_iters = []
        for meta_history_dump in meta_history_dumps:
            file = dumps_dir / (
                meta_history_dump.path.name + ".incremental-rdf-revisions.jsonl.gz"
            )
            revision_iters.append(
                WikidataIncrementalRdfRevision.load_iter_from_file(file)
            )

        cls._merge_incremental_revision_iters(revision_iters, out_file)

    @classmethod
    def _merge_incremental_revision_iters(
        cls, iters: Sequence[Iterator[WikidataIncrementalRdfRevision]], out_file: Path
    ) -> None:
        out_file_tmp = out_file.parent / (out_file.name + ".tmp")

        with gzip.open(out_file_tmp, "wt", encoding="UTF-8") as fout:
            for revision in heapq.merge(*iters, key=lambda r: r.revision_id):
                fout.write(revision.json() + "\n")

        out_file_tmp.rename(out_file)


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataMergeIncrementalRdfProgram.init(*args).run()


if __name__ == "__main__":
    main()

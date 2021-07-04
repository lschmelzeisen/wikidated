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
from collections import defaultdict
from datetime import date, datetime, timedelta
from logging import getLogger
from pathlib import Path
from sys import argv
from typing import (
    AbstractSet,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    cast,
)

from nasty_utils import ColoredBraceStyleAdapter, ProgramConfig
from overrides import overrides
from pydantic import BaseModel as PydanticModel

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
from wikidata_history_analyzer.datamodel.wikidata_rdf_revision import WikidataRdfTriple
from wikidata_history_analyzer.dumpfiles.wikidata_dump_manager import (
    WikidataDumpManager,
)
from wikidata_history_analyzer.dumpfiles.wikidata_meta_history_dump import (
    WikidataMetaHistoryDump,
)

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataRevisionStatistics(PydanticModel):
    time_since_last_revision: Optional[timedelta] = None
    num_added_triples: int = 0
    num_deleted_triples: int = 0
    subject_counter: Mapping[str, int] = defaultdict(int)
    predicate_counter: Mapping[str, int] = defaultdict(int)
    object_counter: Mapping[str, int] = defaultdict(int)
    num_triple_changes_to_time_since_last_change: Mapping[
        int, MutableSequence[timedelta]
    ] = defaultdict(list)


class WikidataDumpStatistics(PydanticModel):
    num_revisions_per_page: MutableSequence[int] = []

    per_month: MutableMapping[str, MutableSequence[WikidataRevisionStatistics]] = {}


class WikidataCollectStatisticsProgram(WikidataRdfRevisionProgram):
    class Config(ProgramConfig):
        title = "wikidata-collect-statistics"
        version = wikidata_history_analyzer.__version__
        description = "Collect statistics from incremental RDF revision stream."

    @overrides
    def run(self) -> None:
        settings = self.settings.wikidata_history_analyzer

        dump_manager = WikidataDumpManager(
            settings.data_dir,
            settings.wikidata_dump_version,
            settings.wikidata_dump_mirror_base,
        )

        page_ids, meta_history_dumps = self._prepare_args(dump_manager)

        _LOGGER.info("Collection statistics from each dump individually...")
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

        # TODO: merge statistics

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
            meta_history_dump.path.name + ".statistics.json.gz"
        )
        if out_file.exists():
            _LOGGER.info("File {} already exists, skipping...", out_file)
            return

        dump_dir_files = sorted(dump_dir.iterdir())
        num_dump_dir_files = len(dump_dir_files)

        progress_callback(meta_history_dump.path.name, 0, num_dump_dir_files)

        dump_statistics = WikidataDumpStatistics(
            num_revisions_per_page=[],
            per_month=defaultdict(list),
        )

        for i, page_revisions_file in enumerate(dump_dir_files):
            progress_callback(meta_history_dump.path.name, i, num_dump_dir_files)

            if not page_revisions_file.name.endswith(".jsonl.gz"):
                continue

            page_id_str = page_revisions_file.name[: -len(".jsonl.gz")]
            if not page_id_str.isdigit():
                continue

            page_id = int(page_id_str)
            if page_ids and page_id not in page_ids:
                continue

            num_revisions = 0
            first_revision_timestamp: Optional[datetime] = None
            last_revision_timestamp: Optional[datetime] = None
            num_times_triples_changed: MutableMapping[WikidataRdfTriple, int] = {}
            last_time_triples_changed: MutableMapping[WikidataRdfTriple, datetime] = {}

            for revision in WikidataIncrementalRdfRevision.load_iter_from_file(
                dump_dir, page_id
            ):
                num_revisions += 1
                revision_statistics = WikidataRevisionStatistics()

                if first_revision_timestamp is None:
                    first_revision_timestamp = revision.timestamp
                else:
                    assert last_revision_timestamp is not None
                    revision_statistics.time_since_last_revision = (
                        revision.timestamp - last_revision_timestamp
                    )

                revision_statistics.num_added_triples += len(revision.added_triples)
                revision_statistics.num_deleted_triples += len(revision.deleted_triples)

                for triple in revision.added_triples:
                    if triple not in num_times_triples_changed:
                        revision_statistics.subject_counter[triple.subject] += 1
                        revision_statistics.predicate_counter[triple.predicate] += 1
                        revision_statistics.object_counter[triple.object_] += 1
                        num_times_triples_changed[triple] = 1
                    else:
                        assert num_times_triples_changed[triple] % 2 == 0
                        num_times_triples_changed[triple] += 1

                    time_since_last_change = revision.timestamp - (
                        last_time_triples_changed.get(triple)
                        or first_revision_timestamp
                    )
                    revision_statistics.num_triple_changes_to_time_since_last_change[
                        num_times_triples_changed[triple]
                    ].append(time_since_last_change)
                    last_time_triples_changed[triple] = revision.timestamp

                for triple in revision.deleted_triples:
                    assert num_times_triples_changed[triple] % 2 == 1
                    num_times_triples_changed[triple] += 1

                    time_since_last_change = revision.timestamp - (
                        last_time_triples_changed.get(triple)
                        or first_revision_timestamp
                    )
                    revision_statistics.num_triple_changes_to_time_since_last_change[
                        num_times_triples_changed[triple]
                    ].append(time_since_last_change)
                    last_time_triples_changed[triple] = revision.timestamp

                month = revision.timestamp.strftime("%Y-%m")
                if month not in dump_statistics.per_month:
                    dump_statistics.per_month[month] = []
                dump_statistics.per_month[month].append(revision_statistics)

                last_revision_timestamp = revision.timestamp

                # Divide statistics into "evolution of Wikidata" (i.e. content-wise
                # on a macro scale) and "editing behavior of Wikidata" (i.e. meta-wise
                # on a micro scale)?
                #
                # Statistics for both all statements and only the filtered ones we would
                # care about for KG embedding.
                #
                # Statistics about simple vs full statements?
                # Statistics about redirects?
                #
                # Proportion of labels, descriptions, aliases, sitelinks, simple
                # statements?
                # What are datatypes and property links?
                # Number of items vs number of properties?
                # Languages used in wikidata literals.
                # Languages used in wikidata sitelinks.
                # Simple statements with literal as target.
                # Simple statement with non-literal as target.
                # Simple statement with in-wikidata entity as target.
                # Simple statement with out-of-wikidata entity as target.
                # links to wikimedia commons?
                #
                # Time between revision of each entity
                # Frequency of each entity/relation
                # Fraction of triples that are deleted
                # Fraction of triples that oscillate
                # (and are positive/negative at the end?)
                # Life-time of deleted triples
                #
                # Probably not interesting:
                # distribution of ranks (e.g. normalrank, bestrank, deprecatedrank,
                # etc.)?

            dump_statistics.num_revisions_per_page.append(num_revisions)

        with gzip.open(out_file, "wt", encoding="UTF") as fout:
            fout.write(dump_statistics.json() + "\n")

        progress_callback(
            meta_history_dump.path.name, num_dump_dir_files, num_dump_dir_files
        )


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataCollectStatisticsProgram.init(*args).run()


if __name__ == "__main__":
    main()

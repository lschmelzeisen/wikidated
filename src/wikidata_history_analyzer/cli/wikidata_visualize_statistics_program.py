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
from datetime import date, datetime, timedelta
from logging import getLogger
from math import ceil, log10
from pathlib import Path
from sys import argv
from typing import Counter, Tuple, Union, cast

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
from wikidata_history_analyzer.cli.wikidata_collect_statistics_program import (
    WikidataDumpStatistics,
)
from wikidata_history_analyzer.dumpfiles.wikidata_dump_manager import (
    WikidataDumpManager,
)
from wikidata_history_analyzer.dumpfiles.wikidata_meta_history_dump import (
    WikidataMetaHistoryDump,
)

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_SECS_IN_MIN = 60
_SECS_IN_HOUR = 60 * 60
_DAYS_IN_MONTH = 30

_SECOND = timedelta(seconds=1)
_MINUTE = timedelta(minutes=1)
_HOUR = timedelta(hours=1)
_DAY = timedelta(days=1)
_WEEK = timedelta(days=7)
_MONTH = timedelta(days=30)
_YEAR = timedelta(days=365)


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

        _, meta_history_dumps = self._prepare_args(dump_manager)

        _LOGGER.info("Aggregating dump statistics...")

        agg_num_entities_per_month = Counter[date]()
        agg_num_revisions_per_month = Counter[date]()
        agg_num_revisions_per_entity_histogram = Counter[float]()
        agg_time_between_revisions_histogram = Counter[float]()
        agg_num_triple_additions_per_revision_histogram = Counter[int]()
        agg_num_triple_deletions_histogram = Counter[int]()
        agg_num_triple_changes_histogram = Counter[int]()
        agg_time_until_triple_inserted = Counter[float]()
        agg_time_until_triple_deleted = Counter[float]()
        agg_time_until_triple_oscillated = Counter[float]()

        for (
            num_entities_per_month,
            num_revisions_per_month,
            num_revisions_per_entity_histogram,
            time_between_revisions_histogram,
            num_triple_additions_per_revision_histogram,
            num_triple_deletions_per_revision_histogram,
            num_triple_changes_histogram,
            time_until_triple_inserted,
            time_until_triple_deleted,
            time_until_triple_oscillated,
        ) in parallelize(
            cast(
                ParallelizeCallback[WikidataMetaHistoryDump, None], self._process_dump
            ),
            meta_history_dumps,
            extra_arguments={
                "data_dir": settings.data_dir,
            },
            total=len(meta_history_dumps),
            max_workers=settings.num_workers,
        ):
            agg_num_entities_per_month += num_entities_per_month
            agg_num_revisions_per_month += num_revisions_per_month
            agg_num_revisions_per_entity_histogram += num_revisions_per_entity_histogram
            agg_time_between_revisions_histogram += time_between_revisions_histogram
            agg_num_triple_additions_per_revision_histogram += (
                num_triple_additions_per_revision_histogram
            )
            agg_num_triple_deletions_histogram += (
                num_triple_deletions_per_revision_histogram
            )
            agg_num_triple_changes_histogram += num_triple_changes_histogram
            agg_time_until_triple_inserted += time_until_triple_inserted
            agg_time_until_triple_deleted += time_until_triple_deleted
            agg_time_until_triple_oscillated += time_until_triple_oscillated

        # ==============================================================================
        print()
        print("figures/num-dels-per-triple.tex")
        print("  agg_num_triple_changes_histogram = {")  # -----------------------------
        for k in sorted(agg_num_triple_changes_histogram.keys()):
            print(f"    {k}: {agg_num_triple_changes_histogram[k]},")
        print("  }")
        ks = [1, 2, 4, 20]
        vs = {
            i: (
                agg_num_triple_changes_histogram[ks[i]]
                - (
                    agg_num_triple_changes_histogram[ks[i + 1]]
                    if i != len(ks) - 1
                    else 0
                )
            )
            for i in range(len(ks))
        }
        print("  \\addplot")  # --------------------------------------------------------
        for k, v in vs.items():
            print(f"    ({k},{v})")
        print("  \\draw")  # -----------------------------------------------------------
        for k, v in vs.items():
            print(f"    (axis cs:{k},{v}) node {{{v}}}")

        # ==============================================================================
        print()
        print("figures/num-entities-revisions-over-time.tex")
        print("  agg_num_entities_per_month = {")  # -----------------------------------
        for d in sorted(agg_num_entities_per_month.keys()):
            v = agg_num_entities_per_month[d]
            print(f"    date({d.year},{d.month},{d.day}): {v},")
        print("  }")
        print("  agg_num_revisions_per_month = {")  # ----------------------------------
        for d in sorted(agg_num_revisions_per_month.keys()):
            v = agg_num_revisions_per_month[d]
            print(f"    date({d.year},{d.month},{d.day}): {v},")
        print("  }")
        vs = {}
        for y in range(2012, 2021):
            vs[y] = (vs[y - 1] if y != 2012 else 0) + sum(
                agg_num_revisions_per_month[date(y, m, 1)] for m in range(1, 13)
            )
        print("  \\addplot (revisions)")  # --------------------------------------------
        for k, v in vs.items():
            print(f"    ({k}-01-01,{v})")
        vs = {}
        for y in range(2012, 2021):
            vs[y] = (vs[y - 1] if y != 2012 else 0) + sum(
                agg_num_entities_per_month[date(y, m, 1)] for m in range(1, 13)
            )
        print("  \\addplot (entities)")  # ---------------------------------------------
        for k, v in vs.items():
            print(f"    ({k}-01-01,{v})")
        print("Total number of entities:", sum(agg_num_entities_per_month.values()))
        print("Total number of revisions:", sum(agg_num_revisions_per_month.values()))

        # ==============================================================================
        print()
        print("figures/num-revisions-per-entity.tex")
        print("  agg_num_revisions_per_entity_histogram = {")  # -----------------------
        for k in sorted(agg_num_revisions_per_entity_histogram.keys()):
            print(f"    {k}: {agg_num_revisions_per_entity_histogram[k]},")
        print("  }")
        s = sum(agg_num_revisions_per_entity_histogram.values())
        vs = {
            k: agg_num_revisions_per_entity_histogram[k] / s
            for k in range(
                0, max(5, max(agg_num_revisions_per_entity_histogram.keys())) + 1
            )
        }
        print("  \\addplot")  # --------------------------------------------------------
        for k, v in vs.items():
            print(f"    ({k},{v:.2f})")
        print(f"    ({k + 1},{v:.2f})")
        print("  \\draw")  # -----------------------------------------------------------
        for k, v in vs.items():
            print(f"    (axis cs: {k + 0.5},{v:.2f}) node {{{v*100:.0f}\\%}}")

        # ==============================================================================
        print()
        print("figures/num-triple-adds-dels-per-revision.tex")
        print("  agg_num_triple_additions_per_revision_histogram = {")  # --------------
        for k in sorted(agg_num_triple_additions_per_revision_histogram.keys()):
            print(f"    {k}: {agg_num_triple_additions_per_revision_histogram[k]},")
        print("  }")
        print("  agg_num_triple_deletions_histogram = {")  # --------------
        for k in sorted(agg_num_triple_deletions_histogram.keys()):
            print(f"    {k}: {agg_num_triple_deletions_histogram[k]},")
        print("  }")
        s = sum(agg_num_triple_additions_per_revision_histogram.values())
        vs = {
            0: agg_num_triple_additions_per_revision_histogram[0] / s,
            1: agg_num_triple_additions_per_revision_histogram[1] / s,
            2: (
                sum(
                    agg_num_triple_additions_per_revision_histogram[i]
                    for i in range(2, 9 + 1)
                )
                / s
            ),
            3: (
                sum(
                    agg_num_triple_additions_per_revision_histogram[i]
                    for i in range(10, 19 + 1)
                )
                / s
            ),
            4: (
                sum(
                    agg_num_triple_additions_per_revision_histogram[i]
                    for i in range(
                        20,
                        max(agg_num_triple_additions_per_revision_histogram.keys()) + 1,
                    )
                )
                / s
            ),
        }
        print("  \\addplot (additions)")  # --------------------------------------------
        for k, v in vs.items():
            print(f"    ({k},{v:.2f})")
        vs = {
            0: agg_num_triple_deletions_histogram[0] / s,
            1: agg_num_triple_deletions_histogram[1] / s,
            2: (
                sum(agg_num_triple_deletions_histogram[i] for i in range(2, 9 + 1)) / s
            ),
            3: (
                sum(agg_num_triple_deletions_histogram[i] for i in range(10, 19 + 1))
                / s
            ),
            4: (
                sum(
                    agg_num_triple_deletions_histogram[i]
                    for i in range(
                        20,
                        max(agg_num_triple_deletions_histogram.keys()) + 1,
                    )
                )
                / s
            ),
        }
        print("  \\addplot (deletions)")  # --------------------------------------------
        for k, v in vs.items():
            print(f"    ({k},{v:.2f})")

        # ==============================================================================
        print()
        print("figures/time-between-revisions.text")
        print("  agg_time_between_revisions_histogram = {")  # -------------------------
        for k in sorted(agg_time_between_revisions_histogram.keys()):
            print(f"    {k}: {agg_time_between_revisions_histogram[k]},")
        print("  }")
        s = sum(agg_time_between_revisions_histogram.values())
        vs = {k: agg_time_between_revisions_histogram[k] / s for k in range(0, 8)}
        print("  \\addplot")  # --------------------------------------------------------
        for k, v in vs.items():
            print(f"    ({k},{v:.2f})")
        print(f"    ({k + 1},{v:.2f})")
        print("  \\draw")  # -----------------------------------------------------------
        for k, v in vs.items():
            print(f"    (axis cs: {k + 0.5},{v:.2f}) node {{{v*100:.0f}\\%}}")

        # ==============================================================================
        print()
        print("figures/time-until-triple-add-del.tex")
        print("  agg_time_until_triple_inserted = {")  # -------------------------------
        for k in sorted(agg_time_until_triple_inserted.keys()):
            print(f"    {k}: {agg_time_until_triple_inserted[k]},")
        print("  }")
        print("  agg_time_until_triple_deleted = {")  # --------------------------------
        for k in sorted(agg_time_until_triple_deleted.keys()):
            print(f"    {k}: {agg_time_until_triple_deleted[k]},")
        print("  agg_time_until_triple_oscillated = {")  # -----------------------------
        for k in sorted(agg_time_until_triple_oscillated.keys()):
            print(f"    {k}: {agg_time_until_triple_oscillated[k]},")
        print("  }")
        s = sum(agg_time_until_triple_inserted.values())
        vs = {
            1: agg_time_until_triple_inserted[1] / s,
            2: (sum(agg_time_until_triple_inserted[i] for i in range(2, 30 + 1)) / s),
            3: (sum(agg_time_until_triple_inserted[i] for i in range(31, 180 + 1)) / s),
            4: (
                sum(agg_time_until_triple_inserted[i] for i in range(181, 365 + 1)) / s
            ),
            5: (
                sum(
                    agg_time_until_triple_inserted[i]
                    for i in range(366, max(agg_time_until_triple_inserted.keys()) + 1)
                )
                / s
            ),
        }
        print("  \\addplot (additions)")  # --------------------------------------------
        for k, v in vs.items():
            print(f"    ({k},{v:.2f})")
        s = sum(agg_time_until_triple_deleted.values())
        vs = {
            1: agg_time_until_triple_deleted[1] / s,
            2: (sum(agg_time_until_triple_deleted[i] for i in range(2, 30 + 1)) / s),
            3: (sum(agg_time_until_triple_deleted[i] for i in range(31, 180 + 1)) / s),
            4: (sum(agg_time_until_triple_deleted[i] for i in range(181, 365 + 1)) / s),
            5: (
                sum(
                    agg_time_until_triple_deleted[i]
                    for i in range(366, max(agg_time_until_triple_deleted.keys()) + 1)
                )
                / s
            ),
        }
        print("  \\addplot (deletions)")  # --------------------------------------------
        for k, v in vs.items():
            print(f"    ({k},{v:.2f})")

    @classmethod
    def _process_dump(
        cls,
        meta_history_dump: WikidataMetaHistoryDump,
        *,
        data_dir: Path,
        progress_callback: ParallelizeProgressCallback,
        **kwargs: object,
    ) -> Tuple[
        Counter[date],
        Counter[date],
        Counter[float],
        Counter[int],
        Counter[int],
        Counter[int],
        Counter[int],
        Counter[float],
        Counter[float],
        Counter[float],
    ]:
        statistics_file = wikidata_incremental_rdf_revision_dir(data_dir) / (
            meta_history_dump.path.name + ".statistics.json.gz"
        )

        progress_callback(meta_history_dump.path.name, 0, 1)

        assert statistics_file.exists()
        with gzip.open(statistics_file, "rt", encoding="UTF-8") as fin:
            statistics = WikidataDumpStatistics.parse_raw(fin.read())

        num_entities_per_month = Counter[date]()
        num_revisions_per_month = Counter[date]()
        num_revisions_per_entity_histogram = Counter[float]()
        time_between_revisions_histogram = Counter[int]()
        num_triple_additions_per_revision_histogram = Counter[int]()
        num_triple_deletions_per_revision_histogram = Counter[int]()
        num_triple_changes_histogram = Counter[int]()
        time_until_triple_inserted = Counter[float]()
        time_until_triple_deleted = Counter[float]()
        time_until_triple_oscillated = Counter[float]()

        for num_revisions in statistics.num_revisions_per_page:
            num_revisions_per_entity_histogram[
                ceil(log10(num_revisions)) if num_revisions != 0 else 0
            ] += 1

        for month_string, revision_statistics in statistics.per_month.items():
            month = datetime.strptime(month_string, "%Y-%m").date()

            for revision_statistic in revision_statistics:
                num_revisions_per_month[month] += 1
                if revision_statistic.time_since_last_revision is None:
                    num_entities_per_month[month] += 1
                else:
                    time_since_last_revision = (
                        revision_statistic.time_since_last_revision
                    )
                    if time_since_last_revision <= _SECOND:
                        b = 0
                    elif time_since_last_revision <= _MINUTE:
                        b = 1
                    elif time_since_last_revision <= _HOUR:
                        b = 2
                    elif time_since_last_revision <= _DAY:
                        b = 3
                    elif time_since_last_revision <= _WEEK:
                        b = 4
                    elif time_since_last_revision <= _MONTH:
                        b = 5
                    elif time_since_last_revision <= _YEAR:
                        b = 6
                    else:
                        b = 7
                    time_between_revisions_histogram[b] += 1

                num_triple_additions_per_revision_histogram[
                    revision_statistic.num_added_triples
                ] += 1
                num_triple_deletions_per_revision_histogram[
                    revision_statistic.num_deleted_triples
                ] += 1

                for (
                    num_triple_changes,
                    times_since_last_change,
                ) in (
                    revision_statistic.num_triple_changes_to_time_since_last_change.items()
                ):
                    num_triple_changes_histogram[num_triple_changes] += len(
                        times_since_last_change
                    )
                    for time_since_last_change in times_since_last_change:
                        b = max(ceil(time_since_last_change / _DAY), 1)
                        if num_triple_changes == 1:
                            time_until_triple_inserted[b] += 1
                        elif num_triple_changes == 2:
                            time_until_triple_deleted[b] += 1
                        else:
                            time_until_triple_oscillated[b] += 1

        progress_callback(meta_history_dump.path.name, 1, 1)

        return (
            num_entities_per_month,
            num_revisions_per_month,
            num_revisions_per_entity_histogram,
            time_between_revisions_histogram,
            num_triple_additions_per_revision_histogram,
            num_triple_deletions_per_revision_histogram,
            num_triple_changes_histogram,
            time_until_triple_inserted,
            time_until_triple_deleted,
            time_until_triple_oscillated,
        )

    @classmethod
    def _log_bin_frequency(cls, value: Union[int, float]) -> float:
        return round(log10(value), 1) if value != 0 else 0

    @classmethod
    def _format_binned_timedelta(cls, value: timedelta) -> str:
        if value >= _YEAR:
            return f"{value / _YEAR} years"
        elif value >= _MONTH:
            return f"{value / _MONTH} months"
        elif value >= _DAY:
            return f"{value / _DAY} days"
        elif value >= _HOUR:
            return f"{value / _HOUR} hours"
        elif value >= _MINUTE:
            return f"{value / _MINUTE} minutes"
        else:
            return f"{value / _SECOND} seconds"


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataCollectStatisticsProgram.init(*args).run()


if __name__ == "__main__":
    main()

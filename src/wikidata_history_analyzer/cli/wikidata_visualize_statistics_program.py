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
from math import ceil, sqrt
from pathlib import Path
from sys import argv, stdout
from typing import Counter, Optional, Sequence, Tuple, Union, cast

import numpy as np
from nasty_utils import ColoredBraceStyleAdapter, ProgramConfig
from overrides import overrides
from statsmodels.stats.weightstats import DescrStatsW  # type: ignore
from welford import Welford  # type: ignore

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
        agg_num_revisions_per_entity = Counter[int]()
        agg_num_revisions_per_entity_welford = None
        agg_time_between_revisions = Counter[int]()
        agg_days_between_revisions = Counter[int]()
        agg_time_between_revisions_welford = None
        agg_num_triple_additions_per_revision = Counter[int]()
        agg_num_triple_additions_per_revision_welford = None
        agg_num_triple_deletions_per_revision = Counter[int]()
        agg_num_triple_deletions_per_revision_welford = None
        agg_num_triple_changes = Counter[int]()
        agg_days_until_triple_inserted = Counter[int]()
        agg_time_until_triple_inserted_welford = None
        agg_days_until_triple_deleted = Counter[int]()
        agg_time_until_triple_deleted_welford = None
        agg_days_until_triple_oscillated = Counter[int]()
        agg_time_until_triple_oscillated_welford = None

        for (
            num_entities_per_month,
            num_revisions_per_month,
            num_revisions_per_entity,
            num_revisions_per_entity_welford,
            time_between_revisions,
            days_between_revisions,
            time_between_revisions_welford,
            num_triple_additions_per_revision,
            num_triple_additions_per_revision_welford,
            num_triple_deletions_per_revision,
            num_triple_deletions_per_revision_welford,
            num_triple_changes,
            days_until_triple_inserted,
            time_until_triple_inserted_welford,
            days_until_triple_deleted,
            time_until_triple_deleted_welford,
            days_until_triple_oscillated,
            time_until_triple_oscillated_welford,
        ) in parallelize(
            cast(
                ParallelizeCallback[
                    WikidataMetaHistoryDump,
                    Tuple[
                        Counter[date],
                        Counter[date],
                        Counter[int],
                        Welford,
                        Counter[int],
                        Counter[int],
                        Welford,
                        Counter[int],
                        Welford,
                        Counter[int],
                        Welford,
                        Counter[int],
                        Counter[int],
                        Welford,
                        Counter[int],
                        Welford,
                        Counter[int],
                        Welford,
                    ],
                ],
                self._process_dump,
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

            agg_num_revisions_per_entity += num_revisions_per_entity
            if agg_num_revisions_per_entity_welford is None:
                agg_num_revisions_per_entity_welford = num_revisions_per_entity_welford
            else:
                agg_num_revisions_per_entity_welford.merge(
                    num_revisions_per_entity_welford
                )

            agg_time_between_revisions += time_between_revisions
            agg_days_between_revisions += days_between_revisions
            if agg_time_between_revisions_welford is None:
                agg_time_between_revisions_welford = time_between_revisions_welford
            else:
                agg_time_between_revisions_welford.merge(time_between_revisions_welford)

            agg_num_triple_additions_per_revision += num_triple_additions_per_revision
            if agg_num_triple_additions_per_revision_welford is None:
                agg_num_triple_additions_per_revision_welford = (
                    num_triple_additions_per_revision_welford
                )
            else:
                agg_num_triple_additions_per_revision_welford.merge(
                    num_triple_additions_per_revision_welford
                )

            agg_num_triple_deletions_per_revision += num_triple_deletions_per_revision
            if agg_num_triple_deletions_per_revision_welford is None:
                agg_num_triple_deletions_per_revision_welford = (
                    num_triple_deletions_per_revision_welford
                )
            else:
                agg_num_triple_deletions_per_revision_welford.merge(
                    num_triple_deletions_per_revision_welford
                )

            agg_num_triple_changes += num_triple_changes

            agg_days_until_triple_inserted += days_until_triple_inserted
            if agg_time_until_triple_inserted_welford is None:
                agg_time_until_triple_inserted_welford = (
                    time_until_triple_inserted_welford
                )
            else:
                agg_time_until_triple_inserted_welford.merge(
                    time_until_triple_inserted_welford
                )

            agg_days_until_triple_deleted += days_until_triple_deleted
            if agg_time_until_triple_deleted_welford is None:
                agg_time_until_triple_deleted_welford = (
                    time_until_triple_deleted_welford
                )
            else:
                agg_time_until_triple_deleted_welford.merge(
                    time_until_triple_deleted_welford
                )

            agg_days_until_triple_oscillated += days_until_triple_oscillated
            if agg_time_until_triple_oscillated_welford is None:
                agg_time_until_triple_oscillated_welford = (
                    time_until_triple_oscillated_welford
                )
            else:
                agg_time_until_triple_oscillated_welford.merge(
                    time_until_triple_oscillated_welford
                )

        def print_figure_name(name: str) -> None:
            stdout.write(f"\nfigures/{name}.tex\n")

        def print_figure_desc(desc: str) -> None:
            stdout.write(f"  {desc}\n")

        def print_histogram_raw(
            name: str, histogram: Union[Counter[int], Counter[date]]
        ) -> None:
            stdout.write(f"  {name} = {{\n")
            keys = sorted(histogram.keys())
            for key in keys:
                key_str = (
                    str(key)
                    if not isinstance(key, date)
                    else f"date({key.year}, {key.month}, {key.day})"
                )
                value = histogram[key]
                stdout.write(f"    {key_str}: {value},\n")
            stdout.write("  }\n")

        def print_histogram_binned(
            *,
            histogram: Counter[int],
            ranges: Sequence[Tuple[Optional[int], Optional[int]]],
            normalize: bool,
            precision: int,
        ) -> None:
            normalization_factor = sum(histogram.values()) if normalize else 1
            key_min = min(histogram.keys())
            key_max = max(histogram.keys())
            values = (
                sum(
                    histogram[key] / normalization_factor
                    for key in range(
                        lower_bound if lower_bound is not None else key_min,
                        (upper_bound if upper_bound is not None else key_max) + 1,
                    )
                )
                for lower_bound, upper_bound in ranges
            )
            for i, value in enumerate(values):
                stdout.write(f"    ({i}," + f"{{:.{precision}f}}".format(value) + ")\n")

        def print_histogram_statistics(histogram: Counter[int]) -> None:
            # Not setting `ddof=1` here to not use Bessel's correction since we are
            # calculating the standard deviation over the entire population of all
            # revisions and not just a sample of it.
            stats = DescrStatsW(
                data=np.array(list(histogram.keys()), dtype=np.float32),
                weights=np.array(list(histogram.values()), dtype=np.float32),
            )
            stdout.write(f"    mean: {float(stats.mean):.4f}\n")
            stdout.write(f"    std: {float(stats.std):.4f}\n")
            stdout.write(f"    std_mean: {float(stats.std_mean):.4f}\n")
            stdout.write(f"    1%-quantile: {int(stats.quantile(0.01))}\n")
            stdout.write(f"    2.5%-quantile: {int(stats.quantile(0.025))}\n")
            stdout.write(f"    25%-quantile: {int(stats.quantile(0.25))}\n")
            stdout.write(f"    median: {int(stats.quantile(0.5))}\n")
            stdout.write(f"    75%-quantile: {int(stats.quantile(0.75))}\n")
            stdout.write(f"    97.5%-quantile: {int(stats.quantile(0.975))}\n")
            stdout.write(f"    99%-quantile: {int(stats.quantile(0.99))}\n")

        def print_welford_statistics(welford: Welford) -> None:
            # Using `var_s` instead of `var_p` here for same reason as in
            # print_histogram_statistics().
            stdout.write(f"    welford mean: {welford.mean.item():.4f}\n")
            stdout.write(f"    welford std: {sqrt(welford.var_s.item()):.4f}\n")

        # ==============================================================================
        print_figure_name("num-dels-per-triple")
        print_histogram_raw(
            "agg_num_triple_changes",
            agg_num_triple_changes,
        )
        agg_num_dels_per_triple = Counter[int]()
        agg_num_dels_per_triple[0] = (
            agg_num_triple_changes[1] - agg_num_triple_changes[2]
        )
        for i in range(2, max(agg_num_triple_changes.keys()) + 1, 2):
            value = agg_num_triple_changes[i] - agg_num_triple_changes[i + 2]
            if value:
                agg_num_dels_per_triple[i // 2] = value
        print_figure_desc("\\addplot")
        print_histogram_binned(
            histogram=agg_num_dels_per_triple,
            ranges=[(0, 0), (1, 1), (2, 9), (10, None)],
            normalize=False,
            precision=0,
        )
        print_figure_desc("\\desc")
        stdout.write("    Need to do manually.\n")
        print_figure_desc("statistics")
        print_histogram_statistics(agg_num_dels_per_triple)

        # ==============================================================================
        print_figure_name("num-entities-revisions-over-time")
        print_histogram_raw(
            "agg_num_entities_per_month",
            agg_num_entities_per_month,
        )
        print_histogram_raw(
            "agg_num_revisions_per_month",
            agg_num_revisions_per_month,
        )
        values = {}
        for year in range(2012, 2021):
            values[year] = (values[year - 1] if year != 2012 else 0) + sum(
                agg_num_revisions_per_month[date(year, month, 1)]
                for month in range(1, 12 + 1)
            )
        print_figure_desc("\\addplot (revisions)")
        for key, value in values.items():
            stdout.write(f"    ({key}-01-01,{value})\n")
        values = {}
        for year in range(2012, 2021):
            values[year] = (values[year - 1] if year != 2012 else 0) + sum(
                agg_num_entities_per_month[date(year, month, 1)]
                for month in range(1, 12 + 1)
            )
        print_figure_desc("\\addplot (entities)")
        for key, value in values.items():
            stdout.write(f"    ({key}-01-01,{value})\n")
        stdout.write(
            f"Total number of entities: {sum(agg_num_entities_per_month.values())}\n"
        )
        stdout.write(
            f"Total number of revisions: {sum(agg_num_revisions_per_month.values())}\n"
        )

        # ==============================================================================
        print_figure_name("num-revisions-per-entity")
        print_histogram_raw(
            "agg_num_revisions_per_entity",
            agg_num_revisions_per_entity,
        )
        print_figure_desc("\\addplot")
        print_histogram_binned(
            histogram=agg_num_revisions_per_entity,
            ranges=[(1, 1), (2, 9), (10, 99), (100, None)],
            normalize=True,
            precision=3,
        )
        print_figure_desc("statistics")
        print_histogram_statistics(agg_num_revisions_per_entity)
        print_welford_statistics(agg_num_revisions_per_entity_welford)

        # ==============================================================================
        print_figure_name("num-triple-adds-dels-per-revision")
        print_histogram_raw(
            "agg_num_triple_additions_per_revision",
            agg_num_triple_additions_per_revision,
        )
        print_histogram_raw(
            "agg_num_triple_deletions_per_revision",
            agg_num_triple_deletions_per_revision,
        )
        print_figure_desc("\\addplot (additions)")
        print_histogram_binned(
            histogram=agg_num_triple_additions_per_revision,
            ranges=[(0, 0), (1, 1), (2, 9), (10, None)],
            normalize=True,
            precision=2,
        )
        print_figure_desc("\\addplot (deletions)")
        print_histogram_binned(
            histogram=agg_num_triple_deletions_per_revision,
            ranges=[(0, 0), (1, 1), (2, 9), (10, None)],
            normalize=True,
            precision=2,
        )
        print_figure_desc("statistics (additions)")
        print_histogram_statistics(agg_num_triple_additions_per_revision)
        print_welford_statistics(agg_num_triple_additions_per_revision_welford)
        print_figure_desc("statistics (deletions)")
        print_histogram_statistics(agg_num_triple_deletions_per_revision)
        print_welford_statistics(agg_num_triple_deletions_per_revision_welford)

        # ==============================================================================
        print_figure_name("time-between-revisions")
        print_histogram_raw(
            "agg_time_between_revisions",
            agg_time_between_revisions,
        )
        normalization_factor = sum(agg_time_between_revisions.values())
        values = {
            key: agg_time_between_revisions[key] / normalization_factor
            for key in range(0, 8)
        }
        print_figure_desc("\\addplot")
        for key, value in values.items():
            stdout.write(f"    ({key},{value:.4f})\n")
        stdout.write(f"    ({key + 1},{value:.4f})\n")
        print_figure_desc("\\draw")
        for key, value in values.items():
            stdout.write(
                f"    (axis cs: {key + 0.5},{value:.4f}) node {{{value * 100:.1f}\\%}}\n"
            )
        print_figure_desc("statistics")
        print_histogram_statistics(agg_days_between_revisions)
        print_welford_statistics(agg_time_between_revisions_welford)

        # ==============================================================================
        print_figure_name("time-until-triple-add-del")
        print_histogram_raw(
            "agg_days_until_triple_inserted",
            agg_days_until_triple_inserted,
        )
        print_histogram_raw(
            "agg_days_until_triple_deleted",
            agg_days_until_triple_deleted,
        )
        print_histogram_raw(
            "agg_days_until_triple_oscillated",
            agg_days_until_triple_oscillated,
        )
        print_figure_desc("\\addplot (additions)")
        print_histogram_binned(
            histogram=agg_days_until_triple_inserted,
            ranges=[(1, 1), (2, 30), (31, 180), (181, 365), (366, None)],
            normalize=True,
            precision=2,
        )
        print_figure_desc("\\addplot (deletions)")
        print_histogram_binned(
            histogram=agg_days_until_triple_deleted,
            ranges=[(1, 1), (2, 30), (31, 180), (181, 365), (366, None)],
            normalize=True,
            precision=2,
        )
        print_figure_desc("statistics (additions)")
        print_histogram_statistics(agg_days_until_triple_inserted)
        print_welford_statistics(agg_time_until_triple_inserted_welford)
        print_figure_desc("statistics (deletions)")
        print_histogram_statistics(agg_days_until_triple_deleted)
        print_welford_statistics(agg_time_until_triple_deleted_welford)

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
        Counter[int],
        Welford,
        Counter[int],
        Counter[int],
        Welford,
        Counter[int],
        Welford,
        Counter[int],
        Welford,
        Counter[int],
        Counter[int],
        Welford,
        Counter[int],
        Welford,
        Counter[int],
        Welford,
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
        num_revisions_per_entity = Counter[int]()
        num_revisions_per_entity_welford = Welford()
        time_between_revisions = Counter[int]()
        days_between_revisions = Counter[int]()
        time_between_revisions_welford = Welford()
        num_triple_additions_per_revision = Counter[int]()
        num_triple_additions_per_revision_welford = Welford()
        num_triple_deletions_per_revision = Counter[int]()
        num_triple_deletions_per_revision_welford = Welford()
        num_triple_changes = Counter[int]()
        days_until_triple_inserted = Counter[int]()
        time_until_triple_inserted_welford = Welford()
        days_until_triple_deleted = Counter[int]()
        time_until_triple_deleted_welford = Welford()
        days_until_triple_oscillated = Counter[int]()
        time_until_triple_oscillated_welford = Welford()

        for num_revisions in statistics.num_revisions_per_page:
            num_revisions_per_entity[num_revisions] += 1
            num_revisions_per_entity_welford.add(np.array([num_revisions]))

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
                    time_between_revisions[b] += 1
                    days_between_revisions[
                        max(ceil(time_since_last_revision / _DAY), 1)
                    ] += 1
                    time_between_revisions_welford.add(
                        np.array([time_since_last_revision.total_seconds()])
                    )

                num_triple_additions_per_revision[
                    revision_statistic.num_added_triples
                ] += 1
                num_triple_additions_per_revision_welford.add(
                    np.array([revision_statistic.num_added_triples])
                )
                num_triple_deletions_per_revision[
                    revision_statistic.num_deleted_triples
                ] += 1
                num_triple_deletions_per_revision_welford.add(
                    np.array([revision_statistic.num_deleted_triples])
                )

                for (
                    num_triple_changes_value,
                    times_since_last_change,
                ) in (
                    revision_statistic.num_triple_changes_to_time_since_last_change.items()
                ):
                    num_triple_changes[num_triple_changes_value] += len(
                        times_since_last_change
                    )
                    for time_since_last_change in times_since_last_change:
                        b = max(ceil(time_since_last_change / _DAY), 1)
                        if num_triple_changes_value == 1:
                            days_until_triple_inserted[b] += 1
                            time_until_triple_inserted_welford.add(
                                np.array([time_since_last_change.total_seconds()])
                            )
                        elif num_triple_changes_value == 2:
                            days_until_triple_deleted[b] += 1
                            time_until_triple_deleted_welford.add(
                                np.array([time_since_last_change.total_seconds()])
                            )
                        else:
                            days_until_triple_oscillated[b] += 1
                            time_until_triple_oscillated_welford.add(
                                np.array([time_since_last_change.total_seconds()])
                            )

        progress_callback(meta_history_dump.path.name, 1, 1)

        return (
            num_entities_per_month,
            num_revisions_per_month,
            num_revisions_per_entity,
            num_revisions_per_entity_welford,
            time_between_revisions,
            days_between_revisions,
            time_between_revisions_welford,
            num_triple_additions_per_revision,
            num_triple_additions_per_revision_welford,
            num_triple_deletions_per_revision,
            num_triple_deletions_per_revision_welford,
            num_triple_changes,
            days_until_triple_inserted,
            time_until_triple_inserted_welford,
            days_until_triple_deleted,
            time_until_triple_deleted_welford,
            days_until_triple_oscillated,
            time_until_triple_oscillated_welford,
        )


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataCollectStatisticsProgram.init(*args).run()


if __name__ == "__main__":
    main()

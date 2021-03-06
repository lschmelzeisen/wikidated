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

from datetime import datetime, timedelta
from logging import getLogger
from math import ceil, floor
from pathlib import Path
from sys import maxsize
from typing import (
    Counter,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import numpy as np
from pylatex import (  # type: ignore
    Axis,
    Command,
    Document,
    NoEscape,
    Plot,
    TikZ,
    TikZCoordinate,
    TikZNode,
    TikZOptions,
    TikZScope,
)
from pylatex.errors import PyLaTeXError  # type: ignore
from statsmodels.stats.weightstats import DescrStatsW  # type: ignore
from tqdm import tqdm  # type: ignore

from wikidated.wikidata import WikidataRdfTriple
from wikidated.wikidated_manager import WikidatedManager
from wikidated.wikidated_revision import WikidatedRevision
from wikidated.wikidated_v1_0 import WikidatedV1_0Dataset

_DAY = timedelta(days=1)
_LOGGER = getLogger(__name__)


class StandaloneTikZ(TikZ):  # type: ignore
    # Class that automatically adds the trim options to the TikZ environment if we are
    # not in a standalone environment.

    def __init__(self) -> None:
        super().__init__(
            options=TikZOptions("baseline", "trim axis left", "trim axis right")
        )

    def dumps(self) -> object:
        orig_begin, orig_content_and_end = (
            super().dumps().split(self.content_separator, maxsplit=1)
        )
        result = (
            Command("ifstandalone").dumps(),
            Command("begin", self.latex_name).dumps(),
            Command("else").dumps(),
            orig_begin,
            Command("fi").dumps(),
            orig_content_and_end,
            # We add the following such that editor highlighting doesn't break.
            Command("iffalse").dumps(),
            Command("end", self.latex_name).dumps(),
            Command("fi").dumps(),
        )
        return self.content_separator.join(result)


class FigureBase(Document):  # type: ignore
    _BARS_REVISIONS_OPTIONS = (
        r"thick",
        r"draw=colorRevisions",
        r"fill=colorRevisions!33",
    )
    _BARS_ADDITIONS_OPTIONS = (
        r"thick",
        r"draw=colorAdditions",
        r"preaction={fill=colorAdditions!33}",
        r"pattern=crosshatch dots",
        r"pattern color=colorAdditions",
    )
    _BARS_DELETIONS_OPTIONS = (
        r"thick",
        r"draw=colorDeletions",
        r"fill=colorDeletions!33",
    )

    _YTICK_PERCENT_OPTIONS = (
        r"ymin=0",
        r"yticklabel={\pgfmathparse{\tick*100}\pgfmathprintnumber{\pgfmathresult}\%},",
        r"ytick style={draw=none}",
        r"ymajorgrids",
    )
    _LABELS_AUTO_PERCENT_OPTIONS = (
        r"point meta={y*100}",
        r"nodes near coords={\pgfmathprintnumber\pgfplotspointmeta\%}",
        r"nodes near coords style={font=\footnotesize}",
    )

    def __init__(
        self,
        *,
        data_dir: Path,
        tikz_library: Optional[str] = None,
    ) -> None:
        super().__init__(
            default_filepath=(
                data_dir / self._camel_case_to_kebab_case(type(self).__name__)
            ),
            documentclass="standalone",
            fontenc=None,
            inputenc=None,
            lmodern=False,
            textcomp=False,
            page_numbers=False,
        )

        if tikz_library:
            self.preamble.append(Command("usetikzlibrary", tikz_library))
        self.preamble.append(
            Command(
                "pgfplotsset",
                NoEscape(r"legend style={font=\small},")
                + NoEscape(r"label style={font=\small},")
                + NoEscape(r"tick label style={font=\small},")
                # Only one legend bar, see: https://tex.stackexchange.com/a/224677/75225
                + NoEscape(
                    r"ybar legend/.style={legend image code/.code={"
                    r"\draw[##1,yshift=-0.4em] (0cm,0cm) rectangle (0.5em,1em);}},"
                ),
            )
        )

        self.add_color("colorEntities", "HTML", "E7298A")
        self.add_color("colorRevisions", "HTML", "7570B3")
        self.add_color("colorAdditions", "HTML", "1B9E77")
        self.add_color("colorDeletions", "HTML", "D95F02")

    @classmethod
    def _camel_case_to_kebab_case(cls, string: str) -> str:
        # From: https://stackoverflow.com/a/44969381
        return "".join(
            ["-" + char.lower() if char.isupper() else char for char in string]
        ).lstrip("-")

    @classmethod
    def _axis_size(cls, width: float) -> Sequence[str]:
        return fr"width=\ifstandalone{width}\fi\textwidth", r"height=2.75cm"

    @classmethod
    def _xticklabels(cls, labels: Sequence[str], *, vphantom: str = "") -> str:
        labels_with_vphantom = (fr"{label}\vphantom{{{vphantom}}}" for label in labels)
        return fr"xticklabels={{{','.join(labels_with_vphantom)}}}"

    @classmethod
    def _xticklabels_for_bins(
        cls,
        bin_intervals: Sequence[range],
        *,
        alt_style: bool = False,
        vphantom: str = "",
    ) -> str:
        def bin_label(bin_interval: range) -> str:
            if len(bin_interval) == 1:
                return f"{{{bin_interval[0]}}}"
            if bin_interval[-1] == maxsize - 1:
                return fr"{{$\geq$\,{bin_interval[0]}}}"
            return f"{{{bin_interval[0]}--{bin_interval[-1]}}}"

        def bin_label_alt(bin_interval: range) -> str:
            if bin_interval[0] == 0:
                return fr"{{$[{bin_interval[0]},{bin_interval[-1]}]$}}"
            if bin_interval[-1] == maxsize - 1:
                return r"{$(\cdot,\infty)$}"
            return fr"{{$(\cdot,{bin_interval[-1]}]$}}"

        return cls._xticklabels(
            [
                (bin_label if not alt_style else bin_label_alt)(bin_interval)
                for bin_interval in bin_intervals
            ],
            vphantom=vphantom,
        )

    def handle_revision(self, revision: WikidatedRevision) -> None:
        pass

    def log_statistics(self) -> None:
        _LOGGER.info(f"{self._camel_case_to_kebab_case(type(self).__name__)}:")

    @classmethod
    def _log_statistics_histogram(
        cls, name: str, histogram: Mapping[int, float]
    ) -> None:
        _LOGGER.info(f"  {name} = {{")
        for key, value in sorted(histogram.items()):
            _LOGGER.info(f"    {key} = {value},")
        _LOGGER.info("  }")

        # Not setting `ddof=1` here to not use Bessel's correction since we are
        # calculating the standard deviation over the entire population of all
        # revisions and not just a sample of it.
        statistics = DescrStatsW(
            data=np.array(list(histogram.keys()), dtype=np.float32),
            weights=np.array(list(histogram.values()), dtype=np.float32),
        )

        _LOGGER.info(f"  {name}_statistics = {{")
        _LOGGER.info(f"    mean: {float(statistics.mean):.4f}")
        _LOGGER.info(f"    std: {float(statistics.std):.4f}")
        _LOGGER.info(f"    std_mean: {float(statistics.std_mean):.4f}")
        _LOGGER.info(f"    1%-quantile: {int(statistics.quantile(0.01))}")
        _LOGGER.info(f"    2.5%-quantile: {int(statistics.quantile(0.025))}")
        _LOGGER.info(f"    25%-quantile: {int(statistics.quantile(0.25))}")
        _LOGGER.info(f"    median: {int(statistics.quantile(0.5))}")
        _LOGGER.info(f"    75%-quantile: {int(statistics.quantile(0.75))}")
        _LOGGER.info(f"    97.5%-quantile: {int(statistics.quantile(0.975))}")
        _LOGGER.info(f"    99%-quantile: {int(statistics.quantile(0.99))}")
        _LOGGER.info("  }")

    def generate(self) -> None:
        axis_options = self._generate_axis_options()
        if axis_options is None:
            axis_options = ()
        if isinstance(axis_options, Sequence):
            axis_options = {"default": axis_options}

        with self.create(StandaloneTikZ()) as tikz:
            for axis_name, axis_options_ in axis_options.items():
                with tikz.create(
                    Axis(options=TikZOptions("scale only axis", *axis_options_))
                ) as axis:
                    self._generate_axis_content(axis, axis_name)

        try:
            self.generate_pdf(clean_tex=False, silent=True)
        except PyLaTeXError:
            _LOGGER.exception("Generating PDF from LaTeX document failed.")

    def _generate_axis_options(
        self,
    ) -> Union[None, Sequence[str], Mapping[str, Sequence[str]]]:
        pass

    def _generate_axis_content(self, axis: Axis, axis_name: str) -> None:
        pass


class FigureNumEntitiesAndRevisionsByYear(FigureBase):
    def __init__(
        self,
        data_dir: Path,
        min_year: int = 2012,
        max_year: int = 2020,
    ) -> None:
        self._last_page_id = 0
        self._min_year = min_year
        self._max_year = max_year
        self._num_entities_total = 0
        self._num_revisions_total = 0
        self._num_entities_by_year_counter = Counter[int]()
        self._num_revisions_by_year_counter = Counter[int]()
        super().__init__(data_dir=data_dir, tikz_library="pgfplots.dateplot")

    def handle_revision(self, revision: WikidatedRevision) -> None:
        year = revision.timestamp.year
        if year < self._min_year or year > self._max_year:
            return

        if self._last_page_id != revision.page_id:
            self._last_page_id = revision.page_id
            self._num_entities_total += 1
            for y in range(year, self._max_year + 1):
                self._num_entities_by_year_counter[y] += 1

        self._num_revisions_total += 1
        for y in range(year, self._max_year + 1):
            self._num_revisions_by_year_counter[y] += 1

    def log_statistics(self) -> None:
        super().log_statistics()

        _LOGGER.info(f"  min_year = {self._min_year}")
        _LOGGER.info(f"  min_year = {self._max_year}")
        _LOGGER.info(f"  num_entities_total = {self._num_entities_total}")
        _LOGGER.info(f"  num_revisions_total = {self._num_revisions_total}")
        _LOGGER.info("  num_entities_by_year_counter = {")
        for year, num_entities in sorted(self._num_entities_by_year_counter.items()):
            _LOGGER.info(f"    {year}: {num_entities},")
        _LOGGER.info("  }")
        _LOGGER.info("  num_revisions_by_year_counter = {")
        for year, num_revisions in sorted(self._num_revisions_by_year_counter.items()):
            _LOGGER.info(f"    {year}: {num_revisions},")
        _LOGGER.info("  }")

    def _generate_axis_options(self) -> Mapping[str, Sequence[str]]:
        shared_axis_options = (
            *self._axis_size(0.4),
            r"set layers",
            r"date coordinates in=x",
            r"enlargelimits=0.05",
            fr"xmin={self._min_year}-12-31",
            fr"xmax={self._max_year}-12-31",
            r"ymin=0",
            r"scaled y ticks=false",
            r"ylabel style={align=center}",
            r"major y tick style={draw=none}",
        )
        return {
            "axis:revisions": (
                *shared_axis_options,
                r"axis x line=none",
                r"axis y line*=right",
                r"ylabel={Number of revisions\\in billions}",
                r"ymax=1.5e9",
                r"ytick={0,3e8,6e8,9e8,1.2e9,1.5e9}",
                r"yticklabels={0,0.3,0.6,0.9,1.2,1.5}",
                r"minor y tick num=2",
            ),
            "axis:entities": (
                *shared_axis_options,
                r"xtick={2012-12-31,2013-12-31,2014-12-31,2015-12-31,"
                r"2016-12-31,2017-12-31,2018-12-31,2019-12-31,2020-12-31}",
                self._xticklabels(
                    ("2012", "", "2014", "", "2016", "", "2018", "", "2020"),
                    vphantom=r"$\geq$",
                ),
                r"xtick pos=bottom",
                r"xtick align=outside",
                r"xlabel={End of year\vphantom{p}}",
                r"grid=major",
                r"axis y line*=left",
                r"ylabel={Number of entities\\in millions}",
                r"ymax=1e8",
                r"ylabel shift=-0.05cm",
                r"ytick={0,2e7,4e7,6e7,8e7,1e8}",
                r"yticklabels={0,20,40,60,80,100}",
                r"minor y tick num=3",
                r"legend cell align={left}",
                r"legend style={at={(0.08, 0.96)}, anchor=north west}",
            ),
        }

    def _generate_axis_content(self, axis: Axis, axis_name: str) -> None:
        if axis_name == "axis:revisions":
            bars = Plot(
                options=TikZOptions(
                    r"colorRevisions",
                    r"densely dashed",
                    r"thick",
                    r"mark=*",
                    r"mark options={solid, fill=colorRevisions!33}",
                ),
                coordinates=[
                    (f"{year}-12-31", num)
                    for year, num in sorted(self._num_revisions_by_year_counter.items())
                ],
            )
            axis.append(bars)
            axis.append(Command("label", "axis:revisions"))

        elif axis_name == "axis:entities":
            bars = Plot(
                options=TikZOptions(
                    r"colorEntities",
                    r"solid",
                    r"thick",
                    r"mark=triangle*",
                    r"mark options={fill=colorEntities!33}",
                ),
                coordinates=[
                    (f"{year}-12-31", num)
                    for year, num in sorted(self._num_entities_by_year_counter.items())
                ],
            )
            axis.append(bars)
            axis.append(Command("addlegendentry", "Entities"))
            axis.append(Command("addlegendimage", "/pgfplots/refstyle=axis:revisions"))
            axis.append(Command("addlegendentry", "Revisions"))

        else:
            raise ValueError(f"Unknown axis_name '{axis_name}'.")


class FigureNumRevisionsPerEntity(FigureBase):
    def __init__(
        self,
        data_dir: Path,
        bin_intervals: Sequence[range] = (
            range(1, 2),
            range(2, 10),
            range(10, 100),
            range(100, maxsize),
        ),
    ) -> None:
        self._last_page_id = 0
        self._num_revisions_of_cur_entity = 0
        self._num_revisions_counter = Counter[int]()
        self._bin_intervals = bin_intervals
        self._bin_data = [0.0 for _ in range(len(self._bin_intervals))]
        super().__init__(data_dir=data_dir)

    def handle_revision(self, revision: WikidatedRevision) -> None:
        if self._last_page_id != revision.page_id:
            if self._last_page_id != 0:
                self._num_revisions_counter[self._num_revisions_of_cur_entity] += 1
            self._num_revisions_of_cur_entity = 0
            self._last_page_id = revision.page_id
        self._num_revisions_of_cur_entity += 1

    def log_statistics(self) -> None:
        super().log_statistics()

        self._num_revisions_counter[self._num_revisions_of_cur_entity] += 1
        self._num_revisions_of_cur_entity = 0

        for num_revisions, count in self._num_revisions_counter.items():
            for i, bin_interval in enumerate(self._bin_intervals):
                if num_revisions in bin_interval:
                    self._bin_data[i] += count
                    break

        num_entities = sum(self._num_revisions_counter.values())
        self._bin_data = [b / num_entities for b in self._bin_data]

        self._log_statistics_histogram(
            "num_revisions_counter", self._num_revisions_counter
        )
        _LOGGER.info(f"  bin_intervals = {self._bin_intervals}")
        _LOGGER.info(f"  bin_data = {self._bin_data}")

    def _generate_axis_options(self) -> Sequence[str]:
        return (
            *self._axis_size(0.4),
            r"ybar",
            r"bar width=0.9cm",
            r"enlarge x limits=0.2",
            r"xlabel={Number of revisions per entity}",
            fr"xtick={{0,...,{len(self._bin_intervals) - 1}}}",
            self._xticklabels_for_bins(self._bin_intervals, vphantom=r"$\geq$"),
            r"xtick pos=bottom",
            r"xtick align=outside",
            r"enlarge y limits={upper, value=0.2}",
            r"ylabel={Relative frequency}",
            r"ylabel near ticks",
            r"yticklabel pos=right",
            *self._YTICK_PERCENT_OPTIONS,
            *self._LABELS_AUTO_PERCENT_OPTIONS,
        )

    def _generate_axis_content(self, axis: Axis, axis_name: str) -> None:
        bars = Plot(
            options=TikZOptions(*self._BARS_REVISIONS_OPTIONS),
            coordinates=[(i, b) for i, b in enumerate(self._bin_data)],
        )
        axis.append(bars)


class FigureTimedeltaBetweenRevisions(FigureBase):
    def __init__(
        self,
        data_dir: Path,
        bin_boundaries: Sequence[Tuple[str, timedelta]] = (
            ("1 s", timedelta(seconds=1)),
            ("1 min", timedelta(minutes=1)),
            ("1 h", timedelta(hours=1)),
            ("1 d", timedelta(days=1)),
            ("7 d", timedelta(days=7)),
            ("30 d", timedelta(days=30)),
            ("1 yr", timedelta(days=365)),
        ),
    ) -> None:
        self._last_page_id = 0
        self._last_revision_timestamp = datetime.min
        self._days_between_revisions_counter = Counter[int]()
        self._bin_boundaries = bin_boundaries
        self._bin_data = [0.0 for _ in range(len(self._bin_boundaries) + 1)]
        super().__init__(data_dir=data_dir)

    def handle_revision(self, revision: WikidatedRevision) -> None:
        if self._last_page_id != revision.page_id:
            self._last_page_id = revision.page_id
        else:
            timedelta_since_last_revision = (
                revision.timestamp - self._last_revision_timestamp
            )
            self._days_between_revisions_counter[
                floor(timedelta_since_last_revision / _DAY)
            ] += 1
            for i, (_, bin_boundary) in enumerate(self._bin_boundaries):
                if timedelta_since_last_revision < timedelta(0):
                    break
                if timedelta_since_last_revision <= bin_boundary:
                    self._bin_data[i] += 1
                    break
            else:
                self._bin_data[-1] += 1

        self._last_revision_timestamp = revision.timestamp

    def log_statistics(self) -> None:
        super().log_statistics()

        num_timedeltas = sum(self._bin_data)
        self._bin_data = [b / num_timedeltas for b in self._bin_data]

        self._log_statistics_histogram(
            "days_between_revisions_counter", self._days_between_revisions_counter
        )
        _LOGGER.info(f"  bin_boundaries = {self._bin_boundaries}")
        _LOGGER.info(f"  bin_data = {self._bin_data}")

    def _generate_axis_options(self) -> Sequence[str]:
        return (
            *self._axis_size(0.55),
            r"bar width=0.9cm",
            r"enlarge x limits=0.04",
            r"xlabel=" r"{Time between consecutive revs. of same entity\vphantom{/}}",
            fr"xtick={{0,...,{len(self._bin_boundaries) + 1}}}",
            self._xticklabels(
                (
                    "0",
                    *(b.replace(" ", r"\,") for (b, _) in self._bin_boundaries),
                    r"$\infty$",
                ),
                vphantom=r"1y$\geq$",
            ),
            r"xtick pos=bottom",
            r"xtick align=outside",
            r"enlarge y limits={upper, value=0.2}",
            r"ylabel={Relative frequency}",
            *self._YTICK_PERCENT_OPTIONS,
        )

    def _generate_axis_content(self, axis: Axis, axis_name: str) -> None:
        bars = Plot(
            options=TikZOptions(r"ybar interval", *self._BARS_REVISIONS_OPTIONS),
            coordinates=[(i, b) for i, b in enumerate(self._bin_data)],
        )
        # Repeat last coordinate.
        bars.coordinates.append((bars.coordinates[-1][0] + 1, bars.coordinates[-1][1]))
        axis.append(bars)

        labels = TikZScope(
            options=TikZOptions(r"anchor=south", r"font=\footnotesize"),
            data=[
                TikZNode(
                    at=TikZCoordinate(i + 0.5, b), text=f"{b:.1%}".replace("%", r"\%")
                )
                for i, b in enumerate(self._bin_data)
            ],
        )
        axis.append(labels)


class FigureNumTripleAdditionsAndDeletionsPerRevision(FigureBase):
    def __init__(
        self,
        data_dir: Path,
        bin_intervals: Sequence[range] = (
            range(0, 1),
            range(1, 2),
            range(2, 10),
            range(10, maxsize),
        ),
    ) -> None:
        self._num_additions_per_revision_counter = Counter[int]()
        self._num_deletions_per_revision_counter = Counter[int]()
        self._bin_intervals = bin_intervals
        self._bin_data_additions = [0.0 for _ in range(len(self._bin_intervals))]
        self._bin_data_deletions = [0.0 for _ in range(len(self._bin_intervals))]
        super().__init__(data_dir=data_dir, tikz_library="patterns")

    def handle_revision(self, revision: WikidatedRevision) -> None:
        self._num_additions_per_revision_counter[len(revision.triple_additions)] += 1
        self._num_deletions_per_revision_counter[len(revision.triple_deletions)] += 1

    def log_statistics(self) -> None:
        super().log_statistics()

        for num_additions, count in self._num_additions_per_revision_counter.items():
            for i, bin_interval in enumerate(self._bin_intervals):
                if num_additions in bin_interval:
                    self._bin_data_additions[i] += count
                    break

        for num_deletions, count in self._num_deletions_per_revision_counter.items():
            for i, bin_interval in enumerate(self._bin_intervals):
                if num_deletions in bin_interval:
                    self._bin_data_deletions[i] += count
                    break

        num_revisions = sum(self._num_additions_per_revision_counter.values())
        assert num_revisions == sum(self._num_deletions_per_revision_counter.values())
        self._bin_data_additions = [b / num_revisions for b in self._bin_data_additions]
        self._bin_data_deletions = [b / num_revisions for b in self._bin_data_deletions]

        self._log_statistics_histogram(
            "num_additions_per_revision_counter",
            self._num_additions_per_revision_counter,
        )
        self._log_statistics_histogram(
            "num_deletions_per_revision_counter",
            self._num_deletions_per_revision_counter,
        )
        _LOGGER.info(f"  bin_intervals = {self._bin_intervals}")
        _LOGGER.info(f"  bin_data_additions = {self._bin_data_additions}")
        _LOGGER.info(f"  bin_data_deletions = {self._bin_data_deletions}")

    def _generate_axis_options(self) -> Sequence[str]:
        return (
            *self._axis_size(0.4),
            r"ybar",
            r"bar width=0.45cm",
            r"enlarge x limits=0.2",
            r"xlabel={Triple additions/deletions per rev.}",
            fr"xtick={{0,...,{len(self._bin_intervals) - 1}}}",
            self._xticklabels_for_bins(self._bin_intervals, vphantom=r"1y$\geq$"),
            r"xtick pos=bottom",
            r"enlarge y limits={upper, value=0.2}",
            r"ylabel={Relative frequency}",
            r"ylabel near ticks",
            r"yticklabel pos=right",
            *self._YTICK_PERCENT_OPTIONS,
            *self._LABELS_AUTO_PERCENT_OPTIONS,
            r"legend entries={Additions, Deletions}",
            r"legend pos=north east",
            r"legend cell align={left}",
        )

    def _generate_axis_content(self, axis: Axis, axis_name: str) -> None:
        bars_additions = Plot(
            options=TikZOptions(*self._BARS_ADDITIONS_OPTIONS),
            coordinates=[
                (i, round(b, 2)) for i, b in enumerate(self._bin_data_additions)
            ],
        )
        axis.append(bars_additions)

        bars_deletions = Plot(
            options=TikZOptions(*self._BARS_DELETIONS_OPTIONS),
            coordinates=[
                (i, round(b, 2)) for i, b in enumerate(self._bin_data_deletions)
            ],
        )
        axis.append(bars_deletions)


class FigureTimeUntilTripleFirstAdditionAndDeletion(FigureBase):
    def __init__(
        self,
        data_dir: Path,
        bin_intervals: Sequence[range] = (
            range(0, 2),
            range(2, 31),
            range(31, 181),
            range(181, 366),
            range(366, maxsize),
        ),
    ) -> None:
        self._last_page_id = 0
        self._cur_entity_created_timestamp = datetime.min
        self._triple_addition_timestamp: MutableMapping[
            WikidataRdfTriple, datetime
        ] = {}
        self._triple_deletion_timestamp: MutableMapping[
            WikidataRdfTriple, datetime
        ] = {}
        self._days_until_triple_addition_counter = Counter[int]()
        self._days_until_triple_deletion_counter = Counter[int]()
        self._bin_intervals = bin_intervals
        self._bin_data_additions = [0.0 for _ in range(len(self._bin_intervals))]
        self._bin_data_deletions = [0.0 for _ in range(len(self._bin_intervals))]
        super().__init__(data_dir=data_dir, tikz_library="patterns")

    def handle_revision(self, revision: WikidatedRevision) -> None:
        if self._last_page_id != revision.page_id:
            self._last_page_id = revision.page_id
            self._cur_entity_created_timestamp = revision.timestamp
            self._triple_addition_timestamp = {}
            self._triple_deletion_timestamp = {}

        for triple in revision.triple_additions:
            if triple in self._triple_addition_timestamp:
                continue
            self._triple_addition_timestamp[triple] = revision.timestamp
            self._days_until_triple_addition_counter[
                ceil((revision.timestamp - self._cur_entity_created_timestamp) / _DAY)
            ] += 1

        for triple in revision.triple_deletions:
            if triple in self._triple_deletion_timestamp:
                continue
            self._triple_deletion_timestamp[triple] = revision.timestamp
            self._days_until_triple_deletion_counter[
                ceil(
                    (revision.timestamp - self._triple_addition_timestamp[triple])
                    / _DAY
                )
            ] += 1

    def log_statistics(self) -> None:
        super().log_statistics()

        for (
            days_until_triple_addition,
            count,
        ) in self._days_until_triple_addition_counter.items():
            for i, bin_interval in enumerate(self._bin_intervals):
                if days_until_triple_addition in bin_interval:
                    self._bin_data_additions[i] += count
                    break

        for (
            days_until_triple_deletion,
            count,
        ) in self._days_until_triple_deletion_counter.items():
            for i, bin_interval in enumerate(self._bin_intervals):
                if days_until_triple_deletion in bin_interval:
                    self._bin_data_deletions[i] += count
                    break

        num_triple_additions = sum(self._days_until_triple_addition_counter.values())
        self._bin_data_additions = [
            b / num_triple_additions for b in self._bin_data_additions
        ]

        num_triple_deletions = sum(self._days_until_triple_deletion_counter.values())
        if num_triple_deletions != 0:
            self._bin_data_deletions = [
                b / num_triple_deletions for b in self._bin_data_deletions
            ]

        self._log_statistics_histogram(
            "days_until_triple_addition_counter",
            self._days_until_triple_addition_counter,
        )
        self._log_statistics_histogram(
            "days_until_triple_deletion_counter",
            self._days_until_triple_deletion_counter,
        )
        _LOGGER.info(f"  num_triple_additions = {num_triple_additions}")
        _LOGGER.info(f"  num_triple_deletions = {num_triple_deletions}")
        _LOGGER.info(f"  bin_intervals = {self._bin_intervals}")
        _LOGGER.info(f"  bin_data_additions = {self._bin_data_additions}")
        _LOGGER.info(f"  bin_data_deletions = {self._bin_data_deletions}")

    def _generate_axis_options(self) -> Sequence[str]:
        return (
            *self._axis_size(0.55),
            r"ybar",
            r"bar width=0.5cm",
            r"enlarge x limits=0.15",
            r"xlabel={Time until triple is first added/deleted in days}",
            fr"xtick={{0,...,{len(self._bin_intervals) - 1}}}",
            self._xticklabels_for_bins(
                self._bin_intervals, alt_style=True, vphantom=r"[($\geq$"
            ),
            r"xtick pos=bottom",
            r"enlarge y limits={upper, value=0.2}",
            r"ylabel={Relative frequency}",
            r"max space between ticks=15",
            *self._YTICK_PERCENT_OPTIONS,
            *self._LABELS_AUTO_PERCENT_OPTIONS,
            r"legend entries={First addition, First deletion}",
            r"legend style={at={(0.5,0.97)}, anchor=north}",
            r"legend cell align={left}",
        )

    def _generate_axis_content(self, axis: Axis, axis_name: str) -> None:
        bars_additions = Plot(
            options=TikZOptions(*self._BARS_ADDITIONS_OPTIONS),
            coordinates=[
                (i, round(b, 2)) for i, b in enumerate(self._bin_data_additions)
            ],
        )
        axis.append(bars_additions)

        bars_deletions = Plot(
            options=TikZOptions(*self._BARS_DELETIONS_OPTIONS),
            coordinates=[
                (i, round(b, 2)) for i, b in enumerate(self._bin_data_deletions)
            ],
        )
        axis.append(bars_deletions)


class FigureNumDeletionsPerTriple(FigureBase):
    def __init__(
        self,
        data_dir: Path,
        bin_intervals: Sequence[range] = (
            range(0, 1),
            range(1, 2),
            range(2, 10),
            range(10, maxsize),
        ),
    ) -> None:
        self._last_page_id = 0
        self._triple_additions: MutableSet[WikidataRdfTriple] = set()
        self._triple_deletions_counter = Counter[WikidataRdfTriple]()
        self._num_triple_deletions_counter = Counter[int]()
        self._bin_intervals = bin_intervals
        self._bin_data = [0 for _ in range(len(self._bin_intervals))]
        super().__init__(data_dir=data_dir, tikz_library="patterns")

    def handle_revision(self, revision: WikidatedRevision) -> None:
        if self._last_page_id != revision.page_id:
            self._last_page_id = revision.page_id
            self._num_triple_deletions_counter[0] += len(self._triple_additions)
            for num_triple_deletions in self._triple_deletions_counter.values():
                self._num_triple_deletions_counter[num_triple_deletions] += 1
            self._triple_additions = set()
            self._triple_deletions_counter = Counter[WikidataRdfTriple]()

        for triple in revision.triple_additions:
            self._triple_additions.add(triple)
        for triple in revision.triple_deletions:
            self._triple_deletions_counter[triple] += 1

    def log_statistics(self) -> None:
        super().log_statistics()

        self._num_triple_deletions_counter[0] += len(self._triple_additions)
        for num_triple_deletions in self._triple_deletions_counter.values():
            self._num_triple_deletions_counter[num_triple_deletions] += 1
        self._triple_additions = set()
        self._triple_deletions_counter = Counter[WikidataRdfTriple]()

        for num_triple_deletions, count in self._num_triple_deletions_counter.items():
            for i, bin_interval in enumerate(self._bin_intervals):
                if num_triple_deletions in bin_interval:
                    self._bin_data[i] += count
                    break

        self._log_statistics_histogram(
            "num_triple_deletions_counter", self._num_triple_deletions_counter
        )
        _LOGGER.info(f"  bin_intervals = {self._bin_intervals}")
        _LOGGER.info(f"  bin_data = {self._bin_data}")

    def _generate_axis_options(self) -> Sequence[str]:
        return (
            r"ybar",
            *self._axis_size(0.4),
            r"bar width=0.9cm",
            r"enlarge x limits=0.2",
            r"xlabel={Number of deletions of same triple\vphantom{[}}",
            fr"xtick={{0,...,{len(self._bin_intervals) - 1}}}",
            self._xticklabels_for_bins(self._bin_intervals, vphantom=r"[($\geq$"),
            r"xtick pos=bottom",
            r"enlarge y limits={upper, value=0.2}",
            r"ylabel={Frequency}",
            r"ylabel near ticks",
            r"yticklabel pos=right",
            r"ytick style={draw=none}",
            r"ymin=1",
            r"ymajorgrids",
            r"max space between ticks=15",
        )

    def _generate_axis_content(self, axis: Axis, axis_name: str) -> None:
        axis._latex_name = "semilogyaxis"
        bars = Plot(
            options=TikZOptions(*self._BARS_DELETIONS_OPTIONS),
            coordinates=[(i, b) for i, b in enumerate(self._bin_data)],
        )
        axis.append(bars)

        labels = TikZScope(
            options=TikZOptions(r"anchor=south", r"font=\footnotesize"),
            data=[
                TikZNode(
                    at=TikZCoordinate(i, b), text=self._into_scientific_notation(b)
                )
                for i, b in enumerate(self._bin_data)
            ],
        )
        axis.append(labels)

    @classmethod
    def _into_scientific_notation(cls, number: float) -> str:
        significant, exponent = f"{number:.1e}".split("e")
        return fr"${significant}\!\cdot\!10^{{{int(exponent)}}}$"


def _main() -> None:
    data_dir = Path("data")

    figures: Sequence[FigureBase] = (
        FigureNumEntitiesAndRevisionsByYear(data_dir=data_dir),
        FigureNumRevisionsPerEntity(data_dir=data_dir),
        FigureTimedeltaBetweenRevisions(data_dir=data_dir),
        FigureNumTripleAdditionsAndDeletionsPerRevision(data_dir=data_dir),
        FigureTimeUntilTripleFirstAdditionAndDeletion(data_dir=data_dir),
        FigureNumDeletionsPerTriple(data_dir=data_dir),
    )

    wikidated_manager = WikidatedManager(data_dir)
    wikidated_manager.configure_logging(
        file_path=data_dir / "wikidated-1.0-analyze.log"
    )

    wikidated_dataset = wikidated_manager.v1_0(auto_download=False)
    wikidated_dataset.entity_streams.download()

    for revision in tqdm(
        wikidated_dataset.iter_revisions(min_page_id=0),
        total=WikidatedV1_0Dataset.NUM_REVISIONS,
        dynamic_ncols=True,
    ):
        for figure in figures:
            figure.handle_revision(revision)

    for figure in figures:
        figure.log_statistics()
        figure.generate()


if __name__ == "__main__":
    try:
        _main()
    except Exception:
        # Make exceptions show up in log.
        _LOGGER.exception("Exception occurred.")
        raise

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

import heapq
import re
from datetime import date, datetime, timedelta, timezone
from itertools import chain, takewhile
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from sys import maxsize
from typing import (
    Any,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from tqdm import tqdm  # type: ignore
from typing_extensions import Final

from wikidated._utils import (
    RangeMap,
    SevenZipArchive,
    days_between_dates,
    months_between_dates,
    next_month,
)
from wikidated.wikidata import WIKIDATA_EARLIEST_REVISION_TIMESTAMP
from wikidated.wikidated_revision import WikidatedRevision
from wikidated.wikidated_sorted_entity_streams import WikidatedSortedEntityStreams

_LOGGER = getLogger(__name__)


class WikidatedGlobalStreamFile:
    def __init__(self, archive_path: Path, month: date, revision_ids: range) -> None:
        self.archive_path: Final = archive_path
        self.month: Final = month
        self.revision_ids: Final = revision_ids

    @property
    def months(self) -> range:
        return range(self.month.toordinal(), next_month(self.month).toordinal())

    def iter_revisions(
        self,
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
        with archive.read() as fd:
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
                    break
                yield revision

    @classmethod
    def archive_path_glob(cls, dataset_dir: Path, month: Optional[date] = None) -> str:
        month_str = f"{month:%4Y%2m}" if month else "*"
        return f"{dataset_dir.name}-global-stream-d{month_str}-r*-r*.7z"

    @classmethod
    def _make_archive_path(
        cls, dataset_dir: Path, month: date, revision_ids: range
    ) -> Path:
        return dataset_dir / (
            f"{dataset_dir.name}-global-stream"
            f"-d{month:%4Y%2m}-r{revision_ids.start}-r{revision_ids.stop - 1}.7z"
        )

    @classmethod
    def _parse_archive_path(cls, path: Path) -> Tuple[Path, date, range]:
        match = re.match(
            r"^(?P<dataset_dir_name>.+)-global-stream"
            r"-d(?P<year>\d{4})(?P<month>\d{2})"
            r"-r(?P<min_revision_id>\d+)-r(?P<max_revision_id>\d+).7z$",
            path.name,
        )
        assert match

        dataset_dir = path.parent.resolve()
        assert dataset_dir.name == match["dataset_dir_name"]
        month = date(year=int(match["year"]), month=int(match["month"]), day=1)
        revision_ids = range(
            int(match["min_revision_id"]), int(match["max_revision_id"]) + 1
        )
        return dataset_dir, month, revision_ids

    @classmethod
    def _make_archive_component_path(cls, day: date, revision_ids: range) -> Path:
        return Path(
            f"d{day:%4Y%2m%2d}-r{revision_ids.start}-r{revision_ids.stop - 1}.jsonl"
        )

    @classmethod
    def _parse_archive_component_path(cls, path: Path) -> Tuple[date, range]:
        match = re.match(
            r"d(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
            r"-r(?P<min_revision_id>\d+)-r(?P<max_revision_id>\d+).jsonl$",
            path.name,
        )
        assert match

        day = date(
            year=int(match["year"]), month=int(match["month"]), day=int(match["day"])
        )
        revision_ids = range(
            int(match["min_revision_id"]), int(match["max_revision_id"]) + 1
        )
        return day, revision_ids

    @classmethod
    def load_custom(cls, path: Path) -> WikidatedGlobalStreamFile:
        _, month, revision_ids = cls._parse_archive_path(path)
        return WikidatedGlobalStreamFile(path, month, revision_ids)

    @classmethod
    def build_custom(
        cls, dataset_dir: Path, month: date, revisions: Iterator[WikidatedRevision]
    ) -> Tuple[WikidatedGlobalStreamFile, Iterator[WikidatedRevision]]:
        if month.day != 1:
            raise ValueError("month must be a date with day=1.")

        existing_file = cls._check_existing_file(dataset_dir, month, revisions)
        if existing_file:
            return existing_file, revisions

        _LOGGER.debug(
            f"Building global stream file {cls.archive_path_glob(dataset_dir, month)}."
        )

        tmp_dir = dataset_dir / f"tmp.{dataset_dir.name}-global-stream-d{month:%4Y%2m}"
        if tmp_dir.exists():
            rmtree(tmp_dir)
        tmp_dir.mkdir(exist_ok=True, parents=True)

        revision_ids: Optional[range] = None

        for day in days_between_dates(month, next_month(month) - timedelta(days=1)):
            if day < WIKIDATA_EARLIEST_REVISION_TIMESTAMP.date():
                continue

            revision_ids_of_day, revisions = cls._write_revisions_of_day(
                dataset_dir, tmp_dir, day, revisions
            )

            if revision_ids_of_day:
                revision_ids = (
                    revision_ids_of_day
                    if revision_ids is None
                    else range(revision_ids.start, revision_ids_of_day.stop)
                )

        if revision_ids is None:
            # This rare case occurs when there are no revisions for the given month.
            _LOGGER.warning(
                "Did not find any revisions for global stream file "
                f"{cls.archive_path_glob(dataset_dir, month)}."
            )
            revision_ids = range(0, 0)

        archive_path = cls._make_archive_path(dataset_dir, month, revision_ids)

        SevenZipArchive.from_dir(tmp_dir, archive_path)
        rmtree(tmp_dir)

        _LOGGER.debug(f"Done building global stream file {archive_path.name}.")

        return WikidatedGlobalStreamFile(archive_path, month, revision_ids), revisions

    @classmethod
    def _write_revisions_of_day(
        cls,
        dataset_dir: Path,
        tmp_dir: Path,
        day: date,
        revisions: Iterator[WikidatedRevision],
    ) -> Tuple[Optional[range], Iterator[WikidatedRevision]]:
        tmp_file = tmp_dir / f"tmp.{day:%4Y%2m%2d}.jsonl"
        revision_ids_of_day: Optional[range] = None
        with tmp_file.open("w", encoding="UTF-8") as fd:
            for revision in revisions:
                revision_date = revision.timestamp.date()
                if revision_date < day:
                    # The revisions stream is sorted by ascending revision IDs and
                    # revision IDs are increasing over time. However, in rare cases
                    # a revision's timestamp can lie a few seconds before that of a
                    # revision with a lower ID. If that happens on date boundaries
                    # we run into this rare case.
                    _LOGGER.warning(
                        f"Revision {revision.revision_id} authored on "
                        f"{revision.timestamp} has revision ID higher than "
                        f"revisions authored on {day}. Including it with revisions "
                        f"of that day."
                    )
                elif revision_date > day:
                    revisions = chain((revision,), revisions)
                    break

                revision_ids_of_day = range(
                    revision.revision_id
                    if revision_ids_of_day is None
                    else revision_ids_of_day.start,
                    revision.revision_id + 1,
                )
                fd.write(revision.json() + "\n")

        if revision_ids_of_day is None:
            # No revisions for this day existed.
            _LOGGER.warning(
                f"Did not find any revisions for day {day:%Y%m%d} in global stream "
                f"file {cls.archive_path_glob(dataset_dir, day.replace(day=1))}."
            )
            tmp_file.unlink()
        else:
            tmp_file.rename(
                tmp_dir / cls._make_archive_component_path(day, revision_ids_of_day)
            )

        return revision_ids_of_day, revisions

    @classmethod
    def _check_existing_file(
        cls, dataset_dir: Path, month: date, revisions: Iterator[WikidatedRevision]
    ) -> Optional[WikidatedGlobalStreamFile]:
        try:
            archive_path = next(
                dataset_dir.glob(cls.archive_path_glob(dataset_dir, month))
            )
        except StopIteration:
            return None

        _, month_, revision_ids = cls._parse_archive_path(archive_path)
        assert month == month_

        _LOGGER.debug(
            f"Global stream file '{archive_path}' already exists, skipping building "
            f"but draining revisions iterator."
        )
        next_month_ = next_month(month)
        for _ in takewhile(
            lambda revision: revision.timestamp.date() < next_month_, revisions
        ):
            pass
        return WikidatedGlobalStreamFile(archive_path, month, revision_ids)


_T_WikidatedGlobalStreamFile_co = TypeVar(
    "_T_WikidatedGlobalStreamFile_co",
    bound=WikidatedGlobalStreamFile,
    covariant=True,
)


class WikidatedGenericGlobalStream(Generic[_T_WikidatedGlobalStreamFile_co]):
    def __init__(
        self,
        files_by_months: RangeMap[_T_WikidatedGlobalStreamFile_co],
        files_by_revision_ids: RangeMap[_T_WikidatedGlobalStreamFile_co],
    ) -> None:
        self._files_by_months = files_by_months
        self._files_by_revision_ids = files_by_revision_ids

    def __len__(self) -> int:
        return len(self._files_by_months)

    def __iter__(self) -> Iterator[_T_WikidatedGlobalStreamFile_co]:
        return iter(self._files_by_months.values())

    @overload
    def __getitem__(self, key: date) -> _T_WikidatedGlobalStreamFile_co:
        ...

    @overload
    def __getitem__(self, key: int) -> _T_WikidatedGlobalStreamFile_co:
        ...

    @overload
    def __getitem__(self, key: slice) -> Iterable[_T_WikidatedGlobalStreamFile_co]:
        ...

    @overload
    def __getitem__(self, key: object) -> Any:  # NoReturn doesn't work here.
        ...

    def __getitem__(
        self, key: object
    ) -> Union[WikidatedGlobalStreamFile, Iterable[_T_WikidatedGlobalStreamFile_co]]:
        if isinstance(key, date):
            return self._files_by_months[key.toordinal()]
        elif isinstance(key, int):
            return self._files_by_revision_ids[key]
        elif isinstance(key, slice):
            if key == slice(None):
                return self._files_by_revision_ids[:]
            elif isinstance(key.start, date) or isinstance(key.stop, date):
                return self._files_by_months[
                    slice(
                        key.start and key.start.toordinal(),
                        key.stop and key.stop.toordinal(),
                        key.step,
                    )
                ]
            elif isinstance(key.start, int) or isinstance(key.stop, int):
                return self._files_by_revision_ids[key]
            else:
                raise TypeError("if key is a slice it must be over dates or ints.")
        else:
            raise TypeError("key needs to be of type date or int.")

    @classmethod
    def load_custom(cls, dataset_dir: Path) -> WikidatedGlobalStream:
        _LOGGER.debug(f"Loading global stream for dataset {dataset_dir.name}.")
        files_by_months = RangeMap[WikidatedGlobalStreamFile]()
        files_by_revision_ids = RangeMap[WikidatedGlobalStreamFile]()
        for path in dataset_dir.glob(
            WikidatedGlobalStreamFile.archive_path_glob(dataset_dir)
        ):
            file = WikidatedGlobalStreamFile.load_custom(path)
            files_by_months[file.months] = file
            files_by_revision_ids[file.revision_ids] = file
        _LOGGER.debug(f"Done loading global stream for dataset {dataset_dir.name}.")
        return WikidatedGlobalStream(files_by_months, files_by_revision_ids)

    @classmethod
    def build_custom(
        cls,
        dataset_dir: Path,
        sorted_entity_streams: WikidatedSortedEntityStreams,
        wikidata_dump_version: date,
    ) -> WikidatedGlobalStream:
        _LOGGER.debug(f"Building global stream for dataset {dataset_dir.name}.")

        sorted_entity_streams_iters = [
            sorted_entity_streams_file.iter_revisions()
            for sorted_entity_streams_file in sorted_entity_streams
        ]
        sorted_revisions = iter(
            heapq.merge(*sorted_entity_streams_iters, key=lambda rev: rev.revision_id)
        )

        files_by_months = RangeMap[WikidatedGlobalStreamFile]()
        files_by_revision_ids = RangeMap[WikidatedGlobalStreamFile]()
        for month in tqdm(
            months_between_dates(
                WIKIDATA_EARLIEST_REVISION_TIMESTAMP.date().replace(day=1),
                wikidata_dump_version,
            ),
            desc="Global Stream",
            dynamic_ncols=True,
        ):
            file, sorted_revisions = WikidatedGlobalStreamFile.build_custom(
                dataset_dir, month, sorted_revisions
            )
            files_by_months[file.months] = file
            files_by_revision_ids[file.revision_ids] = file
        try:
            revision = next(sorted_revisions)
            raise Exception(
                f"Found revisions after Wikidata dump version date: {revision}"
            )
        except StopIteration:
            pass

        _LOGGER.debug(f"Done building global stream for dataset {dataset_dir.name}.")
        return WikidatedGlobalStream(files_by_months, files_by_revision_ids)


WikidatedGlobalStream = WikidatedGenericGlobalStream[WikidatedGlobalStreamFile]

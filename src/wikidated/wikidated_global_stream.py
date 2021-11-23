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

from __future__ import annotations

import heapq
import re
from datetime import date
from itertools import chain, groupby, takewhile
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from typing import Iterator, Optional, Tuple

from tqdm import tqdm  # type: ignore
from typing_extensions import Final

from wikidated._utils import RangeMap, SevenZipArchive, month_between_dates, next_month
from wikidated._wikidated_sorted_entity_streams import (
    WikidatedSortedEntityStreamsManager,
)
from wikidated.wikidated_revision import WikidatedRevision

_LOGGER = getLogger(__name__)
_WIKIDATA_INCEPTION_MONTH = date(year=2012, month=10, day=1)


class WikidatedGlobalStreamFile:
    def __init__(self, archive_path: Path, month: date, revision_ids: range) -> None:
        self.archive_path: Final = archive_path
        self.month: Final = month
        self.revision_ids: Final = revision_ids

    @property
    def months(self) -> range:
        return range(self.month.toordinal(), next_month(self.month).toordinal())

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
            r"-r(?P<min_revision_id>\d+)-r(?P<max_revision_id>\d+).7z$",
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
    def load(cls, path: Path) -> WikidatedGlobalStreamFile:
        assert path.exists()
        _, month, revision_ids = cls._parse_archive_path(path)
        return WikidatedGlobalStreamFile(path, month, revision_ids)

    @classmethod
    def build(
        cls, dataset_dir: Path, month: date, revisions: Iterator[WikidatedRevision]
    ) -> Tuple[Optional[WikidatedGlobalStreamFile], Iterator[WikidatedRevision]]:
        if month.day != 1:
            raise ValueError("month must be a date with day=1.")

        next_month_ = next_month(month)
        try:
            archive_path = next(
                dataset_dir.glob(cls.archive_path_glob(dataset_dir, month))
            )
            _LOGGER.debug(
                f"File '{archive_path}' already exists, skipping building but "
                "draining revisions iterator."
            )
            for _ in takewhile(
                lambda revision: revision.timestamp.date() < next_month_, revisions
            ):
                pass
            return None, revisions
        except StopIteration:
            pass

        tmp_dir = dataset_dir / f"tmp.{dataset_dir.name}-global-stream-d{month:%4Y%2m}"
        if tmp_dir.exists():
            rmtree(tmp_dir)
        tmp_dir.mkdir(exist_ok=True, parents=True)

        revision_ids: Optional[range] = None

        for day, revisions_of_day in groupby(
            revisions, key=lambda revision: revision.timestamp.date()
        ):
            if day < month:
                raise Exception(
                    "Given revision stream has revisions before given month."
                )
            elif day >= next_month_:
                revisions = chain(revisions_of_day, revisions)
                break

            tmp_file = tmp_dir / f"tmp.{day:%4Y%2m%2d}.jsonl"
            revision_ids_of_day: Optional[range] = None
            with tmp_file.open("w", encoding="UTF-8") as fd:
                for revision in revisions_of_day:
                    revision_ids_of_day = range(
                        revision.revision_id
                        if revision_ids_of_day is None
                        else revision_ids_of_day.start,
                        revision.revision_id + 1,
                    )
                    fd.write(revision.json() + "\n")

            # The assertion could only be false if we did not enter the inner for loop
            # above. However, since groupby returned something for the outer loop,
            # revisions_of_day is not empty, and we must have entered the inner loop.
            assert revision_ids_of_day is not None
            revision_ids = (
                revision_ids_of_day
                if revision_ids is None
                else range(revision_ids.start, revision_ids_of_day.stop)
            )

            tmp_file.rename(
                tmp_dir / cls._make_archive_component_path(day, revision_ids_of_day)
            )

        if revision_ids is None:
            # This rare case occurs, when there are no revisions for the given month.
            rmtree(tmp_dir)
            return None, revisions

        archive_path = cls._make_archive_path(dataset_dir, month, revision_ids)

        SevenZipArchive.from_dir(tmp_dir, archive_path)
        rmtree(tmp_dir)

        return WikidatedGlobalStreamFile(archive_path, month, revision_ids), revisions


class WikidatedGlobalStreamManager:
    def __init__(self, dataset_dir: Path) -> None:
        self._dataset_dir = dataset_dir
        self._files_by_months = RangeMap[WikidatedGlobalStreamFile]()
        self._files_by_revision_ids = RangeMap[WikidatedGlobalStreamFile]()

    def load(self) -> None:
        for path in self._dataset_dir.glob(
            WikidatedGlobalStreamFile.archive_path_glob(self._dataset_dir)
        ):
            file = WikidatedGlobalStreamFile.load(path)
            self._files_by_months[file.months] = file
            self._files_by_revision_ids[file.revision_ids] = file

    def build(
        self,
        sorted_entity_streams_manager: WikidatedSortedEntityStreamsManager,
        wikidata_dump_version: date,
    ) -> None:
        sorted_entity_streams_iters = [
            sorted_entity_streams.iter_revisions()
            for sorted_entity_streams in (
                sorted_entity_streams_manager._files_by_page_ids.values()
            )
        ]
        sorted_revisions = iter(
            heapq.merge(
                *sorted_entity_streams_iters, key=lambda revision: revision.revision_id
            )
        )

        for month in tqdm(
            month_between_dates(_WIKIDATA_INCEPTION_MONTH, wikidata_dump_version),
            desc="Global Stream",
        ):
            file, sorted_revisions = WikidatedGlobalStreamFile.build(
                self._dataset_dir, month, sorted_revisions
            )
            if file:
                self._files_by_months[file.months] = file
                self._files_by_revision_ids[file.revision_ids] = file
        try:
            revision = next(sorted_revisions)
            raise Exception(
                f"Found revisions after Wikidata dump version date: {revision}"
            )
        except StopIteration:
            pass

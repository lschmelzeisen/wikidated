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

from calendar import monthrange
from datetime import timedelta
from itertools import groupby, takewhile
from logging import getLogger
from pathlib import Path
from typing import Tuple

from tqdm import tqdm  # type: ignore

from wikidated._utils import SevenZipArchive, months_between_dates
from wikidated.wikidata import WIKIDATA_EARLIEST_REVISION_TIMESTAMP
from wikidated.wikidated_dataset import WikidatedDataset
from wikidated.wikidated_global_stream import WikidatedGlobalStreamFile
from wikidated.wikidated_manager import WikidatedManager
from wikidated.wikidated_v1_0 import WikidatedV1_0Dataset

_LOGGER = getLogger(__name__)

_PERIOD_AFTER_DUMP_VERSION = timedelta(days=1)


def _assert_global_stream_file_structure(wikidated_dataset: WikidatedDataset) -> None:
    assert wikidated_dataset.dump_version
    expected_months = list(
        months_between_dates(
            WIKIDATA_EARLIEST_REVISION_TIMESTAMP.date().replace(day=1),
            wikidated_dataset.dump_version,
        )
    )
    max_revision_id = 0
    for global_stream_file in tqdm(
        wikidated_dataset.global_stream,
        desc="Validating global stream files",
        dynamic_ncols=True,
    ):
        month = global_stream_file.month
        _, num_days_in_month = monthrange(month.year, month.month)

        assert month in expected_months
        expected_months.remove(month)

        revision_ids_of_month = global_stream_file.revision_ids
        assert len(revision_ids_of_month) > 0
        assert max_revision_id < revision_ids_of_month.start

        assert (
            wikidated_dataset.global_stream[month]
            == wikidated_dataset.global_stream[revision_ids_of_month[0]]
            == wikidated_dataset.global_stream[revision_ids_of_month[-1]]
        )

        components = list(
            SevenZipArchive(global_stream_file.archive_path).iter_file_names()
        )
        for i in range(1, num_days_in_month + 1):
            day = month.replace(day=i)
            if not (
                WIKIDATA_EARLIEST_REVISION_TIMESTAMP.date()
                <= day
                <= wikidated_dataset.dump_version + _PERIOD_AFTER_DUMP_VERSION
            ):
                continue

            components_of_day = list(
                takewhile(
                    lambda file: file.name.startswith(f"d{day:%Y%m%d}"), components
                )
            )
            components = components[len(components_of_day) :]
            if day > wikidated_dataset and not components_of_day:
                # In _PERIOD_AFTER_DUMP_VERSION.
                continue
            assert len(components_of_day) == 1

            component = components_of_day[0]
            (
                _,
                revision_ids_of_day,
            ) = WikidatedGlobalStreamFile._parse_archive_component_path(Path(component))
            assert revision_ids_of_day[0] in revision_ids_of_month
            assert revision_ids_of_day[-1] in revision_ids_of_month
            assert max_revision_id < revision_ids_of_day.start
            max_revision_id = revision_ids_of_day[-1]

        assert not components
        assert max_revision_id == revision_ids_of_month[-1]

    assert not expected_months


def _assert_entity_streams_file_structure(wikidated_dataset: WikidatedDataset) -> None:
    max_page_id = 0
    for entity_streams_file in tqdm(
        wikidated_dataset.entity_streams,
        desc="Validating entity streams files",
        dynamic_ncols=True,
    ):
        page_ids = entity_streams_file.page_ids
        assert len(page_ids) > 0
        assert max_page_id < page_ids[0]

        assert (
            wikidated_dataset.entity_streams[page_ids[0]]
            == wikidated_dataset.entity_streams[page_ids[-1]]
        )
        assert (
            wikidated_dataset.sorted_entity_streams[page_ids[0]]
            == wikidated_dataset.sorted_entity_streams[page_ids[-1]]
        )

        for page_id in entity_streams_file.iter_page_ids():
            assert page_id in page_ids
            assert max_page_id < page_id
            max_page_id = page_id

        assert max_page_id <= page_ids[-1]


def _assert_global_stream_revision_iteration(
    wikidated_dataset: WikidatedDataset,
) -> Tuple[int, int]:
    page_ids = set()
    num_revisions = 0
    cur_revision_id = 0
    for revision in tqdm(
        wikidated_dataset.iter_revisions(),
        desc="Iterating global stream",
        total=WikidatedV1_0Dataset.NUM_REVISIONS,
        dynamic_ncols=True,
    ):
        page_ids.add(revision.page_id)
        num_revisions += 1

        assert cur_revision_id < revision.revision_id
        cur_revision_id = revision.revision_id

    return len(page_ids), num_revisions


def _assert_entity_streams_page_iteration(
    wikidated_dataset: WikidatedDataset,
) -> Tuple[int, int]:
    num_pages = 0
    num_revisions = 0
    cur_page_id = 0
    for page_id, revisions in tqdm(
        groupby(
            wikidated_dataset.iter_revisions(min_page_id=0),
            key=lambda revision: revision.page_id,
        ),
        total=WikidatedV1_0Dataset.NUM_PAGES,
        desc="Iterating entity streams (page iteration)",
        dynamic_ncols=True,
    ):
        num_pages += 1
        assert cur_page_id < page_id
        cur_page_id = page_id

        cur_revision_id = 0
        for revision in revisions:
            num_revisions += 1
            assert cur_revision_id < revision.revision_id
            cur_revision_id = revision.revision_id

    return num_pages, num_revisions


def _assert_entity_streams_page_lookup(
    wikidated_dataset: WikidatedDataset,
) -> Tuple[int, int]:
    num_pages = 0
    num_revisions = 0
    cur_page_id = 0
    for page_id in tqdm(
        wikidated_dataset.iter_page_ids(),
        total=WikidatedV1_0Dataset.NUM_PAGES,
        desc="Iterating entity streams (page lookup)",
        dynamic_ncols=True,
    ):
        num_pages += 1
        assert cur_page_id < page_id
        cur_page_id = page_id

        cur_revision_id = 0
        for revision in wikidated_dataset.iter_revisions(page_id):
            num_revisions += 1
            assert cur_revision_id < revision.revision_id
            cur_revision_id = revision.revision_id

    return num_pages, num_revisions


def _main() -> None:
    data_dir = Path("data")

    wikidated_manager = WikidatedManager(data_dir)
    wikidated_manager.configure_logging()

    wikidated_dataset = wikidated_manager.v1_0(auto_download=False)
    wikidated_dataset.download()

    _assert_global_stream_file_structure(wikidated_dataset)
    _assert_entity_streams_file_structure(wikidated_dataset)

    num_pages1, num_revisions1 = _assert_global_stream_revision_iteration(
        wikidated_dataset
    )
    _LOGGER.info(f"Num pages in global stream: {num_pages1}")
    _LOGGER.info(f"Num revisions in global stream: {num_revisions1}")

    num_pages2, num_revisions2 = _assert_entity_streams_page_iteration(
        wikidated_dataset
    )
    _LOGGER.info(f"Num pages in entity streams (page iteration): {num_pages2}")
    _LOGGER.info(f"Num revisions in entity streams (page iteration): {num_revisions2}")

    num_pages3, num_revisions3 = _assert_entity_streams_page_lookup(wikidated_dataset)
    _LOGGER.info(f"Num pages in entity streams (page lookup): {num_pages3}")
    _LOGGER.info(f"Num revisions in entity streams (page lookup): {num_revisions3}")


if __name__ == "__main__":
    try:
        _main()
    except Exception:
        # Make exceptions show up in log.
        _LOGGER.exception("Exception occurred.")
        raise

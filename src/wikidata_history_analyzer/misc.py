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
from typing import AbstractSet, MutableMapping, Sequence

import requests
from nasty_utils import ColoredBraceStyleAdapter
from tqdm import tqdm

from wikidata_history_analyzer.dumpfiles.wikidata_dump_manager import (
    WikidataDumpManager,
)
from wikidata_history_analyzer.dumpfiles.wikidata_meta_history_dump import (
    WikidataMetaHistoryDump,
)

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


def page_ids_from_prefixed_titles(
    titles: Sequence[str], dump_manager: WikidataDumpManager
) -> Sequence[int]:
    _LOGGER.info("Looking up page IDs of given titles...")

    title_to_page_id: MutableMapping[str, int] = {}
    titles_set = set(titles)

    if not titles_set:
        return []

    elif len(titles_set) < 10_000:
        for title in tqdm(titles_set):
            response = requests.get(
                "https://www.wikidata.org/w/api.php?action=query&format=json"
                "&titles=" + title
            ).json()
            page_id = int(next(iter(response["query"]["pages"].keys())))
            title_to_page_id[title] = page_id

    else:
        # Iterating through the page table should be faster for large number of titles.
        page_table = dump_manager.page_table()
        page_table.download()

        namespaces = dump_manager.namespaces()
        namespaces.download()

        for page_meta in page_table.iter_page_metas(namespaces.load_namespace_titles()):
            if page_meta.prefixed_title in titles_set:
                title_to_page_id[page_meta.prefixed_title] = page_meta.page_id

    return [title_to_page_id[title] for title in titles]


def meta_history_dumps_for_dump_names(
    dump_names: AbstractSet[str], dump_manager: WikidataDumpManager
) -> Sequence[WikidataMetaHistoryDump]:
    result = [
        dump
        for dump in dump_manager.meta_history_dumps()
        if dump.path.name in dump_names
    ]
    extra_dump_names = dump_names - set(dump.path.name for dump in result)
    if extra_dump_names:
        raise Exception(
            "Meta history dumps with following names could not be found: "
            + ", ".join(sorted(extra_dump_names))
        )

    return result


def meta_history_dumps_for_page_ids(
    page_ids: AbstractSet[int], dump_manager: WikidataDumpManager
) -> Sequence[WikidataMetaHistoryDump]:
    _LOGGER.info("Looking up meta history dumps for given page IDs...")
    if not page_ids:
        _LOGGER.warning("No given page IDs, will return all meta history dumps!")
        return dump_manager.meta_history_dumps()

    meta_history_dumps = []
    for dump in dump_manager.meta_history_dumps():
        for page_id in page_ids:
            if dump.min_page_id <= page_id <= dump.max_page_id:
                meta_history_dumps.append(dump)
                break
    return meta_history_dumps


def check_page_ids_in_meta_history_dumps(
    page_ids: AbstractSet[int], meta_history_dumps: Sequence[WikidataMetaHistoryDump]
) -> None:
    extra_page_ids = page_ids
    for dump in meta_history_dumps:
        extra_page_ids = {
            page_id
            for page_id in extra_page_ids
            if not dump.min_page_id <= page_id <= dump.max_page_id
        }

    if extra_page_ids:
        _LOGGER.warning(
            "Following page IDs are not included in any given meta history dump: {}",
            ", ".join(map(str, sorted(extra_page_ids))),
        )

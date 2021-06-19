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
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Mapping, Match

from wikidata_history_analyzer.wikidata_dump import WikidataDump


@dataclass
class WikidataPage:
    title: str
    prefixed_title: str
    namespace: int
    page_id: str
    restrictions: str  # TODO: Find out and document what this is.
    is_redirect: int  # TODO: Find out and document what this is.
    is_new: int  # TODO: Find out and document what this is.
    random: float  # TODO: Find out and document what this is.
    touched: datetime  # TODO: Find out and document what this is.
    links_updated: datetime  # TODO: Find out and document what this is.
    latest_revision_id: str
    len: int  # TODO: Find out and document what this is.
    content_model: str
    lang: str  # TODO: Find out and document what this is.

    def __init__(self, match: Match[str], namespace_titles: Mapping[int, str]) -> None:
        self.namespace = int(match["namespace"])
        self.title = match["title"]
        self.prefixed_title = (
            namespace_titles[self.namespace] + ":" + self.title
            if namespace_titles[self.namespace]
            else self.title
        )
        self.page_id = match["page_id"]
        self.restrictions = match["restrictions"]
        self.is_redirect = int(match["is_redirect"])
        self.is_new = int(match["is_new"])
        self.random = float(match["random"])
        self.touched = datetime.strptime(match["touched"], "%Y%m%d%H%M%S")
        self.links_updated = datetime.strptime(match["links_updated"], "%Y%m%d%H%M%S")
        self.latest_revision_id = match["latest_revision_id"]
        self.len = int(match["len"])
        self.content_model = match["content_model"]
        self.lang = match["lang"]


_PATTERN = re.compile(
    r"""
        \(
            (?P<page_id>\d+),
            (?P<namespace>\d+),
            '(?P<title>[^']+)',
            '(?P<restrictions>[^']*)',
            (?P<is_redirect>\d+),
            (?P<is_new>\d+),
            (?P<random>\d.\d+),
            '(?P<touched>\d+)',
            '(?P<links_updated>\d+)',
            (?P<latest_revision_id>\d+),
            (?P<len>\d+),
            '(?P<content_model>[^']*)',
            '?(?P<lang>[^')]*)'?
        \)
    """,
    re.VERBOSE,
)


class WikidataPageTable(WikidataDump):
    def iter_pages(self, namespace_titles: Mapping[int, str]) -> Iterator[WikidataPage]:
        assert self.path.exists()

        insert_line_start = "INSERT INTO `page` VALUES "
        insert_line_end = ";\n"

        with gzip.open(self.path, "rt", encoding="UTF-8") as fin:
            for line in fin:
                if not (
                    line.startswith(insert_line_start)
                    and line.endswith(insert_line_end)
                ):
                    continue

                line = line[len(insert_line_start) : -len(insert_line_end)]
                for match in _PATTERN.finditer(line):
                    yield WikidataPage(match, namespace_titles)

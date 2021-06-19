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
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Match


@dataclass
class WikidataPageMeta:
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

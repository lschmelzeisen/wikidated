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

from typing import Optional

from pydantic import BaseModel as PydanticModel


class WikidataPageMeta(PydanticModel):
    title: str
    prefixed_title: str
    namespace: int
    page_id: int
    restrictions: str  # TODO: Find out and document what this is.
    is_redirect: int  # TODO: Find out and document what this is.
    is_new: int  # TODO: Find out and document what this is.
    random: float  # TODO: Find out and document what this is.
    touched: str  # TODO: Find out and document what this is.
    links_updated: Optional[str]  # TODO: Find out and document what this is.
    latest_revision_id: int
    len: int  # TODO: Find out and document what this is.
    content_model: str
    lang: Optional[str]  # TODO: Find out and document what this is.

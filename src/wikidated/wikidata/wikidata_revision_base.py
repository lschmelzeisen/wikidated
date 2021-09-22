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

from datetime import datetime
from typing import Optional

from pydantic import BaseModel as PydanticModel


class WikidataEntityMeta(PydanticModel):
    entity_id: str
    page_id: int
    namespace: int
    redirect: Optional[str]


class WikidataRevisionMeta(PydanticModel):
    revision_id: int
    parent_revision_id: Optional[int]
    timestamp: datetime
    contributor: Optional[str]
    contributor_id: Optional[int]
    is_minor: bool
    comment: Optional[str]
    wikibase_model: str
    wikibase_format: str
    sha1: Optional[str]


class WikidataRevisionBase(PydanticModel):
    entity: WikidataEntityMeta
    revision: WikidataRevisionMeta

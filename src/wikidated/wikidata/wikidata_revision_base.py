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

    @property
    def entity_id(self) -> str:
        return self.entity.entity_id

    @property
    def page_id(self) -> int:
        return self.entity.page_id

    @property
    def namespace(self) -> int:
        return self.entity.namespace

    @property
    def revision_id(self) -> int:
        return self.revision.revision_id

    @property
    def parent_revision_id(self) -> Optional[int]:
        return self.revision.parent_revision_id

    @property
    def timestamp(self) -> datetime:
        return self.revision.timestamp

    @property
    def contributor(self) -> Optional[str]:
        return self.revision.contributor

    @property
    def contributor_id(self) -> Optional[int]:
        return self.revision.contributor_id

    @property
    def is_minor(self) -> bool:
        return self.revision.is_minor

    @property
    def comment(self) -> Optional[str]:
        return self.revision.comment

    @property
    def wikibase_model(self) -> str:
        return self.revision.wikibase_model

    @property
    def wikibase_format(self) -> str:
        return self.revision.wikibase_format

    @property
    def sha1(self) -> Optional[str]:
        return self.revision.sha1

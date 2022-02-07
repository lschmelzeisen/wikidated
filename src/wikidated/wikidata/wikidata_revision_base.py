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

from datetime import datetime
from typing import Optional

from pydantic import BaseModel as PydanticModel


class WikidataEntityMetadata(PydanticModel):
    entity_id: str
    page_id: int
    namespace: int
    redirect: Optional[str]


class WikidataRevisionMetadata(PydanticModel):
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
    # EntityMetadata
    entity_id: str
    page_id: int
    namespace: int
    redirect: Optional[str]
    # RevisionMetadata
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

    def entity_metadata(self) -> WikidataEntityMetadata:
        return WikidataEntityMetadata(
            entity_id=self.entity_id,
            page_id=self.page_id,
            namespace=self.namespace,
            redirect=self.redirect,
        )

    def revision_metadata(self) -> WikidataRevisionMetadata:
        return WikidataRevisionMetadata(
            revision_id=self.revision_id,
            parent_revision_id=self.parent_revision_id,
            timestamp=self.timestamp,
            contributor=self.contributor,
            contributor_id=self.contributor_id,
            is_minor=self.is_minor,
            comment=self.comment,
            wikibase_model=self.wikibase_model,
            wikibase_format=self.wikibase_format,
            sha1=self.sha1,
        )

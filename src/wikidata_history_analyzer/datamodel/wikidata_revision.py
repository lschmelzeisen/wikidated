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

from datetime import datetime
from typing import Optional

from jpype import JClass, JException, JObject  # type: ignore
from pydantic import BaseModel as PydanticModel

from wikidata_history_analyzer.jvm_manager import JvmManager


class WikidataRevisionProcessingException(Exception):
    def __init__(
        self,
        reason: str,
        revision: WikidataRevision,
        exception: Optional[Exception] = None,
    ) -> None:
        self.reason = reason
        self.revision = revision
        self.exception = exception

    def __str__(self) -> str:
        return (
            f"{self.reason} ({self.revision.prefixed_title}, "
            f"page: {self.revision.page_id}, revision: {self.revision.revision_id})"
        )


class WikidataRevisionWdtkDeserializationException(WikidataRevisionProcessingException):
    pass


class WikidataRevision(PydanticModel):
    prefixed_title: str
    namespace: int
    page_id: str
    redirect: Optional[str]
    revision_id: str
    parent_revision_id: Optional[str]
    timestamp: datetime
    contributor: Optional[str]
    contributor_id: Optional[str]
    is_minor: bool
    comment: Optional[str]
    content_model: str
    format: str
    text: Optional[str]
    sha1: Optional[str]

    def load_wdtk_deserialization(self, jvm_manager: JvmManager) -> JObject:
        if self.text is None:
            raise WikidataRevisionWdtkDeserializationException(
                "Entity has no text.", self
            )

        _load_wdtk_classes_and_objects(jvm_manager)
        assert _WDTK_JSON_SERIALIZER is not None  # for mypy.

        # The following is based on WDTK's WikibaseRevisionProcessor.
        try:
            if '"redirect":' in self.text:
                return _WDTK_JSON_SERIALIZER.deserializeEntityRedirectDocument(
                    self.text
                )

            elif self.content_model == "wikibase-item":
                return _WDTK_JSON_SERIALIZER.deserializeItemDocument(self.text)

            elif self.content_model == "wikibase-property":
                return _WDTK_JSON_SERIALIZER.deserializePropertyDocument(self.text)

            elif self.content_model == "wikibase-lexeme":
                return _WDTK_JSON_SERIALIZER.deserializeLexemeDocument(self.text)

            elif self.content_model == "wikitext":
                return _WDTK_JSON_SERIALIZER.deserializeMediaInfoDocument(self.text)

            else:
                return _WDTK_JSON_SERIALIZER.deserializeEntityDocument(self.text)

        except JException as exception:
            raise WikidataRevisionWdtkDeserializationException(
                "JSON deserialization by Wikidata Toolkit failed.", self, exception
            )


def _load_wdtk_classes_and_objects(_jvm_manager: JvmManager) -> None:
    global _WDTK_JSON_SERIALIZER
    if _WDTK_JSON_SERIALIZER is None:
        _WDTK_JSON_SERIALIZER = JClass(
            "org.wikidata.wdtk.datamodel.helpers.JsonDeserializer"
        )(JClass("org.wikidata.wdtk.datamodel.helpers.Datamodel").SITE_WIKIDATA)


_WDTK_JSON_SERIALIZER: Optional[JObject] = None

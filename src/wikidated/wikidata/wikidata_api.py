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
from logging import getLogger
from typing import Iterable, Iterator, Mapping, Optional, Sequence, Union

import requests
from pydantic import BaseModel as PydanticModel

from wikidated._utils import chunked
from wikidated.wikidata.wikidata_revision_base import (
    WikidataEntityMetadata,
    WikidataRevisionBase,
)

_LOGGER = getLogger(__name__)


class _WikidataApiResultRevision(PydanticModel):
    revid: int
    parentid: int
    minor: Optional[str]
    user: Optional[str]
    userid: Optional[int]
    userhidden: Optional[str]
    timestamp: str
    sha1: str
    comment: str


class _WikidataApiResultMissingRevisionId(PydanticModel):
    missing: str
    revid: int


class _WikidataApiResultPage(PydanticModel):
    pageid: int
    ns: int
    title: str
    revisions: Optional[Sequence[_WikidataApiResultRevision]]


class _WikidataApiResultMissingPageId(PydanticModel):
    missing: str
    pageid: int


class _WikidataApiResultMissingEntityId(PydanticModel):
    missing: str
    ns: int
    title: str


class _WikidataApiResultPages(PydanticModel):
    pages: Optional[
        Mapping[
            str,
            Union[
                _WikidataApiResultPage,
                _WikidataApiResultMissingPageId,
                _WikidataApiResultMissingEntityId,
            ],
        ]
    ]
    badrevids: Optional[Mapping[int, _WikidataApiResultMissingRevisionId]]


class _WikidataApiResult(PydanticModel):
    batchcomplete: str
    query: _WikidataApiResultPages


class WikidataApi:
    _WIKIDATA_API_CHUNK_SIZE = 50

    # TODO: document that these are made against live Wikidata and how that could be
    #  different.

    # TODO: document how redirects can not be detected and how wikibase_model and
    #  wikibase_format are hardcoded.

    @classmethod
    def query_page_id(cls, page_id: int) -> Optional[WikidataEntityMetadata]:
        return next(cls.query_page_ids((page_id,)))

    @classmethod
    def query_page_ids(
        cls, page_ids: Iterable[int]
    ) -> Iterator[Optional[WikidataEntityMetadata]]:
        for page_ids_chunk in chunked(page_ids, cls._WIKIDATA_API_CHUNK_SIZE):
            response = requests.get(
                "https://www.wikidata.org/w/api.php?action=query&format=json"
                "&pageids=" + "|".join(str(p) for p in page_ids_chunk)
            )
            response.raise_for_status()

            result = _WikidataApiResult.parse_obj(response.json())
            if result.batchcomplete != "":
                _LOGGER.warning(
                    "Wikidata API returned unknown batchcomplete value: "
                    f"'{result.batchcomplete}'."
                )

            pages = result.query.pages or {}
            if len(pages) > len(page_ids_chunk):
                _LOGGER.warning("Wikidata API returned more pages than expected.")

            for page_id in page_ids_chunk:
                result_page_ = pages.get(str(page_id))
                if not result_page_ or not isinstance(
                    result_page_, _WikidataApiResultPage
                ):
                    _LOGGER.warning(
                        f"Wikidata API did not return result for page ID {page_id}."
                    )
                    yield None
                else:
                    yield cls._parse_result_page(result_page_)

    @classmethod
    def query_entity_id(cls, entity_id: str) -> Optional[WikidataEntityMetadata]:
        return next(cls.query_entity_ids((entity_id,)))

    @classmethod
    def query_entity_ids(
        cls, entity_ids: Iterable[str]
    ) -> Iterator[Optional[WikidataEntityMetadata]]:
        for entity_ids_chunk in chunked(entity_ids, cls._WIKIDATA_API_CHUNK_SIZE):
            response = requests.get(
                "https://www.wikidata.org/w/api.php?action=query&format=json"
                "&titles=" + "|".join(str(e) for e in entity_ids_chunk)
            )
            response.raise_for_status()

            result = _WikidataApiResult.parse_obj(response.json())
            if result.batchcomplete != "":
                _LOGGER.warning(
                    "Wikidata API returned unknown batchcomplete value: "
                    f"'{result.batchcomplete}'."
                )

            entity_ids_to_result_page = {}
            for result_page in (result.query.pages or {}).values():
                if not isinstance(result_page, _WikidataApiResultPage):
                    continue
                entity_ids_to_result_page[result_page.title] = result_page

            if len(entity_ids_to_result_page) > len(entity_ids_chunk):
                _LOGGER.warning("Wikidata API returned more pages than expected.")

            for entity_id in entity_ids_chunk:
                result_page_ = entity_ids_to_result_page.get(entity_id)
                if not result_page_:
                    _LOGGER.warning(
                        f"Wikidata API did not return result for entity ID {entity_id}."
                    )
                    yield None
                else:
                    yield cls._parse_result_page(result_page_)

    @classmethod
    def query_revision_id(cls, revision_id: int) -> Optional[WikidataRevisionBase]:
        return next(cls.query_revision_ids((revision_id,)))

    @classmethod
    def query_revision_ids(
        cls, revision_ids: Iterable[int]
    ) -> Iterator[Optional[WikidataRevisionBase]]:
        for revision_ids_chunk in chunked(revision_ids, cls._WIKIDATA_API_CHUNK_SIZE):
            response = requests.get(
                "https://www.wikidata.org/w/api.php?action=query&format=json"
                "&prop=revisions&rvprop=ids|flags|timestamp|user|userid|sha1|comment"
                "&revids=" + "|".join(str(r) for r in revision_ids_chunk)
            )
            response.raise_for_status()

            result = _WikidataApiResult.parse_obj(response.json())
            if result.batchcomplete != "":
                _LOGGER.warning(
                    "Wikidata API returned unknown batchcomplete value: "
                    f"'{result.batchcomplete}'."
                )

            revision_ids_to_indices = {}
            pages = list((result.query.pages or {}).values())
            for index_page, result_page in enumerate(pages):
                if not isinstance(result_page, _WikidataApiResultPage):
                    continue
                assert result_page.revisions is not None
                for index_revision, result_revision in enumerate(result_page.revisions):
                    assert result_revision.revid not in revision_ids_to_indices
                    revision_ids_to_indices[result_revision.revid] = (
                        index_page,
                        index_revision,
                    )

            if len(revision_ids_to_indices) > len(revision_ids_chunk):
                _LOGGER.warning("Wikidata API returned more revisions than expected.")

            for revision_id in revision_ids_chunk:
                indices = revision_ids_to_indices.get(revision_id)
                if not indices:
                    _LOGGER.warning(
                        "Wikidata API did not return result for revision ID "
                        f"{revision_id}."
                    )
                    yield None
                else:
                    index_page, index_revision = indices
                    result_page = pages[index_page]
                    assert isinstance(result_page, _WikidataApiResultPage)
                    assert result_page.revisions is not None
                    result_revision = result_page.revisions[index_revision]
                    yield cls._parse_result_page_and_revision(
                        result_page, result_revision
                    )

    @classmethod
    def _parse_result_page(
        cls, result_page: _WikidataApiResultPage
    ) -> WikidataEntityMetadata:
        return WikidataEntityMetadata(
            entity_id=result_page.title,
            page_id=result_page.pageid,
            namespace=result_page.ns,
            redirect=None,
        )

    @classmethod
    def _parse_result_page_and_revision(
        cls,
        result_page: _WikidataApiResultPage,
        result_revision: _WikidataApiResultRevision,
    ) -> WikidataRevisionBase:
        entity_metadata = cls._parse_result_page(result_page)
        return WikidataRevisionBase(
            entity_id=entity_metadata.entity_id,
            page_id=entity_metadata.page_id,
            namespace=entity_metadata.namespace,
            redirect=entity_metadata.redirect,
            revision_id=result_revision.revid,
            parent_revision_id=(
                result_revision.parentid if result_revision.parentid != 0 else None
            ),
            timestamp=datetime.strptime(
                result_revision.timestamp, "%Y-%m-%dT%H:%M:%S%z"
            ),
            contributor=result_revision.user,
            contributor_id=result_revision.userid,
            is_minor=result_revision.minor == "",
            comment=result_revision.comment if result_revision.comment != "" else None,
            wikibase_model="wikibase-item",
            wikibase_format="application/json",
            sha1=result_revision.sha1,
        )

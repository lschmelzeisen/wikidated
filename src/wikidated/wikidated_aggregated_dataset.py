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

from enum import Enum
from pathlib import Path
from typing import AbstractSet, Any, Dict, Iterator, Mapping, Optional, Sequence, Union

from wikidated.wikidata import WikidataDump, WikidataRevisionMeta
from wikidated.wikidated_dataset import (
    WikidatedDataset,
    WikidatedEntityStreams,
    WikidatedGlobalStream,
    WikidatedRevision,
)


class WikidatedAggregateMode(Enum):
    INDIVIDUAL = "individual"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class WikidatedAggregatedRevision(WikidatedRevision):
    # In this class we implement a fairly hacky way so that users do not have to specify
    # the revision parameter and so that it is never output even though this is formally
    # a subclass of `WikidatedRevision`.

    revisions: Sequence[WikidataRevisionMeta]

    def __init__(self, **kwargs: Any) -> None:
        revisions = kwargs["revisions"]
        kwargs["revision"] = revisions[0]
        super().__init__(**kwargs)

    # We are using a few "type: ignore" annotations here because it seems like Pydantic
    # uses `None` as a default value for both `include` and `exclude` even though this
    # would not be permitted by the type.
    def dict(
        self,
        *,
        include: Union[
            AbstractSet[Union[int, str]], Mapping[Union[int, str], Any]
        ] = None,  # type: ignore
        exclude: Union[
            AbstractSet[Union[int, str]], Mapping[Union[int, str], Any]
        ] = None,  # type: ignore
        by_alias: bool = False,
        skip_defaults: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Dict[str, Any]:
        if exclude is None:
            exclude = {"revision"}  # type: ignore
        elif isinstance(exclude, AbstractSet):
            exclude = set(exclude)
            exclude.add("revision")
        elif isinstance(exclude, Mapping):
            exclude = dict(exclude)
            exclude["revision"] = ...
        else:
            raise Exception(f"Unknown type for exclude: '{type(exclude)}'")
        return super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )


class WikidatedAggregatedDataset(WikidatedDataset):
    def __init__(
        self,
        data_dir: Path,
        wikidata_dump: WikidataDump,
        aggregate_mode: WikidatedAggregateMode,
    ) -> None:
        if aggregate_mode == WikidatedAggregateMode.INDIVIDUAL:
            raise ValueError(
                "WikidatedAggregatedDataset can not be used with INDIVIDUAL as "
                "aggregate mode. Just use WikidatedDataset instead."
            )

        super().__init__(data_dir, wikidata_dump)

    def iter_revisions(
        self, page_id: Optional[int] = None, sample_rate: Optional[float] = None
    ) -> Iterator[WikidatedAggregatedRevision]:
        raise NotImplementedError()  # TODO

    def entity_streams(self) -> WikidatedAggregatedEntityStreams:
        raise NotImplementedError()  # TODO

    def global_stream(self) -> WikidatedAggregatedGlobalStream:
        raise NotImplementedError()  # TODO


class WikidatedAggregatedEntityStreams(WikidatedEntityStreams):
    def iter_revisions(
        self, page_id: int, sample_rate: Optional[float] = None
    ) -> Iterator[WikidatedAggregatedRevision]:
        raise NotImplementedError()  # TODO


class WikidatedAggregatedGlobalStream(WikidatedGlobalStream):
    def iter_revisions(
        self, sample_rate: Optional[float] = None
    ) -> Iterator[WikidatedAggregatedRevision]:
        raise NotImplementedError()  # TODO

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

from typing import AbstractSet, Any, Dict, Mapping, Sequence, Union

from wikidated.wikidata import (
    WikidataRdfTriple,
    WikidataRevisionBase,
    WikidataRevisionMeta,
)


class WikidatedRevision(WikidataRevisionBase):
    triple_deletions: Sequence[WikidataRdfTriple]
    triple_additions: Sequence[WikidataRdfTriple]
    triple_deletions_sample: Sequence[float]
    triple_additions_sample: Sequence[float]


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

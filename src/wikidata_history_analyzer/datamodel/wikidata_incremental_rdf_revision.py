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

import gzip
from pathlib import Path
from typing import Iterator, Sequence, Set

from overrides import overrides

from wikidata_history_analyzer._paths import wikidata_incremental_rdf_revision_dir
from wikidata_history_analyzer.datamodel.wikidata_rdf_revision import (
    WikidataRdfRevision,
    WikidataRdfTriple,
)
from wikidata_history_analyzer.datamodel.wikidata_revision import WikidataRevision


class WikidataIncrementalRdfRevision(WikidataRevision):
    deleted_triples: Sequence[WikidataRdfTriple]
    added_triples: Sequence[WikidataRdfTriple]

    @classmethod
    @overrides
    def base_dir(cls, data_dir: Path) -> Path:
        return wikidata_incremental_rdf_revision_dir(data_dir)

    @classmethod
    def from_rdf_revisions(
        cls, revisions: Iterator[WikidataRdfRevision]
    ) -> Iterator[WikidataIncrementalRdfRevision]:
        state: Set[WikidataRdfTriple] = set()

        last_page_id = -1
        for revision in revisions:
            if last_page_id != revision.page_id:
                last_page_id = revision.page_id
                state = set()

            # TODO: blank notes (important!)
            triples_set = set(revision.triples)
            deleted_triples = state - triples_set
            added_triples = triples_set - state
            state -= deleted_triples
            state |= added_triples

            yield WikidataIncrementalRdfRevision(
                prefixed_title=revision.prefixed_title,
                namespace=revision.namespace,
                page_id=revision.page_id,
                redirect=revision.redirect,
                revision_id=revision.revision_id,
                parent_revision_id=revision.parent_revision_id,
                timestamp=revision.timestamp,
                contributor=revision.contributor,
                contributor_id=revision.contributor_id,
                is_minor=revision.is_minor,
                comment=revision.comment,
                content_model=revision.content_model,
                format=revision.format,
                sha1=revision.sha1,
                deleted_triples=sorted(deleted_triples),
                added_triples=sorted(added_triples),
            )

    @classmethod
    def iter_path(cls, data_dir: Path, dump_name: str, page_id: int) -> Path:
        return cls.base_dir(data_dir) / dump_name / (str(page_id) + ".jsonl.gz")

    @classmethod
    def save_iter_to_file(
        cls,
        revisions: Iterator[WikidataIncrementalRdfRevision],
        data_dir: Path,
        dump_name: str,
        page_id: int,
    ) -> None:
        path = cls.iter_path(data_dir, dump_name, page_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="UTF-8") as fout:
            for revision in revisions:
                assert revision.page_id == page_id
                fout.write(revision.json() + "\n")

    @classmethod
    def load_iter_from_file(
        cls, data_dir: Path, dump_name: str, page_id: int
    ) -> Iterator[WikidataIncrementalRdfRevision]:
        path = cls.iter_path(data_dir, dump_name, page_id)
        with gzip.open(path, "rt", encoding="UTF-8") as fin:
            for line in fin:
                yield cls.parse_raw(line)

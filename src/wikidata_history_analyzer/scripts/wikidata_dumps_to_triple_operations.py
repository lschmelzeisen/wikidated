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

from itertools import groupby
from logging import getLogger
from pathlib import Path
from sys import argv
from typing import Counter, Sequence

from jpype import shutdownJVM, startJVM  # type: ignore
from nasty_utils import Argument, ColoredBraceStyleAdapter, Program, ProgramConfig
from overrides import overrides

import wikidata_history_analyzer
from wikidata_history_analyzer._paths import (
    get_wikidata_dump_dir,
    get_wikidata_triple_operation_dir,
)
from wikidata_history_analyzer.java_logging_bride import setup_java_logging_bridge
from wikidata_history_analyzer.settings_ import WikidataHistoryAnalyzerSettings
from wikidata_history_analyzer.triple_operation_builder import TripleOperationBuilder
from wikidata_history_analyzer.wikidata_dump import WikidataDump
from wikidata_history_analyzer.wikidata_rdf_serializer import (
    RdfTriple,
    WikidataRdfSerializationException,
    WikidataRdfSerializer,
)

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataDumpsToTripleOperations(Program):
    class Config(ProgramConfig):
        title = "wikidata-dumps-to-triple-operations"
        version = wikidata_history_analyzer.__version__
        description = (
            "Serialize items from Wikidata dumps into streams of RDF triple operations."
        )

    settings: WikidataHistoryAnalyzerSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    dump_file: Path = Argument(
        alias="dump-file", short_alias="f", description="Dump file to process."
    )

    @overrides
    def run(self) -> None:
        settings = self.settings.wikidata_history_analyzer
        dump_dir = get_wikidata_dump_dir(settings.data_dir)
        triple_operation_dir = get_wikidata_triple_operation_dir(settings.data_dir)
        dump_file = dump_dir / self.dump_file
        dump = WikidataDump(dump_file)

        startJVM(classpath=[str(settings.wikidata_toolkit_jars_dir / "*")])
        setup_java_logging_bridge()

        rdf_serializer = WikidataRdfSerializer(
            dump_dir / f"wikidatawiki-{settings.wikidata_dump_version}-sites.sql.gz"
        )

        rdf_serializer_exception_counter = Counter[str]()
        num_revisions = 0
        for title, revisions in groupby(
            dump.iter_revisions(), lambda r: r.prefixed_title
        ):
            page_file = triple_operation_dir / dump_file.name / (title + ".ttlops")
            if page_file.exists():
                for _ in revisions:  # Deplete iterator.
                    pass
                # Skipping here, falsifies exception statistics, but I'm ok with that.
                continue

            with TripleOperationBuilder(Path(page_file)) as triple_operation_builder:
                for revision in revisions:
                    num_revisions += 1

                    try:
                        triples = rdf_serializer.process_revision(revision)
                    except WikidataRdfSerializationException as exception:
                        rdf_serializer_exception_counter[exception.reason] += 1
                        continue

                    triple_operation_builder.process_triples(
                        self._filter_triples(triples), revision.timestamp
                    )

        with (triple_operation_dir / dump_file.name / "exceptions.log").open(
            "w", encoding="UTF-8"
        ) as fout:
            fout.write(
                "Exceptions occurred when RDF-serializing "
                f"(out of {num_revisions} revisions):\n"
            )
            for reason, count in rdf_serializer_exception_counter.most_common():
                fout.write(f"  {reason} ({count})\n")

        shutdownJVM()

    @classmethod
    def _filter_triples(cls, triples: Sequence[RdfTriple]) -> Sequence[RdfTriple]:
        # Remove all triples, where the respective entity is not the subject, and all
        # triples, which do not point to other Wikidata items with Wikidata predicates.

        if not triples:
            return triples

        # First triple of every revision always has the form:
        # <entity> rdf:type wikibase:<entity-type>
        entity = triples[0].subject

        return [
            triple
            for triple in triples
            if (
                triple.subject == entity
                and triple.predicate.startswith("wdt:")
                and triple.object.startswith("wd:")
            )
        ]


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataDumpsToTripleOperations.init(*args).run()


if __name__ == "__main__":
    main()

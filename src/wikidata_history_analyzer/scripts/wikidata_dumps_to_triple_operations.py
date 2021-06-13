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

import atexit
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import groupby
from logging import getLogger
from os import getpid
from pathlib import Path
from sys import argv
from typing import Counter, Sequence

from jpype import shutdownJVM, startJVM  # type: ignore
from nasty_utils import Argument, ColoredBraceStyleAdapter, Program, ProgramConfig
from overrides import overrides
from tqdm import tqdm

import wikidata_history_analyzer
from wikidata_history_analyzer._paths import (
    get_wikidata_dump_dir,
    get_wikidata_triple_operation_dir,
)
from wikidata_history_analyzer.java_logging_bride import (
    set_java_logging_file_handler,
    setup_java_logging_bridge,
)
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

    @overrides
    def run(self) -> None:
        dump_files = (
            "wikidatawiki-20210401-pages-meta-history1.xml-p407p543.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p1683p3894.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p1318p1682.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p7685p10732.7z",
        )
        with ProcessPoolExecutor(
            max_workers=self.settings.wikidata_history_analyzer.num_workers,
            initializer=self._init_worker,
            initargs=(
                self.settings.wikidata_history_analyzer.wikidata_toolkit_jars_dir / "*",
            ),
        ) as pool, tqdm(desc="Running", total=len(dump_files)) as progress_bar:
            futures = (
                pool.submit(self._process_dump_file, Path(dump_file))
                for dump_file in dump_files
            )
            for future in as_completed(futures):
                assert future.result(timeout=0) is None
                progress_bar.update(1)

    @classmethod
    def _init_worker(cls, jvm_classpath: Path) -> None:
        _LOGGER.info("Starting JVM in worker process {}...", getpid())
        startJVM(classpath=[str(jvm_classpath)])
        setup_java_logging_bridge()
        atexit.register(cls._exit_worker)

    @classmethod
    def _exit_worker(cls) -> None:
        _LOGGER.info("Shutting down JVM in worker process {}...", getpid())
        shutdownJVM()

    def _process_dump_file(self, dump_file: Path) -> None:
        settings = self.settings.wikidata_history_analyzer
        dump_dir = get_wikidata_dump_dir(settings.data_dir)
        triple_operation_dir = get_wikidata_triple_operation_dir(settings.data_dir)

        out_dump_dir = triple_operation_dir / dump_file.name
        out_dump_dir.mkdir(parents=True, exist_ok=True)
        set_java_logging_file_handler(out_dump_dir / "rdf-serialization.exceptions.log")

        dump = WikidataDump(dump_dir / dump_file)
        rdf_serializer = WikidataRdfSerializer(
            dump_dir / f"wikidatawiki-{settings.wikidata_dump_version}-sites.sql.gz"
        )

        rdf_serializer_exception_counter = Counter[str]()
        num_revisions = 0
        for title, revisions in groupby(
            dump.iter_revisions(), lambda r: r.prefixed_title
        ):
            page_file = out_dump_dir / (title + ".ttlops")
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

        with (triple_operation_dir / dump_file.name / "rdf-serialization.log").open(
            "w", encoding="UTF-8"
        ) as fout:
            fout.write(
                "Exceptions occurred when RDF-serializing "
                f"(out of {num_revisions} revisions):\n"
            )
            for reason, count in rdf_serializer_exception_counter.most_common():
                fout.write(f"  {reason} ({count})\n")

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

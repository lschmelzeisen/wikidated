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
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from itertools import groupby
from logging import getLogger
from multiprocessing import Manager
from os import getpid
from pathlib import Path
from sys import argv
from typing import Counter, MutableMapping, Sequence, Tuple

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

_UPDATE_FREQUENCY = 5  # Seconds


class WikidataExtractTripleOperations(Program):
    class Config(ProgramConfig):
        title = "wikidata-extract-triple-operations"
        version = wikidata_history_analyzer.__version__
        description = "Extract steams of RDF triple operations for each page."

    settings: WikidataHistoryAnalyzerSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    @overrides
    def run(self) -> None:
        dump_files = (
            "wikidatawiki-20210401-pages-meta-history1.xml-p1000p1155.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p10733p14304.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p1156p1317.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p1318p1682.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p14305p18638.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p1683p3894.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p193p353.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p1p192.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p267210p283697.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p354p406.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p3895p7684.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p407p543.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p544p999.7z",
            "wikidatawiki-20210401-pages-meta-history1.xml-p7685p10732.7z",
            "wikidatawiki-20210401-pages-meta-history25.xml-p67174382p67502430.7z",
            "wikidatawiki-20210401-pages-meta-history26.xml-p81385859p81615147.7z",
        )

        num_workers = self.settings.wikidata_history_analyzer.num_workers
        with ProcessPoolExecutor(
            max_workers=self.settings.wikidata_history_analyzer.num_workers,
            initializer=self._init_worker,
            initargs=(
                self.settings.wikidata_history_analyzer.wikidata_toolkit_jars_dir / "*",
            ),
        ) as pool, tqdm(  # Progress bar for total progress.
            total=len(dump_files), dynamic_ncols=True, position=-num_workers
        ) as progress_bar_overall, Manager() as manager:
            # The subprocesses use progress_dict to communicate their progress with the
            # main progress, which then updates the progress_bars accordingly.
            # Key is always the name of the respective progress bar (the name of the
            # dump file). Value of progress_dict is a tuple (cur_pages, max_pages).
            progress_dict: MutableMapping[
                str, Tuple[int, int]
            ] = manager.dict()  # type: ignore
            progress_bars: MutableMapping[str, tqdm[None]] = {}

            futures_not_done = {
                pool.submit(self._process_dump_file, Path(dump_file), progress_dict)
                for dump_file in dump_files
            }

            while True:
                futures_done, futures_not_done = wait(
                    futures_not_done,
                    timeout=_UPDATE_FREQUENCY,
                    return_when=FIRST_COMPLETED,
                )

                for progress_bar_name, (num_pages, max_pages) in progress_dict.items():
                    progress_bar = progress_bars.get(progress_bar_name)
                    if not progress_bar:
                        progress_bar = tqdm(
                            desc=progress_bar_name, total=max_pages, dynamic_ncols=True
                        )
                        progress_bars[progress_bar_name] = progress_bar
                    progress_bar.n = num_pages
                    progress_bar.total = max_pages
                    progress_bar.refresh()
                    if num_pages == max_pages:
                        progress_bar.close()
                    progress_bar_overall.refresh()

                if futures_done:
                    for future in futures_done:
                        # Calling .result() triggers potential exceptions passed on
                        # from subprocesses.
                        assert future.result(timeout=0) is None
                        progress_bar_overall.update(1)

                if not futures_not_done:
                    break

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

    def _process_dump_file(
        self, dump_file: Path, progress_dict: MutableMapping[str, Tuple[int, int]]
    ) -> None:
        settings = self.settings.wikidata_history_analyzer
        dump_dir = get_wikidata_dump_dir(settings.data_dir)
        triple_operation_dir = get_wikidata_triple_operation_dir(settings.data_dir)
        triple_operation_dump_dir = triple_operation_dir / dump_file.name
        triple_operation_dump_dir.mkdir(parents=True, exist_ok=True)
        dump = WikidataDump(dump_dir / dump_file)

        # Upper bound for the number of possible pages in a dump.
        max_pages = int(dump.max_page_id) - int(dump.min_page_id) + 1
        progress_dict[dump_file.name] = (0, max_pages)

        set_java_logging_file_handler(
            triple_operation_dump_dir / "rdf-serialization.exceptions.log"
        )
        rdf_serializer = WikidataRdfSerializer(
            dump_dir / f"wikidatawiki-{settings.wikidata_dump_version}-sites.sql.gz"
        )
        rdf_serializer_exception_counter = Counter[str]()

        num_pages = 0
        num_revisions = 0
        for num_pages, (title, revisions) in enumerate(
            groupby(
                dump.iter_revisions(display_progress_bar=False),
                lambda r: r.prefixed_title,
            )
        ):
            page_file = triple_operation_dump_dir / (title + ".ttlops")
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

            progress_dict[dump_file.name] = (num_pages, max_pages)

        set_java_logging_file_handler(None)

        # Now we know how many pages where actually in the dump and can correct the
        # previous upper bound of max_pages. Also this will trigger closing this
        # subprocesses progress bar in the main thread.
        progress_dict[dump_file.name] = (num_pages, num_pages)

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
    WikidataExtractTripleOperations.init(*args).run()


if __name__ == "__main__":
    main()

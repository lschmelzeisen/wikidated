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
from typing import Counter, MutableMapping, Tuple

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
from wikidata_history_analyzer.wikidata_dump_manager import WikidataDumpManager
from wikidata_history_analyzer.wikidata_meta_history_7z_dump import (
    WikidataMetaHistory7zDump,
)
from wikidata_history_analyzer.wikidata_rdf_serializer import (
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
        settings = self.settings.wikidata_history_analyzer

        dump_manager = WikidataDumpManager(
            settings.data_dir,
            settings.wikidata_dump_version,
            settings.wikidata_dump_mirror_base,
        )

        num_workers = self.settings.wikidata_history_analyzer.num_workers
        with ProcessPoolExecutor(
            max_workers=self.settings.wikidata_history_analyzer.num_workers,
            initializer=self._init_worker,
            initargs=(
                self.settings.wikidata_history_analyzer.wikidata_toolkit_jars_dir / "*",
            ),
        ) as pool, tqdm(  # Progress bar for total progress.
            total=len(dump_manager.meta_history_7z_dumps()),
            dynamic_ncols=True,
            position=-num_workers,
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
                pool.submit(
                    self._process_dump_file, meta_history_7z_dump, progress_dict
                )
                for meta_history_7z_dump in dump_manager.meta_history_7z_dumps()
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
        self,
        dump: WikidataMetaHistory7zDump,
        progress_dict: MutableMapping[str, Tuple[int, int]],
    ) -> None:
        settings = self.settings.wikidata_history_analyzer
        dump_dir = get_wikidata_dump_dir(settings.data_dir)
        triple_operation_dir = get_wikidata_triple_operation_dir(settings.data_dir)
        triple_operation_dump_dir = triple_operation_dir / dump.path.name
        triple_operation_dump_dir.mkdir(parents=True, exist_ok=True)

        # Upper bound for the number of possible pages in a dump.
        max_pages = int(dump.max_page_id) - int(dump.min_page_id) + 1
        progress_dict[dump.path.name] = (0, max_pages)

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
                        triples, revision.timestamp
                    )

            progress_dict[dump.path.name] = (num_pages, max_pages)

        # Now we know how many pages where actually in the dump and can correct the
        # previous upper bound of max_pages. Also this will trigger closing this
        # subprocesses progress bar in the main thread.
        progress_dict[dump.path.name] = (num_pages, num_pages)

        set_java_logging_file_handler(None)

        with (triple_operation_dir / dump.path.name / "rdf-serialization.log").open(
            "w", encoding="UTF-8"
        ) as fout:
            fout.write(
                "Exceptions occurred when RDF-serializing "
                f"(out of {num_revisions} revisions):\n"
            )
            for reason, count in rdf_serializer_exception_counter.most_common():
                fout.write(f"  {reason} ({count})\n")


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataExtractTripleOperations.init(*args).run()


if __name__ == "__main__":
    main()

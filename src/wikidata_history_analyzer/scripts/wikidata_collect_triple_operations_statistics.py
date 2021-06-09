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

import lzma
import re
from logging import getLogger
from pathlib import Path
from sys import argv

from nasty_utils import Argument, ColoredBraceStyleAdapter, Program, ProgramConfig
from overrides import overrides
from pydantic import BaseModel as PydanticModel

import wikidata_history_analyzer
from wikidata_history_analyzer._paths import get_wikidata_triple_operation_dir
from wikidata_history_analyzer.settings_ import WikidataHistoryAnalyzerSettings
from wikidata_history_analyzer.triple_operation_builder import (
    TripleOperation,
    TripleOperationType,
)

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class TripleOperationStatistics(PydanticModel):
    pass


class WikidataCollectTripleOperationsStatistics(Program):
    class Config(ProgramConfig):
        title = "wikidata-collect-triple-operations-statistics"
        version = wikidata_history_analyzer.__version__
        description = "Collect statistics from triple operations."

    settings: WikidataHistoryAnalyzerSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    @overrides
    def run(self) -> None:
        settings = self.settings.wikidata_history_analyzer
        triple_operation_dir = get_wikidata_triple_operation_dir(settings.data_dir)

        for d in sorted(triple_operation_dir.iterdir()):
            if not d.is_dir():
                continue

            statistics_file = d / "statistics.json"
            if statistics_file.exists():
                continue

            for i, f in enumerate(sorted(d.iterdir())):
                if f.name.endswith(".7z"):
                    continue

                self._process_file(f)

                if i == 10:
                    break

            break

    @classmethod
    def _process_file(cls, file: Path) -> TripleOperationStatistics:
        _LOGGER.info("Processing file '{}'...", file)

        # We have some weird parsing code here for now, because the data I'm working on
        # at the moment, did not export correctly. TODO: remove this.
        with lzma.open(file, "rt", encoding="UTF-8") as fin:
            split = re.split(r"(\d{4}-)", str(fin.read()))[1:]
        triple_ops = []
        for i in range(len(split) // 2):
            op = TripleOperation.from_str(split[2 * i] + split[2 * i + 1])
            if op.type == TripleOperationType.ADD:
                op = TripleOperation(
                    op.timestamp,
                    TripleOperationType.DELETE,
                    op.subject,
                    op.predicate,
                    op.object,
                )
            elif op.type == TripleOperationType.DELETE:
                op = TripleOperation(
                    op.timestamp,
                    TripleOperationType.ADD,
                    op.subject,
                    op.predicate,
                    op.object,
                )
            else:
                raise NotImplementedError()
            triple_ops.append(op)

        for x in triple_ops:
            _LOGGER.info("{}", x)

        return TripleOperationStatistics()


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataCollectTripleOperationsStatistics.init(*args).run()


if __name__ == "__main__":
    main()

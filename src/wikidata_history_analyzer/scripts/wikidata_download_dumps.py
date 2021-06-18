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

from logging import getLogger
from sys import argv

from nasty_utils import Argument, ColoredBraceStyleAdapter, Program, ProgramConfig
from overrides import overrides

import wikidata_history_analyzer
from wikidata_history_analyzer.settings_ import WikidataHistoryAnalyzerSettings
from wikidata_history_analyzer.wikidata_dump_manager import WikidataDumpManager

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataDownloadDumps(Program):
    class Config(ProgramConfig):
        title = "wikidata-download-dumps"
        version = wikidata_history_analyzer.__version__
        description = "Download Wikidata dumps."

    settings: WikidataHistoryAnalyzerSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    @overrides
    def run(self) -> None:
        settings = self.settings.wikidata_history_analyzer

        manager = WikidataDumpManager(
            settings.data_dir,
            settings.wikidata_dump_version,
            settings.wikidata_dump_mirror_base,
        )
        manager.download_all()


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataDownloadDumps.init(*args).run()


if __name__ == "__main__":
    main()

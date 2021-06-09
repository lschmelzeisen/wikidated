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
from pathlib import Path
from sys import argv
from typing import Mapping, cast

import requests
from nasty_utils import (
    Argument,
    ColoredBraceStyleAdapter,
    Program,
    ProgramConfig,
    download_file_with_progressbar,
)
from overrides import overrides
from tqdm import tqdm

import wikidata_history_analyzer
from wikidata_history_analyzer._paths import get_wikidata_dump_dir
from wikidata_history_analyzer._utils import sha1sum
from wikidata_history_analyzer.settings_ import WikidataHistoryAnalyzerSettings

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
        dump_dir = get_wikidata_dump_dir(
            self.settings.wikidata_history_analyzer.data_dir
        )
        dump_dir.mkdir(exist_ok=True, parents=True)
        dump_mirror_base = (
            self.settings.wikidata_history_analyzer.wikidata_dump_mirror_base
        )
        dump_version = self.settings.wikidata_history_analyzer.wikidata_dump_version

        _LOGGER.info("Downloading dump status...")
        dump_status_url = (
            f"{dump_mirror_base}/wikidatawiki/{dump_version}/dumpstatus.json"
        )
        dump_status = requests.get(dump_status_url).json()

        self._download_files("sitestable", dump_status, dump_dir, dump_mirror_base)
        self._download_files(
            "metahistory7zdump", dump_status, dump_dir, dump_mirror_base
        )

    @classmethod
    def _download_files(
        cls,
        job: str,
        dump_status: Mapping[str, Mapping[str, Mapping[str, object]]],
        dump_dir: Path,
        dump_mirror_base: str,
    ) -> None:
        _LOGGER.info("Downloading {}...", job)
        dump_status_job = dump_status["jobs"][job]
        cls._assert_status_done(job, dump_status_job)

        files = cast(Mapping[str, Mapping[str, object]], dump_status_job["files"])
        total_size = sum(cast(int, file_info["size"]) for file_info in files.values())
        with tqdm(
            total=len(files), dynamic_ncols=True, position=1
        ) as progress_bar_files, tqdm(
            total=total_size,
            dynamic_ncols=True,
            position=2,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar_size:
            for file_name, file_info in files.items():
                file = dump_dir / file_name
                if file.exists():
                    _LOGGER.info("File {} already exists, skipping...", file)
                    progress_bar_files.update(1)
                    progress_bar_size.update(cast(int, file_info["size"]))
                    continue

                file_tmp = file.parent / (file.name + ".tmp")
                url = dump_mirror_base + cast(str, file_info["url"])
                download_file_with_progressbar(url, file_tmp, description=file_name)

                sha1_expected = cast(str, file_info["sha1"])
                sha1_actual = sha1sum(file_tmp)
                if sha1_expected != sha1_actual:
                    raise Exception(
                        f"SHA-1 did not match. Expected '{sha1_expected}', but "
                        f"received {sha1_actual}'."
                    )

                file_tmp.rename(file)
                progress_bar_files.update(1)
                progress_bar_size.update(cast(int, file_info["size"]))

    @classmethod
    def _assert_status_done(
        cls, job: str, dump_status_job: Mapping[str, object]
    ) -> None:
        status = cast(str, dump_status_job["status"])
        if status != "done":
            raise Exception(f"Dump status of {job} is not 'done' but '{status}'.")


def main(*args: str) -> None:
    if not args:
        args = tuple(argv[1:])
    WikidataDownloadDumps.init(*args).run()


if __name__ == "__main__":
    main()

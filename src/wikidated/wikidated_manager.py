#
# Copyright 2021-2022 Lukas Schmelzeisen
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

from datetime import date
from logging import DEBUG, INFO, FileHandler, Formatter, getLogger
from logging import root as root_logger
from pathlib import Path
from typing import Optional, Union

from tqdm.contrib.logging import _TqdmLoggingHandler  # type: ignore
from typing_extensions import Final

from wikidated._utils import JavaArtifact, JavaDependencyDownloader
from wikidated.wikidata import WikidataDump
from wikidated.wikidated_dataset import WikidatedDataset
from wikidated.wikidated_v1_0 import WikidatedV1_0Dataset


class WikidatedManager:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir: Final = data_dir
        self.dump_dir: Final = data_dir / "dumpfiles"
        self.jars_dir: Final = data_dir / "jars"
        self.maven_dir: Final = data_dir / "maven"

    def wikidata_dump(
        self, version: date, *, mirror: str = "https://dumps.wikimedia.org"
    ) -> WikidataDump:
        return WikidataDump(self.dump_dir, version=version, mirror=mirror)

    def load_custom(
        self,
        dataset_dir_or_wikidata_dump: Union[Path, WikidataDump],
    ) -> WikidatedDataset:
        if isinstance(dataset_dir_or_wikidata_dump, Path):
            dataset_dir = dataset_dir_or_wikidata_dump
        elif isinstance(dataset_dir_or_wikidata_dump, WikidataDump):
            dataset_dir = (
                self.data_dir
                / f"wikidated-custom-{dataset_dir_or_wikidata_dump.version:%4Y%2m%2d}"
            )
        else:
            raise TypeError(
                "dataset_dir_or_wikidata_dump must be either a Path or a WikidataDump."
            )
        return WikidatedDataset.load_custom(dataset_dir)

    def build_custom(
        self, wikidata_dump: WikidataDump, max_workers: Optional[int] = 4
    ) -> WikidatedDataset:
        return WikidatedDataset.build_custom(
            self.data_dir / f"wikidated-custom-{wikidata_dump.version:%4Y%2m%2d}",
            self.jars_dir,
            wikidata_dump,
            max_workers=max_workers,
        )

    def v1_0(self, auto_download: bool = True) -> WikidatedV1_0Dataset:
        return WikidatedV1_0Dataset.load_v1_0(
            self.data_dir / "wikidated-1.0", auto_download=auto_download
        )

    def configure_logging(
        self,
        *,
        console: Union[bool, int] = True,
        console_fmt: str = "{asctime} {levelname:.1} {message}",
        file: Union[bool, int] = True,
        file_path: Optional[Path] = None,
        file_fmt: str = (
            "{asctime} {levelname} [{name}:{funcName}@{processName}] {message}"
        ),
        log_wdtk: bool = False,
    ) -> None:
        overall_level = root_logger.level

        if file is not False:
            level = DEBUG if file is True else file
            overall_level = min(overall_level, level)
            if not file_path:
                file_path = self.data_dir / "wikidated.log"
            file_handler = FileHandler(file_path, encoding="UTF-8")
            file_handler.setLevel(level)
            file_handler.setFormatter(Formatter(file_fmt, style="{"))
            root_logger.addHandler(file_handler)

        if console is not False:
            level = INFO if console is True else console
            overall_level = min(overall_level, level)
            console_handler = _TqdmLoggingHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(Formatter(console_fmt, style="{"))
            root_logger.addHandler(console_handler)

        root_logger.setLevel(overall_level)

        if not log_wdtk:
            getLogger("jpype.org.wikidata.wdtk").propagate = False

    def download_java_dependencies(self) -> None:
        java_dependency_downloader = JavaDependencyDownloader(
            jars_dir=self.jars_dir, maven_dir=self.maven_dir
        )
        java_dependency_downloader.download_java_dependencies(
            (
                JavaArtifact("org.slf4j", "slf4j-jdk14", "1.7.36"),
                JavaArtifact("org.wikidata.wdtk", "wdtk-datamodel", "0.13.1"),
                JavaArtifact("org.wikidata.wdtk", "wdtk-dumpfiles", "0.13.1"),
                JavaArtifact("org.wikidata.wdtk", "wdtk-rdf", "0.13.1"),
            )
        )

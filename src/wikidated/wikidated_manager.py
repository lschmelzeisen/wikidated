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

from pathlib import Path

from wikidated._utils import JavaArtifact, JavaDependencyDownloader
from wikidated.wikidata import WikidataDump
from wikidated.wikidated_dataset import WikidatedDataset


class WikidatedManager:
    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir

    @property
    def data_dir(self) -> Path:
        return self._data_dir

    @property
    def jars_dir(self) -> Path:
        return self._data_dir / "jars"

    @property
    def maven_dir(self) -> Path:
        return self._data_dir / "maven"

    def custom(self, wikidata_dump: WikidataDump) -> WikidatedDataset:
        return WikidatedDataset(
            self.data_dir / f"wikidated-custom-{wikidata_dump.version}",
            self.jars_dir,
            wikidata_dump,
        )

    def v1_0(self) -> WikidatedDataset:
        raise NotImplementedError()  # TODO

    def page_id_from_entity_id(self, entity_id: str) -> int:
        raise NotImplementedError()  # TODO

    def entity_id_from_page_id(self, page_id: int) -> str:
        raise NotImplementedError()  # TODO

    def download_java_dependencies(self) -> None:
        java_dependency_downloader = JavaDependencyDownloader(
            jars_dir=self.jars_dir, maven_dir=self.maven_dir
        )
        java_dependency_downloader.download_java_dependencies(
            (
                JavaArtifact("org.slf4j", "slf4j-jdk14", "1.7.32"),
                JavaArtifact("org.wikidata.wdtk", "wdtk-datamodel", "0.12.1"),
                JavaArtifact("org.wikidata.wdtk", "wdtk-dumpfiles", "0.12.1"),
                JavaArtifact("org.wikidata.wdtk", "wdtk-rdf", "0.12.1"),
            )
        )

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
from typing import overload

from typing_extensions import Literal

from wikidated._utils import JavaArtifact, JavaDependencyDownloader, JvmManager
from wikidated.wikidata import WikidataDump
from wikidated.wikidated_aggregated_dataset import (
    WikidatedAggregatedDataset,
    WikidatedAggregateMode,
)
from wikidated.wikidated_dataset import WikidatedDataset

_L_INDIVIDUAL = Literal[WikidatedAggregateMode.INDIVIDUAL]
_L_AGGREGATED = Literal[
    WikidatedAggregateMode.HOURLY,
    WikidatedAggregateMode.DAILY,
    WikidatedAggregateMode.WEEKLY,
    WikidatedAggregateMode.MONTHLY,
]


class WikidatedManager:
    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir

    @overload
    def custom(self, wikidata_dump: WikidataDump) -> WikidatedDataset:
        ...

    @overload
    def custom(
        self, wikidata_dump: WikidataDump, aggregate_mode: _L_INDIVIDUAL
    ) -> WikidatedDataset:
        ...

    @overload
    def custom(
        self, wikidata_dump: WikidataDump, aggregate_mode: _L_AGGREGATED
    ) -> WikidatedAggregatedDataset:
        ...

    def custom(
        self,
        wikidata_dump: WikidataDump,
        aggregate_mode: WikidatedAggregateMode = WikidatedAggregateMode.INDIVIDUAL,
    ) -> WikidatedDataset:
        if aggregate_mode == WikidatedAggregateMode.INDIVIDUAL:
            return WikidatedDataset(self._data_dir, wikidata_dump)
        else:
            return WikidatedAggregatedDataset(
                self._data_dir, wikidata_dump, aggregate_mode
            )

    @overload
    def v1_0(self) -> WikidatedDataset:
        ...

    @overload
    def v1_0(self, aggregate_mode: _L_INDIVIDUAL) -> WikidatedDataset:
        ...

    @overload
    def v1_0(self, aggregate_mode: _L_AGGREGATED) -> WikidatedAggregatedDataset:
        ...

    def v1_0(
        self, aggregate_mode: WikidatedAggregateMode = WikidatedAggregateMode.INDIVIDUAL
    ) -> WikidatedDataset:
        raise NotImplementedError()  # TODO

    def page_id_from_entity_id(self, entity_id: str) -> int:
        raise NotImplementedError()  # TODO

    def entity_id_from_page_id(self, page_id: int) -> str:
        raise NotImplementedError()  # TODO

    def jvm_manager(self, download_java_dependencies: bool = True) -> JvmManager:
        jars_dir = self._data_dir / "jars"
        maven_dir = self._data_dir / "maven"

        if download_java_dependencies:
            java_dependency_downloader = JavaDependencyDownloader(
                jars_dir=jars_dir, maven_dir=maven_dir
            )
            java_dependency_downloader.download_java_dependencies(
                (
                    JavaArtifact("org.slf4j", "slf4j-jdk14", "1.7.32"),
                    JavaArtifact("org.wikidata.wdtk", "wdtk-datamodel", "0.12.1"),
                    JavaArtifact("org.wikidata.wdtk", "wdtk-dumpfiles", "0.12.1"),
                    JavaArtifact("org.wikidata.wdtk", "wdtk-rdf", "0.12.1"),
                )
            )

        return JvmManager(jars_dir=jars_dir)

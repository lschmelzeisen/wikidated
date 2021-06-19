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

from typing import Optional

from jpype import JClass, JObject  # type: ignore

from wikidata_history_analyzer.dumpfiles.wikidata_dump import WikidataDump
from wikidata_history_analyzer.jvm_manager import JvmManager


class WikidataSitesTable(WikidataDump):
    _wdtk_object: Optional[JObject] = None

    def load_wdtk_object(self, _jvm_manager: JvmManager) -> JObject:
        assert self.path.exists()

        if self._wdtk_object is None:
            dump = JClass("org.wikidata.wdtk.dumpfiles.MwLocalDumpFile")(str(self.path))
            processor = JClass("org.wikidata.wdtk.dumpfiles.MwSitesDumpFileProcessor")()
            processor.processDumpFileContents(dump.getDumpFileStream(), dump)
            self._wdtk_object = processor.getSites()

        return self._wdtk_object

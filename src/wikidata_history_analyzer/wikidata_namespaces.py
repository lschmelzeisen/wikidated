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

import gzip
import json
from typing import Any, Mapping

from wikidata_history_analyzer.wikidata_dump import WikidataDump


class WikidataNamespaces(WikidataDump):
    def load_namespace_titles(self) -> Mapping[int, str]:
        assert self.path.exists()

        with gzip.open(self.path, "rt", encoding="UTF-8") as fin:
            obj: Any = json.load(fin)

        return {
            int(namespace["id"]): namespace["*"]
            for namespace in obj["query"]["namespaces"].values()
        }

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
from logging import getLogger
from pathlib import Path

from wikidated.wikidated_manager import WikidatedManager

_LOGGER = getLogger(__name__)

if __name__ == "__main__":
    try:
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True, parents=True)

        wikidated_manager = WikidatedManager(data_dir)
        wikidated_manager.configure_logging(log_wdtk=True)
        wikidated_manager.download_java_dependencies()

        wikidata_dump = wikidated_manager.wikidata_dump(date(year=2021, month=6, day=1))
        wikidata_dump.download()

        wikidated_dataset = wikidated_manager.custom(wikidata_dump)
        wikidated_dataset.build(max_workers=4)

    except Exception:
        # Make exceptions show up in log.
        _LOGGER.exception("Exception occurred.")
        raise

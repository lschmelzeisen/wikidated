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

from hashlib import sha1
from logging import getLogger
from pathlib import Path

from typing_extensions import Final

from wikidated._utils import download_file_with_progressbar, hashcheck

_LOGGER = getLogger(__name__)


class WikidataDumpFile:
    def __init__(self, *, path: Path, url: str, sha1: str, size: int) -> None:
        self.path: Final = path
        self.url: Final = url
        self.sha1: Final = sha1
        self.size: Final = size

    def download(self) -> None:
        if self.path.exists():
            hashcheck(self.path, sha1(), self.sha1)
            _LOGGER.debug(
                f"Wikidata dump file '{self.path.name}' already exists with matching "
                "sha1 checksum, skipping download."
            )
            return

        _LOGGER.debug(
            f"Downloading Wikidata dump file '{self.path.name}' from '{self.url}'."
        )
        self.path.parent.mkdir(exist_ok=True, parents=True)
        path_tmp = self.path.parent / ("tmp." + self.path.name)
        download_file_with_progressbar(self.url, path_tmp, description=self.path.name)
        hashcheck(path_tmp, sha1(), self.sha1)
        path_tmp.rename(self.path)
        _LOGGER.debug(f"Done downloading Wikidata dump file '{self.path.name}'.")

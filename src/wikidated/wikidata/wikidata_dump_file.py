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

from wikidated._utils import download_file_with_progressbar, hashcheck

_LOGGER = getLogger(__name__)


class WikidataDumpFile:
    def __init__(self, *, path: Path, url: str, sha1: str, size: int) -> None:
        self._path = path
        self._url = url
        self._sha1 = sha1
        self._size = size

    def download(self) -> None:
        if self._path.exists():
            hashcheck(self._path, sha1(), self._sha1)
            _LOGGER.debug(
                f"File '{self._path.name}' already exists, skipping download..."
            )
            return

        self._path.parent.mkdir(exist_ok=True, parents=True)
        path_tmp = self._path.parent / ("tmp." + self._path.name)
        download_file_with_progressbar(self._url, path_tmp, description=self._path.name)
        hashcheck(path_tmp, sha1(), self._sha1)
        path_tmp.rename(self._path)

    @property
    def size(self) -> int:
        return self._size

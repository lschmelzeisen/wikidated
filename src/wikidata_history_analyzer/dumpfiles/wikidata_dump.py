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

from nasty_utils import ColoredBraceStyleAdapter, download_file_with_progressbar

from wikidata_history_analyzer._utils import sha1sum

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataDump:
    # Base class for all types of Wikidata dump formats.

    def __init__(self, *, path: Path, url: str, sha1: str, size: int) -> None:
        self.path = path
        self.url = url
        self.sha1 = sha1
        self.size = size

    def download(self) -> None:
        if self.path.exists():
            sha1 = sha1sum(self.path)
            if sha1 != self.sha1:
                raise Exception(
                    "SHA-1 of already downloaded file did not match. "
                    f"Expected '{self.sha1}', but received {sha1}'."
                )
            _LOGGER.debug("File '{}' already exists, skipping...", self.path.name)
            return

        path_tmp = self.path.parent / (self.path.name + ".tmp")
        download_file_with_progressbar(self.url, path_tmp, description=self.path.name)

        sha1 = sha1sum(path_tmp)
        if sha1 != self.sha1:
            raise Exception(
                "SHA-1 of file just downloaded did not match. "
                f"Expected '{self.sha1}', but received {sha1}'."
            )

        path_tmp.rename(self.path)

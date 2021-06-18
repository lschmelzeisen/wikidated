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

from __future__ import annotations

import json
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Mapping, Sequence

import requests
from nasty_utils import ColoredBraceStyleAdapter, download_file_with_progressbar
from pydantic import BaseModel as PydanticModel
from pydantic import validator
from tqdm import tqdm

from wikidata_history_analyzer._paths import get_wikidata_dump_dir
from wikidata_history_analyzer._utils import sha1sum

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataDumpFile(PydanticModel):
    size: int
    url: str
    md5: str
    sha1: str

    def download(self, path: Path, dump_mirror: str) -> None:
        if path.exists():
            sha1 = sha1sum(path)
            if sha1 != self.sha1:
                raise Exception(
                    "SHA-1 of already downloaded file did not match. "
                    f"Expected '{self.sha1}', but received {sha1}'."
                )
            _LOGGER.debug("File '{}' already exists, skipping...", path.name)
            return

        path_tmp = path.parent / (path.name + ".tmp")
        download_file_with_progressbar(
            dump_mirror + self.url, path_tmp, description=path.name
        )

        sha1 = sha1sum(path_tmp)
        if sha1 != self.sha1:
            raise Exception(
                "SHA-1 of file just downloaded did not match. "
                f"Expected '{self.sha1}', but received {sha1}'."
            )

        path_tmp.rename(path)


class WikidataDumpJob(PydanticModel):
    status: str
    updated: datetime
    files: Mapping[str, WikidataDumpFile]

    @classmethod
    @validator("updated", pre=True)
    def _parse_datetime(cls, value: str) -> datetime:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


class WikidataDumpStatus(PydanticModel):
    jobs: Mapping[str, WikidataDumpJob]
    version: str


class WikidataSitesTable:
    def __init__(self, path: Path, dump_file: WikidataDumpFile):
        self.path = path
        self.dump_file = dump_file


class WikidataMetaHistory7zDump:
    def __init__(self, path: Path, dump_file: WikidataDumpFile):
        self.path = path
        self.dump_file = dump_file


class WikidataDumpManager:
    def __init__(self, data_dir: Path, dump_version: str, dump_mirror: str):
        self._dump_dir = get_wikidata_dump_dir(data_dir)
        self._dump_version = dump_version
        self._dump_mirror = dump_mirror

        self._dump_status = self._load_dump_status()

    def _load_dump_status(self) -> WikidataDumpStatus:
        file = self._dump_dir / f"wikidatawiki-{self._dump_version}-dumpstatus.json"
        if not file.exists():
            url = (
                f"{self._dump_mirror}/wikidatawiki/{self._dump_version}/dumpstatus.json"
            )
            _LOGGER.debug("Downloading dumpstatus from URL '{}'...", url)
            with file.open("w", encoding="UTF-8") as fout:
                fout.write(json.dumps(requests.get(url).json(), indent=2) + "\n")

        dump_status = WikidataDumpStatus.parse_file(file)
        for job_name, job in dump_status.jobs.items():
            if job.status != "done":
                file.unlink()
                raise Exception(f"Job '{job_name}' is not 'done', but '{job.status}'.")

        return dump_status

    def sites_table(self) -> WikidataSitesTable:
        sites_tables_files = self._dump_status.jobs["sitestable"].files
        assert len(sites_tables_files) == 1
        path, dump_file = next(iter(sites_tables_files.items()))
        return WikidataSitesTable(self._dump_dir / path, dump_file)

    def meta_history_7z_dumps(self) -> Sequence[WikidataMetaHistory7zDump]:
        meta_history_7z_dump_files = self._dump_status.jobs["metahistory7zdump"].files
        return [
            WikidataMetaHistory7zDump(self._dump_dir / path, dump_file)
            for path, dump_file in meta_history_7z_dump_files.items()
        ]

    def download_all(self) -> None:
        sites_table = self.sites_table()
        meta_history_7z_dumps = self.meta_history_7z_dumps()

        paths = [sites_table.path] + [dump.path for dump in meta_history_7z_dumps]
        dump_files = [sites_table.dump_file] + [
            dump.dump_file for dump in meta_history_7z_dumps
        ]

        with tqdm(
            total=len(dump_files), dynamic_ncols=True, position=1
        ) as progress_bar_files, tqdm(
            total=sum(dump_file.size for dump_file in dump_files),
            dynamic_ncols=True,
            position=2,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar_size:
            for path, dump_file in zip(paths, dump_files):
                dump_file.download(path, self._dump_mirror)
                progress_bar_files.update(1)
                progress_bar_size.update(dump_file.size)

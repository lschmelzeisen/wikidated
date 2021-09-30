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
from typing import Mapping, MutableSequence, Sequence, Type, TypeVar

import requests
from pydantic import BaseModel as PydanticModel
from pydantic import validator
from tqdm import tqdm  # type: ignore

from wikidated.wikidata.wikidata_dump_file import WikidataDumpFile
from wikidated.wikidata.wikidata_dump_pages_meta_history import (
    WikidataDumpPagesMetaHistory,
)
from wikidated.wikidata.wikidata_dump_sites_table import WikidataDumpSitesTable

_LOGGER = getLogger(__name__)

_T_WikidataDumpFile = TypeVar("_T_WikidataDumpFile", bound=WikidataDumpFile)


class WikidataDump:
    def __init__(
        self,
        data_dir: Path,
        version: str,
        *,
        mirror: str = "https://dumps.wikimedia.org",
    ) -> None:
        self._dump_dir = data_dir / "dumpfiles"
        self._version = version
        self._mirror = mirror
        self._dump_status = _WikidataDumpStatus.load(
            self._dump_dir, self._version, self._mirror
        )

    @property
    def version(self) -> str:
        return self._version

    def download(
        self, *, sites_table: bool = True, pages_meta_history: bool = True
    ) -> None:
        dump_files: MutableSequence[WikidataDumpFile] = []

        if sites_table:
            dump_files.append(self.sites_table())

        if pages_meta_history:
            dump_files.extend(self.pages_meta_history())

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
            for dump_file in dump_files:
                dump_file.download()
                progress_bar_files.update(1)
                progress_bar_size.update(dump_file.size)

    def sites_table(self) -> WikidataDumpSitesTable:
        return self._construct_dumps(WikidataDumpSitesTable, "sitestable")[0]

    def pages_meta_history(self) -> Sequence[WikidataDumpPagesMetaHistory]:
        return self._construct_dumps(WikidataDumpPagesMetaHistory, "metahistory7zdump")

    def _construct_dumps(
        self, dump_type: Type[_T_WikidataDumpFile], dump_type_id: str
    ) -> Sequence[_T_WikidataDumpFile]:
        return [
            dump_type(
                path=self._dump_dir / path,
                url=self._mirror + dump_status_file.url,
                sha1=dump_status_file.sha1,
                size=dump_status_file.size,
            )
            for path, dump_status_file in self._dump_status.jobs[
                dump_type_id
            ].files.items()
        ]


class _WikidataDumpStatusFile(PydanticModel):
    size: int
    url: str
    md5: str
    sha1: str


class _WikidataDumpStatusJob(PydanticModel):
    status: str
    updated: datetime
    files: Mapping[str, _WikidataDumpStatusFile]

    @validator("updated", pre=True)
    def _parse_datetime(cls, value: str) -> datetime:  # noqa: N805
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


class _WikidataDumpStatus(PydanticModel):
    jobs: Mapping[str, _WikidataDumpStatusJob]
    version: str

    @classmethod
    def load(cls, dump_dir: Path, version: str, mirror: str) -> _WikidataDumpStatus:
        path = dump_dir / f"wikidatawiki-{version}-dumpstatus.json"
        if not path.exists():
            url = f"{mirror}/wikidatawiki/{version}/dumpstatus.json"
            _LOGGER.debug(f"Downloading dump status from URL '{url}'...")

            response = requests.get(url)
            response.raise_for_status()
            path.parent.mkdir(exist_ok=True, parents=True)
            with path.open("w", encoding="UTF-8") as fd:
                fd.write(json.dumps(response.json(), indent=2) + "\n")

        dump_status = _WikidataDumpStatus.parse_file(path)
        for job_name, job in dump_status.jobs.items():
            if job.status != "done":
                path.unlink()
                raise Exception(f"Job '{job_name}' is not 'done', but '{job.status}'.")

        return dump_status

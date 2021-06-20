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
from typing import Mapping, Sequence, Type, TypeVar

import requests
from nasty_utils import ColoredBraceStyleAdapter
from pydantic import BaseModel as PydanticModel
from pydantic import validator
from tqdm import tqdm

from wikidata_history_analyzer._paths import wikidata_dump_dir
from wikidata_history_analyzer.dumpfiles.wikidata_dump import WikidataDump
from wikidata_history_analyzer.dumpfiles.wikidata_meta_history_dump import (
    WikidataMetaHistoryDump,
)
from wikidata_history_analyzer.dumpfiles.wikidata_namespaces import WikidataNamespaces
from wikidata_history_analyzer.dumpfiles.wikidata_page_table import WikidataPageTable
from wikidata_history_analyzer.dumpfiles.wikidata_sites_table import WikidataSitesTable

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_T_WikidataDump = TypeVar("_T_WikidataDump", bound=WikidataDump)


class WikidataDumpManager:
    def __init__(self, data_dir: Path, dump_version: str, dump_mirror: str):
        self._dump_dir = wikidata_dump_dir(data_dir)
        self._dump_version = dump_version
        self._dump_mirror = dump_mirror
        self._dump_status = self._load_dump_status()

    def _load_dump_status(self) -> _WikidataDumpStatus:
        file = self._dump_dir / f"wikidatawiki-{self._dump_version}-dumpstatus.json"
        if not file.exists():
            url = (
                f"{self._dump_mirror}/wikidatawiki/{self._dump_version}/dumpstatus.json"
            )
            _LOGGER.debug("Downloading dumpstatus from URL '{}'...", url)
            with file.open("w", encoding="UTF-8") as fout:
                fout.write(json.dumps(requests.get(url).json(), indent=2) + "\n")

        dump_status = _WikidataDumpStatus.parse_file(file)
        for job_name, job in dump_status.jobs.items():
            if job.status != "done":
                file.unlink()
                raise Exception(f"Job '{job_name}' is not 'done', but '{job.status}'.")

        return dump_status

    def namespaces(self) -> WikidataNamespaces:
        return self._construct_dumps(WikidataNamespaces, "namespaces")[0]

    def page_table(self) -> WikidataPageTable:
        return self._construct_dumps(WikidataPageTable, "pagetable")[0]

    def sites_table(self) -> WikidataSitesTable:
        return self._construct_dumps(WikidataSitesTable, "sitestable")[0]

    def meta_history_dumps(self) -> Sequence[WikidataMetaHistoryDump]:
        return self._construct_dumps(WikidataMetaHistoryDump, "metahistory7zdump")

    def _construct_dumps(
        self, dump_type: Type[_T_WikidataDump], dump_type_id: str
    ) -> Sequence[_T_WikidataDump]:
        return [
            dump_type(
                path=self._dump_dir / path,
                url=self._dump_mirror + file.url,
                sha1=file.sha1,
                size=file.size,
            )
            for path, file in self._dump_status.jobs[dump_type_id].files.items()
        ]

    def download_all(self) -> None:
        dumps: Sequence[WikidataDump] = [
            self.namespaces(),
            self.page_table(),
            self.sites_table(),
            *self.meta_history_dumps(),
        ]

        with tqdm(
            total=len(dumps), dynamic_ncols=True, position=1
        ) as progress_bar_files, tqdm(
            total=sum(dump.size for dump in dumps),
            dynamic_ncols=True,
            position=2,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar_size:
            for dump in dumps:
                dump.download()
                progress_bar_files.update(1)
                progress_bar_size.update(dump.size)


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

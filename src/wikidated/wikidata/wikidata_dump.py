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
from datetime import date, datetime
from logging import getLogger
from pathlib import Path
from typing import Mapping, MutableSequence, Sequence, Type, TypeVar

import requests
from pydantic import BaseModel as PydanticModel
from pydantic import validator
from tqdm import tqdm  # type: ignore
from typing_extensions import Final

from wikidated._utils import RangeMap
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
        version: date,
        *,
        mirror: str = "https://dumps.wikimedia.org",
    ) -> None:
        self._dump_dir = data_dir / "dumpfiles"
        self.version: Final = version
        self.mirror: Final = mirror

        self._dump_status = _WikidataDumpStatus.load(
            self._dump_dir, self.version, self.mirror
        )

        self.sites_table: Final = self._construct_dumps(
            WikidataDumpSitesTable, "sitestable"
        )[0]
        self.pages_meta_history: Final = RangeMap[WikidataDumpPagesMetaHistory]()
        for dump_file in self._construct_dumps(
            WikidataDumpPagesMetaHistory, "metahistory7zdump"
        ):
            self.pages_meta_history[dump_file.page_ids] = dump_file

    def download(
        self, *, sites_table: bool = True, pages_meta_history: bool = True
    ) -> None:
        _LOGGER.info(
            f"Downloading Wikidata dump {self.version:%4Y%2m%2d} from '{self.mirror}'."
        )
        dump_files: MutableSequence[WikidataDumpFile] = []

        if sites_table:
            dump_files.append(self.sites_table)

        if pages_meta_history:
            dump_files.extend(self.pages_meta_history.values())

        with tqdm(
            desc=f"Wikidata dump {self.version:%4Y%2m%2d} files",
            total=len(dump_files),
            dynamic_ncols=True,
            position=1,
        ) as progress_bar_files, tqdm(
            desc=f"Wikidata dump {self.version:%4Y%2m%2d} bytes",
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

        _LOGGER.info(f"Done downloading Wikidata dump {self.version:%4Y%2m%2d}.")

    def _construct_dumps(
        self, dump_type: Type[_T_WikidataDumpFile], dump_type_id: str
    ) -> Sequence[_T_WikidataDumpFile]:
        return [
            dump_type(
                path=self._dump_dir / path,
                url=self.mirror + dump_status_file.url,
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
    def load(cls, dump_dir: Path, version: date, mirror: str) -> _WikidataDumpStatus:
        path = dump_dir / f"wikidatawiki-{version:%4Y%2m%2d}-dumpstatus.json"
        if not path.exists():
            url = f"{mirror}/wikidatawiki/{version:%4Y%2m%2d}/dumpstatus.json"
            _LOGGER.debug(f"Downloading Wikidata dump status from '{url}'.")

            response = requests.get(url)
            response.raise_for_status()
            path.parent.mkdir(exist_ok=True, parents=True)
            with path.open("w", encoding="UTF-8") as fd:
                fd.write(json.dumps(response.json(), indent=2) + "\n")

            _LOGGER.debug("Done downloading Wikidata dump status.")

        dump_status = _WikidataDumpStatus.parse_file(path)
        for job_name, job in dump_status.jobs.items():
            if job.status != "done":
                path.unlink()
                raise Exception(f"Job '{job_name}' is not 'done', but '{job.status}'.")

        return dump_status

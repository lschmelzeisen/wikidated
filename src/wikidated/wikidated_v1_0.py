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

from __future__ import annotations

from datetime import date, datetime
from errno import ENOENT
from hashlib import sha1 as calc_sha1
from logging import getLogger
from pathlib import Path
from typing import Iterator, Optional

from wikidated._utils import RangeMap, download_file_with_progressbar, hashcheck
from wikidated.wikidated_dataset import WikidatedGenericDataset
from wikidated.wikidated_entity_streams import (
    WikidatedEntityStreamsFile,
    WikidatedGenericEntityStreams,
)
from wikidated.wikidated_global_stream import (
    WikidatedGenericGlobalStream,
    WikidatedGlobalStreamFile,
)
from wikidated.wikidated_revision import WikidatedRevision
from wikidated.wikidated_sorted_entity_streams import (
    WikidatedGenericSortedEntityStreams,
    WikidatedSortedEntityStreamsFile,
)

_LOGGER = getLogger(__name__)


class _WikidatedV1_0FileDownload:  # noqa: N801
    def __init__(
        self, path: Path, darus_id: int, sha1: str, size: int, auto_download: bool
    ) -> None:
        self.path = path
        self.darus_id = darus_id
        self.sha1 = sha1
        self.size = size
        self.auto_download = auto_download
        self._downloaded = False

    def download(self) -> None:
        if self._downloaded:
            return
        if self.path.exists():
            hashcheck(self.path, calc_sha1(), self.sha1)
            _LOGGER.debug(
                f"Wikidated 1.0 file '{self.path.name}' already exists with matching "
                "sha1 checksum, skipping download."
            )
            self._downloaded = True
            return
        if not self.auto_download:
            raise FileNotFoundError(
                ENOENT,
                f"File '{self.path}' does not exist and auto-downloading is disabled.",
                str(self.path),
            )

        url = f"https://darus.uni-stuttgart.de/api/access/datafile/{self.darus_id}"
        _LOGGER.debug(
            f"Downloading Wikidated 1.0 file '{self.path.name}' from '{url}'."
        )
        self.path.parent.mkdir(exist_ok=True, parents=True)
        path_tmp = self.path.parent / ("tmp." + self.path.name)
        download_file_with_progressbar(url, path_tmp, description=self.path.name)
        hashcheck(path_tmp, calc_sha1(), self.sha1)
        path_tmp.rename(self.path)
        _LOGGER.debug(f"Done downloading Wikidated 1.0 dump file '{self.path.name}'.")
        self._downloaded = True


class WikidatedV1_0EntityStreamsFile(WikidatedEntityStreamsFile):  # noqa: N801
    def __init__(
        self,
        archive_path: Path,
        page_ids: range,
        darus_id: int,
        sha1: str,
        size: int,
        auto_download: bool,
    ) -> None:
        super().__init__(archive_path, page_ids)
        self._file_download = _WikidatedV1_0FileDownload(
            archive_path, darus_id, sha1, size, auto_download
        )

    def download(self) -> None:
        self._file_download.download()

    def iter_revisions(
        self,
        page_id: Optional[int] = None,
        *,
        min_revision_id: Optional[int] = None,
        max_revision_id: Optional[int] = None,
        min_timestamp: Optional[datetime] = None,
        max_timestamp: Optional[datetime] = None,
    ) -> Iterator[WikidatedRevision]:
        self.download()
        return super().iter_revisions(
            page_id=page_id,
            min_revision_id=min_revision_id,
            max_revision_id=max_revision_id,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
        )

    def iter_page_ids(self) -> Iterator[int]:
        self.download()
        return super().iter_page_ids()


class WikidatedV1_0SortedEntityStreamsFile(  # noqa: N801
    WikidatedSortedEntityStreamsFile
):
    def __init__(
        self,
        archive_path: Path,
        page_ids: range,
        darus_id: int,
        sha1: str,
        size: int,
        auto_download: bool,
    ) -> None:
        super().__init__(archive_path, page_ids)
        self._file_download = _WikidatedV1_0FileDownload(
            archive_path, darus_id, sha1, size, auto_download
        )

    def download(self) -> None:
        self._file_download.download()

    def iter_revisions(
        self,
        *,
        min_revision_id: Optional[int] = None,
        max_revision_id: Optional[int] = None,
        min_timestamp: Optional[datetime] = None,
        max_timestamp: Optional[datetime] = None,
    ) -> Iterator[WikidatedRevision]:
        self.download()
        return super().iter_revisions(
            min_revision_id=min_revision_id,
            max_revision_id=max_revision_id,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
        )


class WikidatedV1_0GlobalStreamFile(WikidatedGlobalStreamFile):  # noqa: N801
    def __init__(
        self,
        archive_path: Path,
        month: date,
        revision_ids: range,
        darus_id: int,
        sha1: str,
        size: int,
        auto_download: bool,
    ) -> None:
        super().__init__(archive_path, month, revision_ids)
        self._file_download = _WikidatedV1_0FileDownload(
            archive_path, darus_id, sha1, size, auto_download
        )

    def download(self) -> None:
        self._file_download.download()

    def iter_revisions(
        self,
        *,
        min_revision_id: Optional[int] = None,
        max_revision_id: Optional[int] = None,
        min_timestamp: Optional[datetime] = None,
        max_timestamp: Optional[datetime] = None,
    ) -> Iterator[WikidatedRevision]:
        self.download()
        return super().iter_revisions(
            min_revision_id=min_revision_id,
            max_revision_id=max_revision_id,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
        )


class WikidatedV1_0EntityStreams(  # noqa: N801
    WikidatedGenericEntityStreams[WikidatedV1_0EntityStreamsFile]
):
    def download(self) -> None:
        for file in self:
            file.download()

    @classmethod
    def load_v1_0(
        cls, dataset_dir: Path, auto_download: bool
    ) -> WikidatedV1_0EntityStreams:
        from wikidated._wikidated_v1_0_files import wikidated_v1_0_entity_streams_files

        _LOGGER.debug(f"Loading entity streams for dataset {dataset_dir.name}.")
        files_by_page_ids = RangeMap[WikidatedV1_0EntityStreamsFile]()
        for file in wikidated_v1_0_entity_streams_files(
            dataset_dir, auto_download=auto_download
        ):
            files_by_page_ids[file.page_ids] = file
        _LOGGER.debug(f"Done loading entity streams for dataset {dataset_dir.name}.")
        return WikidatedV1_0EntityStreams(files_by_page_ids)


class WikidatedV1_0SortedEntityStreams(  # noqa: N801
    WikidatedGenericSortedEntityStreams[WikidatedV1_0SortedEntityStreamsFile]
):
    def download(self) -> None:
        for file in self:
            file.download()

    @classmethod
    def load_v1_0(
        cls, dataset_dir: Path, auto_download: bool
    ) -> WikidatedV1_0SortedEntityStreams:
        from wikidated._wikidated_v1_0_files import (
            wikidated_v1_0_sorted_entity_streams_files,
        )

        _LOGGER.debug(f"Loading sorted entity streams for dataset {dataset_dir.name}.")
        files_by_page_ids = RangeMap[WikidatedV1_0SortedEntityStreamsFile]()
        for file in wikidated_v1_0_sorted_entity_streams_files(
            dataset_dir, auto_download=auto_download
        ):
            files_by_page_ids[file.page_ids] = file
        _LOGGER.debug(
            f"Done loading sorted entity streams for dataset {dataset_dir.name}."
        )
        return WikidatedV1_0SortedEntityStreams(files_by_page_ids)


class WikidatedV1_0GlobalStream(  # noqa: N801
    WikidatedGenericGlobalStream[WikidatedV1_0GlobalStreamFile]
):
    def download(self) -> None:
        for file in self:
            file.download()

    @classmethod
    def load_v1_0(
        cls, dataset_dir: Path, auto_download: bool
    ) -> WikidatedV1_0GlobalStream:
        from wikidated._wikidated_v1_0_files import wikidated_v1_0_global_stream_files

        _LOGGER.debug(f"Loading global stream for dataset {dataset_dir.name}.")
        files_by_months = RangeMap[WikidatedV1_0GlobalStreamFile]()
        files_by_revision_ids = RangeMap[WikidatedV1_0GlobalStreamFile]()
        for file in wikidated_v1_0_global_stream_files(
            dataset_dir, auto_download=auto_download
        ):
            files_by_months[file.months] = file
            files_by_revision_ids[file.revision_ids] = file
        _LOGGER.debug(f"Done loading global stream for dataset {dataset_dir.name}.")
        return WikidatedV1_0GlobalStream(files_by_months, files_by_revision_ids)


class WikidatedV1_0Dataset(  # noqa: N801
    WikidatedGenericDataset[
        WikidatedV1_0EntityStreams,
        WikidatedV1_0SortedEntityStreams,
        WikidatedV1_0GlobalStream,
    ]
):
    NUM_PAGES = 96_646_606
    NUM_REVISIONS = 1_411_008_075

    def download(self) -> None:
        self.entity_streams.download()
        self.global_stream.download()

    @classmethod
    def load_v1_0(cls, dataset_dir: Path, auto_download: bool) -> WikidatedV1_0Dataset:
        _LOGGER.debug(f"Loading dataset {dataset_dir.name}.")
        entity_streams = WikidatedV1_0EntityStreams.load_v1_0(
            dataset_dir, auto_download=auto_download
        )
        sorted_entity_streams = WikidatedV1_0SortedEntityStreams.load_v1_0(
            dataset_dir, auto_download=auto_download
        )
        global_stream = WikidatedV1_0GlobalStream.load_v1_0(
            dataset_dir, auto_download=auto_download
        )
        _LOGGER.debug(f"Done loading dataset {dataset_dir.name}")
        return WikidatedV1_0Dataset(
            dataset_dir=dataset_dir,
            dump_version=date(2021, 6, 1),
            entity_streams=entity_streams,
            sorted_entity_streams=sorted_entity_streams,
            global_stream=global_stream,
        )

    @property
    def dataset_version(self) -> str:
        return "wikidated-1.0"

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

import re
from datetime import date, datetime
from itertools import chain
from pathlib import Path
from typing import Iterator, Mapping, MutableMapping, Optional, Tuple
from xml.sax.saxutils import unescape

from pydantic import BaseModel as PydanticModel
from tqdm import tqdm  # type: ignore

from wikidated._utils import SevenZipArchive
from wikidated.wikidata.wikidata_dump_file import WikidataDumpFile
from wikidated.wikidata.wikidata_revision_base import (
    WikidataEntityMeta,
    WikidataRevisionBase,
    WikidataRevisionMeta,
)


class WikidataSiteInfo(PydanticModel):
    site_name: str
    db_name: str
    base: str
    generator: str
    case: str
    namespaces: Mapping[int, str]


class WikidataRawRevision(WikidataRevisionBase):
    text: Optional[str]


class WikidataDumpPagesMetaHistory(WikidataDumpFile):
    # Does not use an actual XML library for parsing the dumps content as we can make
    # some fairly strong assumptions about the XML used in the dump. Mainly we have that
    # each element starts/ends on it's own line and that we know the exact order of
    # elements occurring within each other. As such the code will need to be manually
    # updated to changes in the dump format but on the other hand is much faster.

    def __init__(self, *, path: Path, url: str, sha1: str, size: int) -> None:
        super().__init__(path=path, url=url, sha1=sha1, size=size)

        match = re.match(
            r"^wikidatawiki-(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})-pages-meta-"
            r"history\d+.xml-p(?P<min_page_id>\d+)p(?P<max_page_id>\d+).7z$",
            self._path.name,
        )
        if not match:
            raise Exception(
                f"File '{self._path.name}' is not a Wikidata dump pages-meta-history "
                f"file (based on file name)."
            )
        self._date = date(int(match["year"]), int(match["month"]), int(match["day"]))
        self._page_id_range = range(
            int(match["min_page_id"]), int(match["max_page_id"]) + 1
        )

    @property
    def date(self) -> date:
        return self._date

    @property
    def page_id_range(self) -> range:
        return self._page_id_range

    def site_info(self) -> WikidataSiteInfo:
        assert self._path.exists()
        with SevenZipArchive(self._path).read() as fd:
            lines = iter(fd)
            self._assert_opening_tag(next(lines), "mediawiki")
            return self._process_site_info(lines)

    @classmethod
    def _process_site_info(cls, lines: Iterator[str]) -> WikidataSiteInfo:
        cls._assert_opening_tag(next(lines), "siteinfo")
        site_name = cls._extract_value(next(lines), "sitename")
        db_name = cls._extract_value(next(lines), "dbname")
        base = cls._extract_value(next(lines), "base")
        generator = cls._extract_value(next(lines), "generator")
        case = cls._extract_value(next(lines), "case")
        namespaces: MutableMapping[int, str] = {}

        cls._assert_opening_tag(next(lines), "namespaces")
        for line in lines:
            if cls._is_closing_tag(line, "namespaces"):
                break
            cls._assert_opening_tag(line, "namespace")
            key_index = line.index('key="') + len('key="')
            namespace_key = int(line[key_index : line.index('"', key_index)])
            if line.rstrip().endswith("/>"):  # <namespace key="0" />
                namespaces[namespace_key] = ""
            else:
                namespaces[namespace_key] = cls._extract_value(line, "namespace")
        cls._assert_closing_tag(next(lines), "siteinfo")

        return WikidataSiteInfo.construct(
            site_name=site_name,
            db_name=db_name,
            base=base,
            generator=generator,
            case=case,
            namespaces=namespaces,
        )

    def __iter__(self) -> Iterator[WikidataRawRevision]:
        return self.iter_revisions()

    def iter_revisions(
        self, *, display_progress_bar: bool = True
    ) -> Iterator[WikidataRawRevision]:
        assert self._path.exists()

        progress_bar: Optional[tqdm] = (
            tqdm(
                desc=self._path.name, total=len(self._page_id_range), dynamic_ncols=True
            )
            if display_progress_bar
            else None
        )

        with SevenZipArchive(self._path).read() as fd:
            lines = iter(fd)
            self._assert_opening_tag(next(lines), "mediawiki")
            self._assert_opening_tag(next(lines), "siteinfo")

            for line in lines:
                if self._is_closing_tag(line, "siteinfo"):
                    break

            for line in lines:
                if self._is_closing_tag(line, "mediawiki"):
                    break
                yield from self._process_page(chain((line,), lines))
                if progress_bar:
                    progress_bar.update(1)

            if progress_bar:
                progress_bar.total = progress_bar.n
                progress_bar.refresh()

            try:
                line = next(lines)
                raise Exception(f"Expected EOF, instead line was: '{line}'.")
            except StopIteration:
                pass

    @classmethod
    def _process_page(cls, lines: Iterator[str]) -> Iterator[WikidataRawRevision]:
        cls._assert_opening_tag(next(lines), "page")
        entity_id = cls._unescape_xml(cls._extract_value(next(lines), "title"))
        namespace = int(cls._extract_value(next(lines), "ns"))
        page_id = int(cls._extract_value(next(lines), "id"))

        redirect: Optional[str] = None
        line = next(lines)
        if cls._is_opening_tag(line, "redirect"):
            title_index = line.index('title="') + len('title="')
            redirect = line[title_index : line.index('"', title_index)]
        else:
            lines = chain((line,), lines)

        entity = WikidataEntityMeta(
            entity_id=entity_id, page_id=page_id, namespace=namespace, redirect=redirect
        )

        for line in lines:
            if cls._is_closing_tag(line, "page"):
                break
            revision, text = cls._process_revision(chain((line,), lines))
            yield WikidataRawRevision(entity=entity, revision=revision, text=text)

    @classmethod
    def _process_revision(
        cls, lines: Iterator[str]
    ) -> Tuple[WikidataRevisionMeta, Optional[str]]:
        cls._assert_opening_tag(next(lines), "revision")
        revision_id = int(cls._extract_value(next(lines), "id"))

        parent_revision_id: Optional[int] = None
        line = next(lines)
        if cls._is_opening_tag(line, "parentid"):
            parent_revision_id = int(cls._extract_value(line, "parentid"))
        else:
            lines = chain((line,), lines)

        timestamp = datetime.strptime(
            cls._extract_value(next(lines), "timestamp"), "%Y-%m-%dT%H:%M:%S%z"
        )

        contributor: Optional[str] = None
        contributor_id: Optional[int] = None
        line = next(lines)
        cls._assert_opening_tag(line, "contributor")
        if 'deleted="deleted"' not in line:  # <contributor deleted="deleted" />
            line = next(lines)
            if cls._is_opening_tag(line, "ip"):
                contributor = cls._extract_value(line, "ip")
            else:
                contributor = cls._extract_value(line, "username")
                contributor_id = int(cls._extract_value(next(lines), "id"))
            cls._assert_closing_tag(next(lines), "contributor")

        is_minor = False
        line = next(lines)
        if cls._is_opening_tag(line, "minor"):
            is_minor = True
        else:
            lines = chain((line,), lines)

        comment: Optional[str] = None
        line = next(lines)
        if cls._is_opening_tag(line, "comment"):
            if 'deleted="deleted"' not in line:
                comment = cls._extract_value_multiline(chain((line,), lines), "comment")
                if comment:
                    comment = cls._unescape_xml(comment)
        else:
            lines = chain((line,), lines)

        wikibase_model = cls._extract_value(next(lines), "model")
        wikibase_format = cls._extract_value(next(lines), "format")

        text = cls._extract_value_multiline(lines, "text")
        if text:
            text = cls._unescape_xml(text)

        sha1 = None
        line = next(lines)
        cls._assert_opening_tag(line, "sha1")
        if not line.rstrip().endswith("/>"):
            sha1 = cls._extract_value(line, "sha1")

        cls._assert_closing_tag(next(lines), "revision")

        return (
            WikidataRevisionMeta(
                revision_id=revision_id,
                parent_revision_id=parent_revision_id,
                timestamp=timestamp,
                contributor=contributor,
                contributor_id=contributor_id,
                is_minor=is_minor,
                comment=comment,
                wikibase_model=wikibase_model,
                wikibase_format=wikibase_format,
                sha1=sha1,
            ),
            text,
        )

    @classmethod
    def _is_opening_tag(cls, line: str, element: str) -> bool:
        return line.lstrip().startswith("<" + element)

    @classmethod
    def _assert_opening_tag(cls, line: str, element: str) -> None:
        if not cls._is_opening_tag(line, element):
            raise Exception(f"Expected <{element}>, instead line was: '{line}'.")

    @classmethod
    def _is_closing_tag(cls, line: str, element: str) -> bool:
        return line.rstrip().endswith("</" + element + ">")

    @classmethod
    def _assert_closing_tag(cls, line: str, element: str) -> None:
        if not cls._is_closing_tag(line, element):
            raise Exception(f"Expected </{element}>, instead line was: '{line}'.")

    @classmethod
    def _extract_value(cls, line: str, element: str) -> str:
        cls._assert_opening_tag(line, element)
        cls._assert_closing_tag(line, element)
        return line[line.index(">") + 1 : line.rindex("</")]

    @classmethod
    def _extract_value_multiline(
        cls, lines: Iterator[str], element: str
    ) -> Optional[str]:
        line = next(lines)
        cls._assert_opening_tag(line, element)
        closing_tag = "</" + element + ">"

        stripped_line = line.rstrip()
        if stripped_line.endswith("/>"):  # <text bytes="0" />
            return None
        elif stripped_line.endswith(closing_tag):
            return cls._extract_value(line, element)

        value = [line[line.index(">") + 1 :]]
        for line in lines:
            if cls._is_closing_tag(line, element):
                value.append(line[: line.rindex("</")])
                break
            value.append(line)
        return "".join(value)

    @classmethod
    def _unescape_xml(cls, value: str) -> str:
        return unescape(value, entities={"&quot;": '"'})

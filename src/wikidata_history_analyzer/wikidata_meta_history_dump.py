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

import re
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime
from itertools import chain
from logging import getLogger
from pathlib import Path
from typing import Iterator, Mapping, MutableMapping, Optional, TextIO, cast
from xml.sax.saxutils import unescape

from nasty_utils import ColoredBraceStyleAdapter
from tqdm import tqdm

from wikidata_history_analyzer._utils import p7z_open
from wikidata_history_analyzer.wikidata_dump_meta import WikidataDumpFile
from wikidata_history_analyzer.wikidata_revision import WikidataRevision

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


@dataclass
class WikidataSiteInfo:
    site_name: str
    db_name: str
    base: str
    generator: str
    case: str
    namespaces: Mapping[int, str]


class WikidataDumpInvalidFileException(Exception):
    def __init__(self, file: Path):
        super().__init__(
            f"File '{file.name}' is not a Wikidata pages-meta-history dump file (based "
            f"on file name). Full path: '{file}'."
        )


class WikidataMetaHistoryDump:
    # Does not use an actual XML library for parsing the dumps content as we can make
    # some fairly strong assumptions about the XML used in the dump. Mainly we have that
    # each element starts/ends on it's own line and that we know the exact order of
    # elements occurring within each other. As such the code will need to be manually
    # updated to changes in the dump format but on the other hand is much faster.

    def __init__(self, path: Path, dump_file: WikidataDumpFile):
        self.path = path
        self.dump_file = dump_file

        match = re.match(
            r"^wikidatawiki-(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})-pages-meta-"
            r"history\d+.xml-p(?P<min_page_id>\d+)p(?P<max_page_id>\d+).7z$",
            self.path.name,
        )
        if not match:
            raise WikidataDumpInvalidFileException(self.path)
        self.date_ = date(int(match["year"]), int(match["month"]), int(match["day"]))
        self.min_page_id = match["min_page_id"]
        self.max_page_id = match["max_page_id"]

    def site_info(self) -> WikidataSiteInfo:
        assert self.path.exists()

        with p7z_open(self.path, encoding="UTF-8") as fin:
            lines = iter(cast(TextIO, fin))
            self._assert_opening_tag(next(lines), "mediawiki")
            return self._process_site_info(lines)

    def iter_revisions(
        self, *, display_progress_bar: bool = True
    ) -> Iterator[WikidataRevision]:
        assert self.path.exists()

        num_pages = int(self.max_page_id) - int(self.min_page_id) + 1
        progress_bar: Optional[tqdm[None]] = (
            tqdm(desc=self.path.name, total=num_pages, dynamic_ncols=True)
            if display_progress_bar
            else None
        )

        with p7z_open(
            self.path, encoding="UTF-8"
        ) as fin, progress_bar or nullcontext():
            lines = iter(cast(TextIO, fin))
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

            try:
                line = next(lines)
                raise Exception(f"Expected EOF, instead line was: '{line}'.")
            except StopIteration:
                pass

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
        # Does not work with nested tags.
        cls._assert_opening_tag(line, element)
        cls._assert_closing_tag(line, element)
        return line[line.index(">") + 1 : line.index("</")]

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
                value.append(line[: line.index("</")])
                break
            value.append(line)
        return "".join(value)

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

        return WikidataSiteInfo(
            site_name=site_name,
            db_name=db_name,
            base=base,
            generator=generator,
            case=case,
            namespaces=namespaces,
        )

    @classmethod
    def _process_page(cls, lines: Iterator[str]) -> Iterator[WikidataRevision]:
        cls._assert_opening_tag(next(lines), "page")
        prefixed_title = cls._unescape_xml(cls._extract_value(next(lines), "title"))
        namespace = int(cls._extract_value(next(lines), "ns"))
        page_id = cls._extract_value(next(lines), "id")

        redirect: Optional[str] = None
        line = next(lines)
        if cls._is_opening_tag(line, "redirect"):
            title_index = line.index('title="') + len('title="')
            redirect = line[title_index : line.index('"', title_index)]
        else:
            lines = chain((line,), lines)

        for line in lines:
            if cls._is_closing_tag(line, "page"):
                break
            yield cls._process_revision(
                chain((line,), lines),
                prefixed_title=prefixed_title,
                namespace=namespace,
                page_id=page_id,
                redirect=redirect,
            )

    @classmethod
    def _process_revision(
        cls,
        lines: Iterator[str],
        *,
        prefixed_title: str,
        namespace: int,
        page_id: str,
        redirect: Optional[str],
    ) -> WikidataRevision:
        cls._assert_opening_tag(next(lines), "revision")
        revision_id = cls._extract_value(next(lines), "id")

        parent_revision_id: Optional[str] = None
        line = next(lines)
        if cls._is_opening_tag(line, "parentid"):
            parent_revision_id = cls._extract_value(line, "parentid")
        else:
            lines = chain((line,), lines)

        timestamp = datetime.strptime(
            cls._extract_value(next(lines), "timestamp"), "%Y-%m-%dT%H:%M:%S%z"
        )

        contributor: Optional[str] = None
        contributor_id: Optional[str] = None
        line = next(lines)
        cls._assert_opening_tag(line, "contributor")
        if 'deleted="deleted"' not in line:  # <contributor deleted="deleted" />
            line = next(lines)
            if cls._is_opening_tag(line, "ip"):
                contributor = cls._extract_value(line, "ip")
            else:
                contributor = cls._extract_value(line, "username")
                contributor_id = cls._extract_value(next(lines), "id")
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

        content_model = cls._extract_value(next(lines), "model")
        format_ = cls._extract_value(next(lines), "format")

        text = cls._extract_value_multiline(lines, "text")
        if text:
            text = cls._unescape_xml(text)

        sha1 = None
        line = next(lines)
        cls._assert_opening_tag(line, "sha1")
        if not line.rstrip().endswith("/>"):
            sha1 = cls._extract_value(line, "sha1")

        cls._assert_closing_tag(next(lines), "revision")

        return WikidataRevision(
            prefixed_title=prefixed_title,
            namespace=namespace,
            page_id=page_id,
            redirect=redirect,
            revision_id=revision_id,
            parent_revision_id=parent_revision_id,
            timestamp=timestamp,
            contributor=contributor,
            contributor_id=contributor_id,
            is_minor=is_minor,
            comment=comment,
            content_model=content_model,
            format=format_,
            text=text,
            sha1=sha1,
        )

    @classmethod
    def _unescape_xml(cls, value: str) -> str:
        return unescape(value, entities={"&quot;": '"'})

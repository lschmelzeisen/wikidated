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

from dataclasses import dataclass
from itertools import chain
from logging import getLogger
from pathlib import Path
from subprocess import PIPE, Popen, TimeoutExpired
from typing import Iterator, Mapping, MutableMapping, Optional, TextIO, cast
from xml.sax.saxutils import unescape

from jpype import JClass, JLong, shutdownJVM, startJVM  # type: ignore
from nasty_utils import ColoredBraceStyleAdapter

from kg_evolve.settings_ import KgEvolveSettings

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


@dataclass
class SiteInfo:
    site_name: str
    db_name: str
    base: str
    generator: str
    case: str
    namespaces: Mapping[int, str]


@dataclass
class Revision:
    prefixed_title: str
    namespace: int
    page_id: str
    redirect: Optional[str]
    revision_id: str
    parent_revision_id: Optional[str]
    timestamp: str
    contributor: Optional[str]
    contributor_id: Optional[str]
    is_minor: bool
    comment: Optional[str]
    model: str
    format: str
    text: str
    sha1: Optional[str]

    def to_java(self) -> JClass:
        return JClass("wikidatadumpprocessor.FullRevision")(
            self.prefixed_title,
            self.namespace,
            int(self.page_id),
            JOptional.ofNullable(self.redirect),
            int(self.revision_id),
            (
                JOptional.of(JLong(int(self.parent_revision_id)))
                if self.parent_revision_id
                else JOptional.empty()
            ),
            self.timestamp,
            JOptional.ofNullable(self.contributor),
            (
                JOptional.of(JLong(int(self.contributor_id)))
                if self.contributor_id
                else JOptional.empty()
            ),
            self.is_minor,
            JOptional.ofNullable(self.comment),
            self.model,
            self.format,
            self.text,
            JOptional.ofNullable(self.sha1),
        )


settings = KgEvolveSettings.find_and_load_from_settings_file()
settings.setup_logging()


startJVM(classpath=["jars/*"])

JOptional = JClass("java.util.Optional")
JEntityTimerProcessor = JClass("org.wikidata.wdtk.dumpfiles.EntityTimerProcessor")
JEntityDocumentProcessorBroker = JClass(
    "org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessorBroker"
)
JWikibaseRevisionProcessor = JClass(
    "org.wikidata.wdtk.dumpfiles.WikibaseRevisionProcessor"
)
JExampleHelpers = JClass("examples.ExampleHelpers")

JExampleHelpers.configureLogging()

entity_timer_processor = JEntityTimerProcessor(0)
entity_document_processor = JEntityDocumentProcessorBroker()
entity_document_processor.registerEntityDocumentProcessor(entity_timer_processor)
wikibase_revision_processor = JWikibaseRevisionProcessor(
    entity_document_processor, "http://www.wikidata.org/"
)

entity_timer_processor.open()


def process_dump_file(dump_file: Path) -> None:
    seven_zip = Popen(
        ["7z", "x", "-so", str(dump_file)],
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        encoding="UTF-8",
    )

    dump_contents = iter(cast(TextIO, seven_zip.stdout))

    _assert_opening_tag(next(dump_contents), "mediawiki")
    _assert_opening_tag(next(dump_contents), "siteinfo")
    site_info = _process_site_info(dump_contents)

    wikibase_revision_processor.startRevisionProcessing(
        site_info.site_name, site_info.base, site_info.namespaces
    )
    for line in dump_contents:
        if _is_closing_tag(line, "mediawiki"):
            break
        _assert_opening_tag(line, "page")
        _process_page(dump_contents)
    wikibase_revision_processor.finishRevisionProcessing()

    try:
        line = next(dump_contents)
        raise Exception(f"Expected EOF, instead line was: '{line}'.")
    except StopIteration:
        pass

    # TODO: I have no idea how to check for errors in 7z. Calling
    #  seven_zip.stderr.read() will block the thread indefinitely, if there is no error.
    #  It seems the correct way to do this, would be to create the process using
    #  asyncio, see https://stackoverflow.com/a/34114767/211404
    seven_zip.terminate()
    try:
        seven_zip.wait(timeout=1)
    except TimeoutExpired as e:
        _LOGGER.exception("7z Process did not terminate, killing...", e)
        seven_zip.kill()
    if seven_zip.returncode != 0:
        raise Exception(
            "7z exited with non-zero return code: {}\n{}".format(
                seven_zip.returncode, cast(TextIO, seven_zip.stderr).read()
            )
        )


def _is_opening_tag(line: str, element: str) -> bool:
    return line.lstrip().startswith("<" + element)


def _assert_opening_tag(line: str, element: str) -> None:
    if not _is_opening_tag(line, element):
        raise Exception(f"Expected <{element}>, instead line was: '{line}'.")


def _is_closing_tag(line: str, element: str) -> bool:
    return line.rstrip().endswith("</" + element + ">")


def _assert_closing_tag(line: str, element: str) -> None:
    if not _is_closing_tag(line, element):
        raise Exception(f"Expected </{element}>, instead line was: '{line}'.")


def _extract_value(line: str, element: str) -> str:
    # Does not work with nested tags.
    _assert_opening_tag(line, element)
    _assert_closing_tag(line, element)
    return line[line.index(">") + 1 : line.index("</")]


def _extract_value_multiline(
    lines: Iterator[str], element: str, *, line: Optional[str] = None
) -> str:
    if line is None:
        line = next(lines)
    _assert_opening_tag(line, element)
    closing_tag = "</" + element + ">"

    stripped_line = line.rstrip()
    if stripped_line.endswith("/>"):  # <text bytes="0" />
        return ""
    elif stripped_line.endswith(closing_tag):
        return _extract_value(line, element)

    value = [line[line.index(">") + 1 :]]
    for line in lines:
        if _is_closing_tag(line, element):
            value.append(line[0 : line.index("</")])
            break
        value.append(line)
    return "".join(value)


def _process_site_info(lines: Iterator[str]) -> SiteInfo:
    site_name = _extract_value(next(lines), "sitename")
    db_name = _extract_value(next(lines), "dbname")
    base = _extract_value(next(lines), "base")
    generator = _extract_value(next(lines), "generator")
    case = _extract_value(next(lines), "case")
    namespaces: MutableMapping[int, str] = {}

    _assert_opening_tag(next(lines), "namespaces")
    for line in lines:
        if _is_closing_tag(line, "namespaces"):
            break
        _assert_opening_tag(line, "namespace")
        key_index = line.index('key="') + len('key="')
        namespace_key = int(line[key_index : line.index('"', key_index)])
        if line.rstrip().endswith("/>"):
            namespaces[namespace_key] = ""
        else:
            namespaces[namespace_key] = _extract_value(line, "namespace")
    _assert_closing_tag(next(lines), "siteinfo")

    return SiteInfo(
        site_name=site_name,
        db_name=db_name,
        base=base,
        generator=generator,
        case=case,
        namespaces=namespaces,
    )


def _process_page(lines: Iterator[str]) -> None:
    prefixed_title = unescape(
        _extract_value(next(lines), "title"), entities={"&quot;": '"'}
    )
    namespace = int(_extract_value(next(lines), "ns"))
    page_id = _extract_value(next(lines), "id")

    line = next(lines)
    redirect: Optional[str] = None
    if _is_opening_tag(line, "redirect"):
        title_index = line.index('title="') + len('title="')
        redirect = line[title_index : line.index('"', title_index)]
        line = next(lines)

    for line in chain((line,), lines):
        if _is_closing_tag(line, "page"):
            break
        _assert_opening_tag(line, "revision")
        _process_revision(
            lines,
            prefixed_title=prefixed_title,
            namespace=namespace,
            page_id=page_id,
            redirect=redirect,
        )


def _process_revision(
    lines: Iterator[str],
    *,
    prefixed_title: str,
    namespace: int,
    page_id: str,
    redirect: Optional[str],
) -> None:
    revision_id = _extract_value(next(lines), "id")

    line = next(lines)
    parent_revision_id: Optional[str] = None
    if _is_opening_tag(line, "parentid"):
        parent_revision_id = _extract_value(line, "parentid")
        line = next(lines)

    timestamp = _extract_value(line, "timestamp")

    line = next(lines)
    contributor: Optional[str] = None
    contributor_id: Optional[str] = None
    _assert_opening_tag(line, "contributor")
    if 'deleted="deleted"' not in line:
        line = next(lines)
        if _is_opening_tag(line, "ip"):
            contributor = _extract_value(line, "ip")
        else:
            contributor = _extract_value(line, "username")
            contributor_id = _extract_value(next(lines), "id")
        _assert_closing_tag(next(lines), "contributor")

    line = next(lines)
    is_minor = False
    if _is_opening_tag(line, "minor"):
        is_minor = True
        line = next(lines)

    comment: Optional[str] = None
    if _is_opening_tag(line, "comment"):
        if 'deleted="deleted"' not in line:
            comment = _extract_value_multiline(lines, "comment", line=line)
        line = next(lines)

    model = _extract_value(line, "model")
    format_ = _extract_value(next(lines), "format")

    text = unescape(_extract_value_multiline(lines, "text"), entities={"&quot;": '"'})

    line = next(lines)
    sha1 = None
    _assert_opening_tag(line, "sha1")
    if not line.rstrip().endswith("sha1"):
        sha1 = _extract_value(line, "sha1")

    _assert_closing_tag(next(lines), "revision")

    revision = Revision(
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
        model=model,
        format=format_,
        text=text,
        sha1=sha1,
    )

    wikibase_revision_processor.processRevision(revision.to_java())


process_dump_file(
    Path(
        "data/dumpfiles/"
        "wikidatawiki-20210401-pages-meta-history25.xml-p67174382p67502430.7z"
    )
)

entity_timer_processor.close()

shutdownJVM()

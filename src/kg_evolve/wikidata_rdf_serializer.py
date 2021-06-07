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

from collections import Counter
from logging import getLogger
from pathlib import Path
from typing import NamedTuple, Optional, Sequence

from jpype import JClass, JException, JObject, shutdownJVM, startJVM  # type: ignore
from nasty_utils import ColoredBraceStyleAdapter

from kg_evolve.java_logging_bride import setup_java_logging_bridge
from kg_evolve.settings_ import KgEvolveSettings
from kg_evolve.wikidata_dump import WikidataDump, WikidataDumpRevision

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class RdfTriple(NamedTuple):
    head: str
    relation: str
    tail: str


class WikidataRdfSerializationException(Exception):
    def __init__(
        self,
        reason: str,
        revision: WikidataDumpRevision,
        exception: Optional[Exception] = None,
    ) -> None:
        self.reason = reason
        self.revision = revision
        self.exception = exception

    def __str__(self):
        return (
            f"{self.reason} ({self.revision.prefixed_title}, "
            f"page: {self.revision.page_id}, revision: {self.revision.revision_id})"
        )


class WikidataRdfSerializer:
    # TODO: document that this is basically a mix of RdfSerializer, RdfWriter,
    #  RdfConverter and AbstractRdfConverter.
    # TODO: document that RdfConverter basically only adds the "TASK filtering" on top
    #  of AbstractRdfConverter.

    # Prefixes taken from
    # https://www.mediawiki.org/w/index.php?title=Wikibase/Indexing/RDF_Dump_Format&oldid=4471307#Full_list_of_prefixes
    # but sorted so that the longer URLs come first to enable one-pass prefixing.
    PREFIXES = {
        "cc": "http://creativecommons.org/ns#",
        "dct": "http://purl.org/dc/terms/",
        "schema": "http://schema.org/",
        "wikibase": "http://wikiba.se/ontology#",
        "hint": "http://www.bigdata.com/queryHints#",
        "bd": "http://www.bigdata.com/rdf#",
        "geo": "http://www.opengis.net/ont/geosparql#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "owl": "http://www.w3.org/2002/07/owl#",
        "skos": "http://www.w3.org/2004/02/skos/core#",
        "ontolex": "http://www.w3.org/ns/lemon/ontolex#",
        "prov": "http://www.w3.org/ns/prov#",
        "wds": "http://www.wikidata.org/entity/statement/",
        "wd": "http://www.wikidata.org/entity/",
        "wdtn": "http://www.wikidata.org/prop/direct-normalized/",
        "wdt": "http://www.wikidata.org/prop/direct/",
        "wdno": "http://www.wikidata.org/prop/novalue/",
        "pqn": "http://www.wikidata.org/prop/qualifier/value-normalized/",
        "pqv": "http://www.wikidata.org/prop/qualifier/value/",
        "pq": "http://www.wikidata.org/prop/qualifier/",
        "prn": "http://www.wikidata.org/prop/reference/value-normalized/",
        "prv": "http://www.wikidata.org/prop/reference/value/",
        "pr": "http://www.wikidata.org/prop/reference/",
        "psn": "http://www.wikidata.org/prop/statement/value-normalized/",
        "psv": "http://www.wikidata.org/prop/statement/value/",
        "ps": "http://www.wikidata.org/prop/statement/",
        "p": "http://www.wikidata.org/prop/",
        "wdref": "http://www.wikidata.org/reference/",
        "wdv": "http://www.wikidata.org/value/",
        "wdata": "http://www.wikidata.org/wiki/Special:EntityData/",
    }

    def __init__(self, sites_file: Path) -> None:
        self._sites = self._get_sites(sites_file)
        self._property_register = self._get_property_register()
        self._json_deserializer = self._get_json_deserializer()

        JMwRevision = JClass("org.wikidata.wdtk.dumpfiles.MwRevision")  # noqa: N806
        self._model_item = str(JMwRevision.MODEL_WIKIBASE_ITEM)
        self._model_property = str(JMwRevision.MODEL_WIKIBASE_PROPERTY)

        self._format_ntriples = JClass("org.eclipse.rdf4j.rio.RDFFormat").NTRIPLES

        # Keep references to Java classes here, so they do not have to be looked up
        # when processing each revision individually.
        self._JRdfConverter = JClass("org.wikidata.wdtk.rdf.RdfConverter")  # noqa: N806
        self._JRdfSerializer = JClass(  # noqa: N806
            "org.wikidata.wdtk.rdf.RdfSerializer"
        )
        self._JRdfWriter = JClass("org.wikidata.wdtk.rdf.RdfWriter")  # noqa: N806
        self._JByteArrayOutputStream = JClass(  # noqa: N806
            "java.io.ByteArrayOutputStream"
        )

        self._tasks = (
            0x0
            # Document terms
            # TODO: document labels, descriptions, aliases
            | self._JRdfSerializer.TASK_LABELS
            | self._JRdfSerializer.TASK_DESCRIPTIONS
            | self._JRdfSerializer.TASK_ALIASES
            #
            # Statements
            # TODO: document that this means "full" statements (i.e. with reification).
            | self._JRdfSerializer.TASK_STATEMENTS
            # TODO: document that TASK_SIMPLE_STATEMENTS writes a "simple" statement
            #  (i.e. not more complex stuff that would need reification to express and
            #  that additionally it will only print statements of the "best" rank of
            #  that particular statement group. (Not 100% sure what that means, but it
            #  is atleast specific to that respective entity.)
            | self._JRdfSerializer.TASK_SIMPLE_STATEMENTS
            #
            # Items
            # TODO: Document item selector
            | self._JRdfSerializer.TASK_ITEMS
            # TODO: document that TASK_SITELINKS refers to links to Wikipedia articles.
            | self._JRdfSerializer.TASK_SITELINKS
            #
            # Properties
            # TODO: Document property selector
            | self._JRdfSerializer.TASK_PROPERTIES
            # TODO: not sure what this is (but it is only applicable to properties)
            | self._JRdfSerializer.TASK_DATATYPES
            # TODO: not sure what this is (but it is only applicable to properties)
            | self._JRdfSerializer.TASK_PROPERTY_LINKS
        )

    @classmethod
    def _get_sites(cls, file: Path) -> JObject:
        dump = JClass("org.wikidata.wdtk.dumpfiles.MwLocalDumpFile")(str(file))
        processor = JClass("org.wikidata.wdtk.dumpfiles.MwSitesDumpFileProcessor")()
        processor.processDumpFileContents(dump.getDumpFileStream(), dump)
        return processor.getSites()

    @classmethod
    def _get_property_register(cls) -> JObject:
        return JClass(
            "org.wikidata.wdtk.rdf.PropertyRegister"
        ).getWikidataPropertyRegister()

    @classmethod
    def _get_json_deserializer(cls) -> JObject:
        return JClass("org.wikidata.wdtk.datamodel.helpers.JsonDeserializer")(
            JClass("org.wikidata.wdtk.datamodel.helpers.Datamodel").SITE_WIKIDATA
        )

    def process_revision(self, revision: WikidataDumpRevision) -> Sequence[RdfTriple]:
        if revision.text is None:
            raise WikidataRdfSerializationException("Entity has no text.", revision)

        output_stream = self._JByteArrayOutputStream()

        rdf_writer = self._JRdfWriter(self._format_ntriples, output_stream)
        rdf_writer.start()

        rdf_converter = self._JRdfConverter(
            rdf_writer, self._sites, self._property_register
        )
        rdf_converter.setTasks(self._tasks)

        model = revision.model
        if '"redirect":' in revision.text:
            # TODO: document that revisions that contain the "redirect" field in their
            #  JSON indicate that the respective entity is being redirected to the
            #  target entity starting from that point in time. Additionally, if an
            #  entity is ever the source of a redirect all revisions of it will also
            #  carry the revision.redirect attribute indicating the target of the
            #  redirect, even if at that time the entity is not yet being redirect.
            raise WikidataRdfSerializationException("Entity is redirected.", revision)

        elif model == self._model_item:
            try:
                document = self._json_deserializer.deserializeItemDocument(
                    revision.text
                )
            except JException as exception:
                raise WikidataRdfSerializationException(
                    "Item could not be JSON-deserialized.", revision, exception
                )
            try:
                rdf_converter.writeItemDocument(document)
            except JException as exception:
                raise WikidataRdfSerializationException(
                    "Item could not be RDF-serialized.", revision, exception
                )

        elif model == self._model_property:
            try:
                document = self._json_deserializer.deserializePropertyDocument(
                    revision.text
                )
            except JException as exception:
                raise WikidataRdfSerializationException(
                    "Property could not be JSON-deserialized.", revision, exception
                )
            try:
                rdf_converter.writePropertyDocument(document)
            except JException as exception:
                raise WikidataRdfSerializationException(
                    "Property could not be RDF-serialized.", revision, exception
                )

        else:
            # Lexemes, Wikitext pages (i.e., discussion pages), and others.
            raise WikidataRdfSerializationException(
                f"Entity model '{revision.model}' is not RDF-serializable.", revision
            )

        rdf_writer.finish()

        return [
            RdfTriple(
                *map(self._prefix_ntriples_uri, triple_str[: -len(" .")].split(" ", 2))
            )
            for triple_str in str(output_stream).splitlines()
        ]

    @classmethod
    def _prefix_ntriples_uri(cls, uri: str) -> str:
        if not uri[0] == "<":
            return uri  # Argument is not an URI.

        uri = uri[1:-1]  # Remove brackets before and after URI.
        for prefix, prefix_url in cls.PREFIXES.items():
            if uri.startswith(prefix_url):
                return prefix + ":" + uri[len(prefix_url) :]
        return "<" + uri + ">"


def main() -> None:
    settings = KgEvolveSettings.find_and_load_from_settings_file()
    settings.setup_logging()

    startJVM(classpath=[str(settings.kg_evolve.wikidata_toolkit_jars_dir / "*")])

    setup_java_logging_bridge()

    rdf_serializer = WikidataRdfSerializer(
        settings.kg_evolve.data_dir / "dumpfiles" / "wikidatawiki-20210401-sites.sql.gz"
    )

    wikidata_dump = WikidataDump(
        settings.kg_evolve.data_dir
        / "dumpfiles"
        / "wikidatawiki-20210401-pages-meta-history1.xml-p1p192.7z"
        # / "wikidatawiki-20210401-pages-meta-history1.xml-p14305p18638.7z"
        # / "wikidatawiki-20210401-pages-meta-history1.xml-p267210p283697.7z"
        # / "wikidatawiki-20210401-pages-meta-history25.xml-p67174382p67502430.7z"
    )

    exception_reason_counter = Counter()

    for i, revision in enumerate(wikidata_dump.iter_revisions()):
        p = Path(
            settings.kg_evolve.data_dir
            / "rdf"
            / wikidata_dump._file.name
            / revision.prefixed_title
            / (revision.revision_id + ".ttl")
        )
        if p.exists():
            continue

        try:
            triples = rdf_serializer.process_revision(revision) or ()
        except WikidataRdfSerializationException as exception:
            exception_reason_counter[exception.reason] += 1
            continue

        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="UTF-8") as fout:
            for triple in triples:
                fout.write(" ".join(triple) + " .\n")  # noqa: T001

        if i == 10000:
            break

    with Path(
        settings.kg_evolve.data_dir
        / "rdf"
        / wikidata_dump._file.name
        / "exceptions.log"
    ).open("w", encoding="UTF-8") as fout:
        for k, v in exception_reason_counter.most_common():
            fout.write(f"{k}: {v}\n")

    shutdownJVM()


if __name__ == "__main__":
    main()

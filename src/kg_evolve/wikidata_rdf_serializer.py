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
from typing import NamedTuple, Optional, Sequence

from jpype import JClass, JObject, shutdownJVM, startJVM  # type: ignore
from nasty_utils import ColoredBraceStyleAdapter

from kg_evolve.java_logging_bride import setup_java_logging_bridge
from kg_evolve.settings_ import KgEvolveSettings
from kg_evolve.wikidata_dump import WikidataDump, WikidataDumpRevision

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class RdfTriple(NamedTuple):
    head: str
    relation: str
    tail: str


class WikidataRdfSerializer:
    # Prefixes from https://www.wikidata.org/wiki/EntitySchema:E49 but sorted after
    # URLs so that the longer URLs come first to enable one-pass prefix replacing.
    PREFIXES = {
        "cc": "http://creativecommons.org/ns#",
        "dct": "http://purl.org/dc/terms/",
        "schema": "http://schema.org/",
        "wikibase": "http://wikiba.se/ontology#",
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

    def process_revision(
        self, revision: WikidataDumpRevision
    ) -> Optional[Sequence[RdfTriple]]:
        JMwRevision = JClass("org.wikidata.wdtk.dumpfiles.MwRevision")  # noqa: N806
        JRdfConverter = JClass("org.wikidata.wdtk.rdf.RdfConverter")  # noqa: N806
        JRdfSerializer = JClass("org.wikidata.wdtk.rdf.RdfSerializer")  # noqa: N806
        JRdfWriter = JClass("org.wikidata.wdtk.rdf.RdfWriter")  # noqa: N806
        JRDFFormat = JClass("org.eclipse.rdf4j.rio.RDFFormat")  # noqa: N806

        revision_log_str = "page {} revision {} ({})"
        revision_log_args = (
            revision.page_id,
            revision.revision_id,
            revision.prefixed_title,
        )

        if revision.text is None:
            _LOGGER.warning(
                "Substituting no text for " + revision_log_str + ".", *revision_log_args
            )
            revision.text = ""

        is_redirect = revision.redirect is not None
        is_wikibase_redirection = '"redirect":' in revision.text
        if is_redirect != is_wikibase_redirection:
            _LOGGER.warning(
                "Diverging redirection detection for " + revision_log_str + ".",
                *revision_log_args,
            )
            _LOGGER.warning("  revision.redirect: {}", revision.redirect)
            _LOGGER.warning("  revision.text: {}", revision.text)

        output_stream = self._get_byte_array_output_stream()
        rdf_writer = JRdfWriter(JRDFFormat.NTRIPLES, output_stream)
        rdf_converter = JRdfConverter(rdf_writer, self._sites, self._property_register)

        rdf_converter.setTasks(
            JRdfSerializer.TASK_TERMS
            | JRdfSerializer.TASK_STATEMENTS
            | JRdfSerializer.TASK_SITELINKS
            | JRdfSerializer.TASK_DATATYPES
            | JRdfSerializer.TASK_PROPERTY_LINKS
            | JRdfSerializer.TASK_SIMPLE_STATEMENTS
            | JRdfSerializer.TASK_ITEMS
            | JRdfSerializer.TASK_PROPERTIES
        )
        rdf_writer.start()

        model = revision.model
        if is_redirect or is_wikibase_redirection:
            document = self._json_deserializer.deserializeEntityRedirectDocument(
                revision.text
            )
            _LOGGER.info(
                "Ignoring " + revision_log_str + " because it is a redirection.",
                *revision_log_args,
            )
            _LOGGER.info("  Document: {}", document)
        elif model == JMwRevision.MODEL_WIKIBASE_ITEM:
            document = self._json_deserializer.deserializeItemDocument(revision.text)
            rdf_converter.writeItemDocument(document)
        elif model == JMwRevision.MODEL_WIKIBASE_PROPERTY:
            document = self._json_deserializer.deserializePropertyDocument(
                revision.text
            )
            rdf_converter.writePropertyDocument(document)
        elif model == JMwRevision.MODEL_WIKIBASE_LEXEME:
            document = self._json_deserializer.deserializeLexemeDocument(revision.text)
            _LOGGER.info(
                "Ignoring " + revision_log_str + " because it is a lexeme.",
                *revision_log_args,
            )
            _LOGGER.info("  Document: {}", document)
        elif model == JMwRevision.MODEL_WIKITEXT:
            pass
        else:
            _LOGGER.info(
                "Ignoring " + revision_log_str + " because model '{}' is unknown.",
                *revision_log_args,
                model,
            )

        rdf_writer.finish()

        triples = []
        for triple_str in str(output_stream).splitlines():
            assert triple_str.endswith(" .")
            triple_str = triple_str[:-2]
            triples.append(
                RdfTriple(*map(self._prefix_ntriples_uri, triple_str.split(" ", 2)))
            )
        return triples

    @classmethod
    def _prefix_ntriples_uri(cls, uri: str) -> str:
        if not uri[0] == "<":
            return uri  # Argument is not an URI.

        uri = uri[1:-1]  # Remove brackets before and after URI.
        for prefix, prefix_url in cls.PREFIXES.items():
            if uri.startswith(prefix_url):
                return prefix + ":" + uri[len(prefix_url) :]
        return "<" + uri + ">"

    @classmethod
    def _get_sites(cls, file: Path) -> JObject:
        JMwLocalDumpFile = JClass(  # noqa: N806
            "org.wikidata.wdtk.dumpfiles.MwLocalDumpFile"
        )
        JMwSitesDumpFileProcessor = JClass(  # noqa: N806
            "org.wikidata.wdtk.dumpfiles.MwSitesDumpFileProcessor"
        )

        sites_table_dump = JMwLocalDumpFile(str(file))
        sites_dump_file_processor = JMwSitesDumpFileProcessor()
        sites_dump_file_processor.processDumpFileContents(
            sites_table_dump.getDumpFileStream(), sites_table_dump
        )
        return sites_dump_file_processor.getSites()

    @classmethod
    def _get_property_register(cls) -> JObject:
        JPropertyRegister = JClass(  # noqa: N806
            "org.wikidata.wdtk.rdf.PropertyRegister"
        )

        return JPropertyRegister.getWikidataPropertyRegister()

    @classmethod
    def _get_json_deserializer(cls) -> JObject:
        JDataModel = JClass(  # noqa: N806
            "org.wikidata.wdtk.datamodel.helpers.Datamodel"
        )
        JJsonDeserializer = JClass(  # noqa: N806
            "org.wikidata.wdtk.datamodel.helpers.JsonDeserializer"
        )
        return JJsonDeserializer(JDataModel.SITE_WIKIDATA)

    @classmethod
    def _get_byte_array_output_stream(cls) -> JObject:
        JByteArrayOutputStream = JClass("java.io.ByteArrayOutputStream")  # noqa: N806
        return JByteArrayOutputStream()


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
        / "wikidatawiki-20210401-pages-meta-history1.xml-p14305p18638.7z"
    )

    for i, revision in enumerate(wikidata_dump.iter_revisions()):
        p = Path(
            settings.kg_evolve.data_dir
            / "rdf"
            / revision.page_id
            / (revision.revision_id + ".ttl")
        )
        if p.exists():
            continue
        p.parent.mkdir(parents=True, exist_ok=True)

        with p.open("w", encoding="UTF-8") as fout:
            for triple in rdf_serializer.process_revision(revision) or ():
                fout.write(" ".join(triple) + " .\n")  # noqa: T001
        if i % 100 == 0:
            print(i)
        # if i == 1000:
        #     break

    shutdownJVM()


if __name__ == "__main__":
    main()

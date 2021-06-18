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
from typing import NamedTuple, Optional, Sequence

from jpype import JClass, JException, JObject  # type: ignore
from nasty_utils import ColoredBraceStyleAdapter

from wikidata_history_analyzer.jvm_manager import JvmManager
from wikidata_history_analyzer.wikidata_revision import WikidataRevision
from wikidata_history_analyzer.wikidata_sites_table import WikidataSitesTable

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class RdfTriple(NamedTuple):
    subject: str
    predicate: str
    object: str


class WikidataRdfSerializationException(Exception):
    def __init__(
        self,
        reason: str,
        revision: WikidataRevision,
        exception: Optional[Exception] = None,
    ) -> None:
        self.reason = reason
        self.revision = revision
        self.exception = exception

    def __str__(self) -> str:
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

    def __init__(
        self, sites_table: WikidataSitesTable, jvm_manager: JvmManager
    ) -> None:
        self._sites = sites_table.load_wdtk_object(jvm_manager)
        self._property_register = self._get_property_register()
        self._json_deserializer = self._get_json_deserializer()

        JMwRevision = JClass("org.wikidata.wdtk.dumpfiles.MwRevision")  # noqa: N806
        self._model_item = str(JMwRevision.MODEL_WIKIBASE_ITEM)
        self._model_property = str(JMwRevision.MODEL_WIKIBASE_PROPERTY)

        self._format_ntriples = JClass("org.eclipse.rdf4j.rio.RDFFormat").NTRIPLES

        JRdfSerializer = JClass("org.wikidata.wdtk.rdf.RdfSerializer")  # noqa: N806
        self._tasks = (
            0x0
            # Document terms
            # TODO: document labels, descriptions, aliases
            | JRdfSerializer.TASK_LABELS
            | JRdfSerializer.TASK_DESCRIPTIONS
            | JRdfSerializer.TASK_ALIASES
            #
            # Statements
            # TODO: document that TASK_SIMPLE_STATEMENTS writes a "simple" statement
            #  (i.e. not more complex stuff that would need reification to express and
            #  that additionally it will only print statements of the "best" rank of
            #  that particular statement group. (Not 100% sure what that means, but it
            #  is atleast specific to that respective entity.)
            | JRdfSerializer.TASK_SIMPLE_STATEMENTS
            # TODO: document that this means "full" statements (i.e. with reification).
            | JRdfSerializer.TASK_STATEMENTS
            #
            # Items
            # TODO: Document item selector
            | JRdfSerializer.TASK_ITEMS
            # TODO: document that TASK_SITELINKS refers to links to Wikipedia articles.
            | JRdfSerializer.TASK_SITELINKS
            #
            # Properties
            # TODO: Document property selector
            | JRdfSerializer.TASK_PROPERTIES
            # TODO: not sure what this is (but it is only applicable to properties)
            | JRdfSerializer.TASK_DATATYPES
            # TODO: not sure what this is (but it is only applicable to properties)
            | JRdfSerializer.TASK_PROPERTY_LINKS
        )

        # Keep references to Java classes here, so they do not have to be looked up
        # when processing each revision individually.
        self._JRdfConverter = JClass("org.wikidata.wdtk.rdf.RdfConverter")  # noqa: N806
        self._JRdfWriter = JClass("org.wikidata.wdtk.rdf.RdfWriter")  # noqa: N806
        self._JByteArrayOutputStream = JClass(  # noqa: N806
            "java.io.ByteArrayOutputStream"
        )

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

    def process_revision(self, revision: WikidataRevision) -> Sequence[RdfTriple]:
        if revision.text is None:
            raise WikidataRdfSerializationException("Entity has no text.", revision)

        output_stream = self._JByteArrayOutputStream()

        rdf_writer = self._JRdfWriter(self._format_ntriples, output_stream)
        rdf_writer.start()

        rdf_converter = self._JRdfConverter(
            rdf_writer, self._sites, self._property_register
        )
        rdf_converter.setTasks(self._tasks)

        # The following two method calls are part of the Wikidata Toolkit's
        # RdfSerializer. They add RDF triples that are independent of the actual
        # revision being exported. Because of this, we do not call them here.
        # rdf_converter.writeNamespaceDeclarations() # noqa: E800
        # rdf_converter.writeBasicDeclarations() # noqa: E800

        if '"redirect":' in revision.text:
            # TODO: document that revisions that contain the "redirect" field in their
            #  JSON indicate that the respective entity is being redirected to the
            #  target entity starting from that point in time. Additionally, if an
            #  entity is ever the source of a redirect all revisions of it will also
            #  carry the revision.redirect attribute indicating the target of the
            #  redirect, even if at that time the entity is not yet being redirect.
            raise WikidataRdfSerializationException("Entity is redirected.", revision)

        model = revision.content_model
        if model != self._model_item and model != self._model_property:
            # Lexemes, Wikitext pages (i.e., discussion pages), and others.
            raise WikidataRdfSerializationException(
                f"Entity model '{revision.content_model}' is not RDF-serializable.",
                revision,
            )

        exception_msg = ""
        try:
            if model == self._model_item:
                exception_msg = "Item could not be JSON-deserialized."
                doc = self._json_deserializer.deserializeItemDocument(revision.text)
                subject = rdf_writer.getUri(doc.getEntityId().getIri())

                exception_msg = "Item could not be RDF-serialized."
                # Taken from RdfConverter.writeItemDocument:
                rdf_converter.writeDocumentType(subject, self._JRdfWriter.WB_ITEM)
                rdf_converter.writeDocumentTerms(doc)
                rdf_converter.writeStatements(doc)
                rdf_converter.writeSiteLinks(subject, doc.getSiteLinks())

            elif model == self._model_property:
                exception_msg = "Property could not be JSON-deserialized."
                doc = self._json_deserializer.deserializePropertyDocument(revision.text)
                subject = rdf_writer.getUri(doc.getEntityId().getIri())

                exception_msg = "Property could not be RDF-serialized."
                # Taken from RdfConverter.writePropertyDocument:
                rdf_converter.writeDocumentType(subject, self._JRdfWriter.WB_PROPERTY)
                rdf_converter.writePropertyDatatype(doc)
                rdf_converter.writeDocumentTerms(doc)
                rdf_converter.writeStatements(doc)
                rdf_converter.writeInterPropertyLinks(doc)

        except JException as exception:
            raise WikidataRdfSerializationException(exception_msg, revision, exception)

        # The RdfConverter.finishDocument() method in Wikidata Toolkit is called from
        # both RdfConverter.writeItemDocument and RdfConverter.writePropertyDocument and
        # exports RDF triples like "this property used above is a complement of this
        # other property". However, this information is not actually stored in the
        # revisions themselves, but rather queried from the internet. Because of this
        # it is also does not change between revisions.  Because of this, we do not call
        # it here.
        # rdf_converter.finishDocument() # noqa: E800

        rdf_writer.finish()

        return [
            RdfTriple(*map(self._prefix_ntriples_uri, line[: -len(" .")].split(" ", 2)))
            for line in str(output_stream).splitlines()
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

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

from dataclasses import asdict, dataclass
from logging import getLogger
from typing import ClassVar, Mapping, NamedTuple, Optional, Sequence

from jpype import JClass, JException, JObject  # type: ignore
from nasty_utils import ColoredBraceStyleAdapter

from wikidata_history_analyzer.jvm_manager import JvmManager
from wikidata_history_analyzer.wikidata_revision import (
    WikidataRevision,
    WikidataRevisionProcessingException,
)
from wikidata_history_analyzer.wikidata_sites_table import WikidataSitesTable

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataRevisionWdtkRdfSerializationException(
    WikidataRevisionProcessingException
):
    pass


class WikidataRdfTriple(NamedTuple):
    subject: str
    predicate: str
    object: str


@dataclass
class WikidataRdfRevision(WikidataRevision):
    # Prefixes taken from
    # https://www.mediawiki.org/w/index.php?title=Wikibase/Indexing/RDF_Dump_Format&oldid=4471307#Full_list_of_prefixes
    # but sorted so that the longer URLs come first to enable one-pass prefixing.
    PREFIXES: ClassVar[Mapping[str, str]] = {
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

    triples: Sequence[WikidataRdfTriple]

    @classmethod
    def from_revision(
        cls,
        revision: WikidataRevision,
        sites_table: WikidataSitesTable,
        jvm_manager: JvmManager,
        *,
        include_bloat: bool = False,
    ) -> WikidataRdfRevision:
        _load_wdtk_classes_and_objects(jvm_manager)
        assert (  # for mypy.
            _JAVA_BYTE_ARRAY_OUTPUT_STREAM is not None
            and _WDTK_RDF_WRITER is not None
            and _WDTK_RDF_CONVERTER is not None
        )

        # TODO: document that this is basically a mix of RdfSerializer, RdfWriter,
        #  RdfConverter and AbstractRdfConverter.
        # TODO: document that RdfConverter basically only adds the "TASK filtering" on
        #  top of AbstractRdfConverter.

        java_output_stream = _JAVA_BYTE_ARRAY_OUTPUT_STREAM()
        wdtk_sites = sites_table.load_wdtk_object(jvm_manager)
        wdtk_rdf_writer = _WDTK_RDF_WRITER(_WDTK_NTRIPLES_FORMAT, java_output_stream)
        wdtk_rdf_writer.start()
        wdtk_rdf_converter = _WDTK_RDF_CONVERTER(
            wdtk_rdf_writer, wdtk_sites, _WDTK_PROPERTY_REGISTER
        )

        if include_bloat:
            wdtk_rdf_converter.writeNamespaceDeclarations()
            wdtk_rdf_converter.writeBasicDeclarations()

        wdtk_document = revision.load_wdtk_deserialization(jvm_manager)
        wdtk_document_class = str(wdtk_document.getClass().getName())
        wdtk_resource = wdtk_rdf_writer.getUri(wdtk_document.getEntityId().getIri())

        if not (
            wdtk_document_class == "ItemDocument"
            or wdtk_document_class == "PropertyDocument"
        ):
            raise WikidataRevisionWdtkRdfSerializationException(
                f"RDF serialization of {wdtk_document_class} not implemented.", revision
            )

        try:
            if wdtk_document_class == "ItemDocument":
                wdtk_rdf_converter.writeDocumentType(wdtk_resource, _WDTK_ITEM_IRI)
                wdtk_rdf_converter.writeDocumentTerms(wdtk_document)
                wdtk_rdf_converter.writeStatements(wdtk_document)
                wdtk_rdf_converter.writeSiteLinks(
                    wdtk_resource, wdtk_document.getSiteLinks()
                )

            elif wdtk_document_class == "PropertyDocument":
                wdtk_rdf_converter.writeDocumentType(wdtk_resource, _WDTK_PROPERTY_IRI)
                wdtk_rdf_converter.writePropertyDatatype(wdtk_document)
                wdtk_rdf_converter.writeDocumentTerms(wdtk_document)
                wdtk_rdf_converter.writeStatements(wdtk_document)
                wdtk_rdf_converter.writeInterPropertyLinks(wdtk_document)

            # TODO: Handle EntityRedirectDocument here?
            # TODO: document that revisions that contain the "redirect" field in their
            #  JSON indicate that the respective entity is being redirected to the
            #  target entity starting from that point in time. Additionally, if an
            #  entity is ever the source of a redirect all revisions of it will also
            #  carry the revision.redirect attribute indicating the target of the
            #  redirect, even if at that time the entity is not yet being redirect.

        except JException as exception:
            raise WikidataRevisionWdtkRdfSerializationException(
                "RDF serialization by Wikidata Toolkit failed.", revision, exception
            )

        if include_bloat:
            # The RdfConverter.finishDocument() method in Wikidata Toolkit is called
            # from both RdfConverter.writeItemDocument and
            # RdfConverter.writePropertyDocument and exports RDF triples like "this
            # property used above is a complement of this other property". However, this
            # information is not actually stored in the revisions themselves, but rather
            # queried from the internet. Because of this it is also does not change
            # between revisions.
            wdtk_rdf_converter.finishDocument()

        wdtk_rdf_writer.finish()

        return WikidataRdfRevision(
            **asdict(revision),
            triples=[
                WikidataRdfTriple(
                    *map(cls._prefix_ntriples_uri, line[: -len(" .")].split(" ", 2))
                )
                for line in str(java_output_stream).splitlines()
            ],
        )

    @classmethod
    def _prefix_ntriples_uri(cls, uri: str) -> str:
        if not uri[0] == "<":
            return uri  # Argument is not an URI.

        uri = uri[1:-1]  # Remove brackets before and after URI.
        for prefix, prefix_url in cls.PREFIXES.items():
            if uri.startswith(prefix_url):
                return prefix + ":" + uri[len(prefix_url) :]
        return "<" + uri + ">"


_JAVA_BYTE_ARRAY_OUTPUT_STREAM: Optional[JClass] = None
_WDTK_RDF_CONVERTER: Optional[JClass] = None
_WDTK_RDF_WRITER: Optional[JClass] = None
_WDTK_PROPERTY_REGISTER: Optional[JObject] = None
_WDTK_NTRIPLES_FORMAT: Optional[JObject] = None
_WDTK_ITEM_IRI: Optional[JObject] = None
_WDTK_PROPERTY_IRI: Optional[JObject] = None


def _load_wdtk_classes_and_objects(_jvm_manager: JvmManager) -> None:
    global _JAVA_BYTE_ARRAY_OUTPUT_STREAM
    if _JAVA_BYTE_ARRAY_OUTPUT_STREAM is None:
        _JAVA_BYTE_ARRAY_OUTPUT_STREAM = JClass("java.io.ByteArrayOutputStream")

    global _WDTK_RDF_CONVERTER
    if _WDTK_RDF_CONVERTER is None:
        _WDTK_RDF_CONVERTER = JClass("org.wikidata.wdtk.rdf.RdfConverter")

    global _WDTK_RDF_WRITER
    if _WDTK_RDF_WRITER is None:
        _WDTK_RDF_WRITER = JClass("org.wikidata.wdtk.rdf.RdfWriter")

    global _WDTK_PROPERTY_REGISTER
    if _WDTK_PROPERTY_REGISTER is None:
        _WDTK_PROPERTY_REGISTER = JClass(
            "org.wikidata.wdtk.rdf.PropertyRegister"
        ).getWikidataPropertyRegister()

    global _WDTK_NTRIPLES_FORMAT
    if _WDTK_NTRIPLES_FORMAT is None:
        _WDTK_NTRIPLES_FORMAT = JClass("org.eclipse.rdf4j.rio.RDFFormat").NTRIPLES

    global _WDTK_ITEM_IRI
    if _WDTK_ITEM_IRI is None:
        _WDTK_ITEM_IRI = _WDTK_RDF_WRITER.WB_ITEM

    global _WDTK_PROPERTY_IRI
    if _WDTK_PROPERTY_IRI is None:
        _WDTK_PROPERTY_IRI = _WDTK_RDF_WRITER.WB_PROPERTY

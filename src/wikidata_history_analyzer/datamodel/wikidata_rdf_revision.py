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

from logging import getLogger
from typing import NamedTuple, Optional, Sequence

from jpype import JClass, JException, JObject  # type: ignore
from nasty_utils import ColoredBraceStyleAdapter

from wikidata_history_analyzer.datamodel.wikidata_raw_revision import (
    WikidataRawRevision,
)
from wikidata_history_analyzer.datamodel.wikidata_revision import (
    WikidataRevision,
    WikidataRevisionProcessingException,
)
from wikidata_history_analyzer.dumpfiles.wikidata_sites_table import WikidataSitesTable
from wikidata_history_analyzer.jvm_manager import JvmManager

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class WikidataRdfRevisionWdtkSerializationException(
    WikidataRevisionProcessingException
):
    pass


# Prefixes taken from
# https://www.mediawiki.org/w/index.php?title=Wikibase/Indexing/RDF_Dump_Format&oldid=4471307#Full_list_of_prefixes
# but sorted so that the longer URLs come first to enable one-pass prefixing.
WIKIDATA_RDF_PREFIXES = {
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


class WikidataRdfTriple(NamedTuple):
    subject: str
    predicate: str
    object_: str

    # We need to reimplement equivalence and hash generation, since WDTK generates blank
    # nodes as something like "_:node1f8mm5pv5x4125", i.e., it gives them an
    # auto-generated ID. There is now way to ensure that the blank node ID for the same
    # triple will be the same if the RDF is regenerated. Because of this, we are here
    # treating all blank nodes as being equal to one another. Luckily, for WDTK's RDF
    # generation, blank nodes only occur in the object position and are never reused.

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, WikidataRdfTriple):
            return False
        if self.object_[:2] == "_:" and other.object_[:2] == "_:":
            return self.subject == other.subject and self.predicate == other.predicate
        else:
            return (
                self.subject == other.subject
                and self.predicate == other.predicate
                and self.object_ == other.object_
            )

    def __hash__(self) -> int:
        if self.object_[:2] == "_:":
            return hash((self.subject, self.predicate, "_:"))
        else:
            return hash((self.subject, self.predicate, self.object_))


class WikidataRdfRevision(WikidataRevision):
    triples: Sequence[WikidataRdfTriple]

    @classmethod
    def from_raw_revision(
        cls,
        revision: WikidataRawRevision,
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
        wdtk_document_class = str(wdtk_document.getClass().getSimpleName())
        wdtk_resource = wdtk_rdf_writer.getUri(wdtk_document.getEntityId().getIri())

        if not (
            wdtk_document_class == "ItemDocumentImpl"
            or wdtk_document_class == "PropertyDocumentImpl"
        ):
            raise WikidataRdfRevisionWdtkSerializationException(
                f"RDF serialization of {wdtk_document_class} not implemented.", revision
            )

        try:
            if wdtk_document_class == "ItemDocumentImpl":
                wdtk_rdf_converter.writeDocumentType(wdtk_resource, _WDTK_ITEM_IRI)
                wdtk_rdf_converter.writeDocumentTerms(wdtk_document)
                wdtk_rdf_converter.writeStatements(wdtk_document)
                wdtk_rdf_converter.writeSiteLinks(
                    wdtk_resource, wdtk_document.getSiteLinks()
                )

            elif wdtk_document_class == "PropertyDocumentImpl":
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
            raise WikidataRdfRevisionWdtkSerializationException(
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

        return WikidataRdfRevision.construct(
            prefixed_title=revision.prefixed_title,
            namespace=revision.namespace,
            page_id=revision.page_id,
            redirect=revision.redirect,
            revision_id=revision.revision_id,
            parent_revision_id=revision.parent_revision_id,
            timestamp=revision.timestamp,
            contributor=revision.contributor,
            contributor_id=revision.contributor_id,
            is_minor=revision.is_minor,
            comment=revision.comment,
            content_model=revision.content_model,
            format=revision.format,
            sha1=revision.sha1,
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
        for prefix, prefix_url in WIKIDATA_RDF_PREFIXES.items():
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

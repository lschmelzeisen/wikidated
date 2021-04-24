/*
 * Copyright 2021 Lukas Schmelzeisen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lschmelzeisen.kgevolve;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.wikidata.wdtk.datamodel.helpers.JsonDeserializer;
import org.wikidata.wdtk.datamodel.interfaces.*;
import org.wikidata.wdtk.dumpfiles.MwRevision;
import org.wikidata.wdtk.dumpfiles.MwRevisionProcessor;
import org.wikidata.wdtk.rdf.PropertyRegister;
import org.wikidata.wdtk.rdf.RdfConverter;
import org.wikidata.wdtk.rdf.RdfSerializer;
import org.wikidata.wdtk.rdf.RdfWriter;

public class MyRdfSerializer implements MwRevisionProcessor {
    private final Path outputDir;
    private final Sites sites;
    private final PropertyRegister propertyRegister;
    private final JsonDeserializer jsonDeserializer;

    public MyRdfSerializer(
            Path outputDir, Sites sites, PropertyRegister propertyRegister, String siteIri)
            throws IOException {
        if (!Files.exists(outputDir)) Files.createDirectory(outputDir);
        this.outputDir = outputDir;
        this.sites = sites;
        this.propertyRegister = propertyRegister;
        jsonDeserializer = new JsonDeserializer(siteIri);
    }

    @Override
    public void startRevisionProcessing(
            String siteName, String baseUrl, Map<Integer, String> namespaces) {
        // Nothing to do.
    }

    @Override
    public void processRevision(MwRevision mwRevision) {
        Path file = outputDir.resolve(mwRevision.getPageId() + ".ttl.gz");
        if (!Files.exists(file)) {
            try (var outputStream =
                    new GZIPOutputStream(
                            Files.newOutputStream(file, StandardOpenOption.CREATE_NEW))) {
                var rdfWriter = new RdfWriter(RDFFormat.TURTLE, outputStream);
                var rdfConverter = new RdfConverter(rdfWriter, this.sites, this.propertyRegister);
                rdfWriter.start();
                rdfConverter.writeNamespaceDeclarations();
                rdfConverter.writeBasicDeclarations();
                rdfWriter.finish();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try (var writer =
                new BufferedWriter(
                        new OutputStreamWriter(
                                new GZIPOutputStream(
                                        Files.newOutputStream(
                                                file,
                                                StandardOpenOption.CREATE,
                                                StandardOpenOption.APPEND)),
                                StandardCharsets.UTF_8))) {
            writer.write("\n");
            writer.write("\n");
            writer.write("\n");
            writer.write("#".repeat(80) + "\n");
            writer.write(
                    String.format(
                            "# %s (pageId: %d, revisionId: %d)\n",
                            mwRevision.getTitle(),
                            mwRevision.getPageId(),
                            mwRevision.getRevisionId()));
            writer.write("#".repeat(80) + "\n");

            try (var inMemoryStream = new ByteArrayOutputStream()) {
                var rdfWriter = new RdfWriter(RDFFormat.TURTLE, inMemoryStream);
                var rdfConverter = new RdfConverter(rdfWriter, this.sites, this.propertyRegister);
                rdfConverter.setTasks(RdfSerializer.TASK_ALL_ENTITIES | RdfSerializer.TASK_ALL_EXACT_DATA);
//                rdfConverter.setTasks(RdfSerializer.TASK_ITEMS | RdfSerializer.TASK_SIMPLE_STATEMENTS);
                rdfWriter.start();

                // TODO: redirects!
                if (MwRevision.MODEL_WIKIBASE_ITEM.equals(mwRevision.getModel())) {
                    ItemDocument document =
                            jsonDeserializer.deserializeItemDocument(mwRevision.getText());
                    rdfConverter.writeItemDocument(document);
                } else if (MwRevision.MODEL_WIKIBASE_PROPERTY.equals(mwRevision.getModel())) {
                    PropertyDocument document =
                            jsonDeserializer.deserializePropertyDocument(mwRevision.getText());
                    rdfConverter.writePropertyDocument(document);
                } else if (MwRevision.MODEL_WIKIBASE_LEXEME.equals(mwRevision.getModel())) {
                    // LexemeDocument document =
                    // jsonDeserializer.deserializeLexemeDocument(mwRevision.getText());
                }

                rdfWriter.finish();
                try (var inputStream =
                        new InputStreamReader(
                                new ByteArrayInputStream(inMemoryStream.toByteArray()),
                                StandardCharsets.UTF_8)) {
                    inputStream.transferTo(writer);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finishRevisionProcessing() {
        // Nothing to do.
    }
}

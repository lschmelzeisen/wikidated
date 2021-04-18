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

import examples.ExampleHelpers;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessorBroker;
import org.wikidata.wdtk.dumpfiles.EntityTimerProcessor;
import org.wikidata.wdtk.dumpfiles.WikibaseRevisionProcessor;

public class KgEvolve {
    public static void main(String[] args) throws IOException, InterruptedException {
        ExampleHelpers.configureLogging();

        Path fullDumpFilesDirectory = Paths.get("dumpfiles/wikidatawiki/full-20210401");
        Path dumpFile =
                fullDumpFilesDirectory.resolve(
                        // "wikidatawiki-20210401-pages-meta-history1.xml-p1p192.7z");
                        "wikidatawiki-20210401-pages-meta-history25.xml-p67174382p67502430.7z");

        var entityTimerProcessor = new EntityTimerProcessor(0);

        var entityDocumentProcessor = new EntityDocumentProcessorBroker();
        entityDocumentProcessor.registerEntityDocumentProcessor(entityTimerProcessor);

        var dumpProcessor =
                new FastRevisionDumpFileProcessor(
                        new WikibaseRevisionProcessor(
                                entityDocumentProcessor, "http://www.wikidata.org/"));

        entityTimerProcessor.open();
        dumpProcessor.processDumpFile(dumpFile);
        entityTimerProcessor.close();
    }
}

package com.lschmelzeisen.kgevolve;

import examples.ExampleHelpers;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocumentProcessorBroker;
import org.wikidata.wdtk.dumpfiles.EntityTimerProcessor;
import org.wikidata.wdtk.dumpfiles.WikibaseRevisionProcessor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class KgEvolve {
    public static void main(String[] args) throws IOException, InterruptedException {
        ExampleHelpers.configureLogging();

        Path fullDumpFilesDirectory = Paths.get("dumpfiles/wikidatawiki/full-20210401");
//        Path dumpFile = fullDumpFilesDirectory.resolve("wikidatawiki-20210401-pages-meta-history1.xml-p1p192.7z");
        Path dumpFile = fullDumpFilesDirectory.resolve("wikidatawiki-20210401-pages-meta-history25.xml-p67174382p67502430.7z");

        var entityTimerProcessor = new EntityTimerProcessor(0);

        var entityDocumentProcessor = new EntityDocumentProcessorBroker();
        entityDocumentProcessor.registerEntityDocumentProcessor(entityTimerProcessor);

        var dumpProcessor = new FastRevisionDumpFileProcessor(
                new WikibaseRevisionProcessor(entityDocumentProcessor, "http://www.wikidata.org/"));

        entityTimerProcessor.open();
        dumpProcessor.processDumpFile(dumpFile);
        entityTimerProcessor.close();
    }
}

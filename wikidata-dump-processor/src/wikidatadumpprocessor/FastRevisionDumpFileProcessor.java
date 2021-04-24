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

package wikidatadumpprocessor;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.unbescape.xml.XmlEscape;
import org.wikidata.wdtk.dumpfiles.MwRevisionProcessor;

/**
 * Similar to {@link org.wikidata.wdtk.dumpfiles.MwRevisionDumpFileProcessor} but faster.
 *
 * <p>Mainly faster because it doesn't use an XML library but does manual string parsing. Also uses
 * .7z dump files because uncompressing is faster than with .bz2. Therefore, the "7z' utility needs
 * to be in the PATH. Assumes one XML element per line and a fixed order of elements. Will fail if
 * new elements are introduces or old ones deleted from future dumps.
 *
 * <p>Does not implement {@link org.wikidata.wdtk.dumpfiles.MwDumpFileProcessor} because there is
 * {@link org.wikidata.wdtk.dumpfiles.MwDumpFile} subclass that handles 7z archives.
 */
public class FastRevisionDumpFileProcessor {
    private static class SiteInfo {
        public final String siteName;
        public final String dbName;
        public final String base;
        public final String generator;
        public final String case_;
        public final Map<Integer, String> namespaces;

        public SiteInfo(
                String siteName,
                String dbName,
                String base,
                String generator,
                String case_,
                Map<Integer, String> namespaces) {
            this.siteName = siteName;
            this.dbName = dbName;
            this.base = base;
            this.generator = generator;
            this.case_ = case_;
            this.namespaces = namespaces;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", SiteInfo.class.getSimpleName() + "[", "]")
                    .add("siteName='" + siteName + "'")
                    .add("dbName='" + dbName + "'")
                    .add("base='" + base + "'")
                    .add("generator='" + generator + "'")
                    .add("case_='" + case_ + "'")
                    .add("namespaces=" + namespaces)
                    .toString();
        }
    }

    private final MwRevisionProcessor revisionProcessor;
    public int numPages = 0;
    public int numRevision = 0;

    public FastRevisionDumpFileProcessor(MwRevisionProcessor revisionProcessor) {
        this.revisionProcessor = revisionProcessor;
    }

    public void processDumpFile(Path dumpFile) throws IOException, InterruptedException {
        Process sevenZip = new ProcessBuilder("7z", "x", "-so", dumpFile.toString()).start();
        try (var dumpContents =
                        new BufferedReader(
                                new InputStreamReader(
                                        sevenZip.getInputStream(), StandardCharsets.UTF_8));
                var sevenZipError =
                        new BufferedReader(
                                new InputStreamReader(
                                        sevenZip.getErrorStream(), StandardCharsets.UTF_8))) {
            if (sevenZipError.ready()) {
                String errorMessage =
                        sevenZipError
                                .lines()
                                .filter(Predicate.not(String::isBlank))
                                .collect(Collectors.joining(" "));
                throw new RuntimeException("7z process error: " + errorMessage);
            }

            assertOpeningTag(dumpContents.readLine(), "mediawiki");
            assertOpeningTag(dumpContents.readLine(), "siteinfo");
            var siteInfo = processSiteinfo(dumpContents);

            revisionProcessor.startRevisionProcessing(
                    siteInfo.siteName, siteInfo.base, siteInfo.namespaces);
            while (true) {
                String line = dumpContents.readLine();
                if (line == null) throw new EOFException();
                else if (isClosingTag(line, "mediawiki")) break;

                assertOpeningTag(line, "page");
                processPage(dumpContents);
            }
            revisionProcessor.finishRevisionProcessing();

            String line = dumpContents.readLine();
            if (line != null)
                throw new RuntimeException("Expected EOF, instead line was: \"" + line + "\"");
        }

        sevenZip.waitFor();
        if (sevenZip.exitValue() != 0)
            throw new RuntimeException("7z return non-zero exit value: " + sevenZip.exitValue());
    }

    private boolean isOpeningTag(String line, String element) {
        return line.stripLeading().startsWith("<" + element);
    }

    private void assertOpeningTag(String line, String element) {
        if (!isOpeningTag(line, element))
            throw new RuntimeException(
                    "Expected <" + element + ">, instead line was: \"" + line + "\"");
    }

    private boolean isClosingTag(String line, String element) {
        return line.stripLeading().startsWith("</" + element);
    }

    private void assertClosingTag(String line, String element) {
        if (!isClosingTag(line, element))
            throw new RuntimeException(
                    "Expected </" + element + ">, instead line was: \"" + line + "\"");
    }

    private String extractValue(String line, String element) {
        String openingTag = "<" + element + ">";
        String closingTag = "</" + element + ">";
        line = line.stripLeading();
        if (!line.startsWith(openingTag) || !line.endsWith(closingTag))
            throw new RuntimeException(
                    "Expected <" + element + ">, instead line was: \"" + line + "\"");
        return line.substring(openingTag.length(), line.length() - closingTag.length());
    }

    private String extractMultilineValue(BufferedReader dumpContents, String element)
            throws IOException {
        return extractMultilineValue(dumpContents, dumpContents.readLine(), element);
    }

    private String extractMultilineValue(BufferedReader dumpContents, String line, String element)
            throws IOException {
        assertOpeningTag(line, element);
        String closingTag = "</" + element + ">";

        if (line.endsWith("/>")) return ""; // <text bytes="0" />
        else if (line.endsWith(closingTag))
            return line.substring(line.indexOf(">") + 1, line.length() - closingTag.length());

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(line, line.indexOf(">") + 1, line.length());
        stringBuilder.append("\n");
        while (true) {
            if (line == null) throw new EOFException();
            else if (line.endsWith(closingTag)) {
                stringBuilder.append(line, 0, line.length() - closingTag.length());
                break;
            }
            stringBuilder.append(line);
            stringBuilder.append("\n");
            line = dumpContents.readLine();
        }
        return stringBuilder.toString();
    }

    private SiteInfo processSiteinfo(BufferedReader dumpContents) throws IOException {
        String siteName = extractValue(dumpContents.readLine(), "sitename");
        String dbName = extractValue(dumpContents.readLine(), "dbname");
        String base = extractValue(dumpContents.readLine(), "base");
        String generator = extractValue(dumpContents.readLine(), "generator");
        String case_ = extractValue(dumpContents.readLine(), "case");
        Map<Integer, String> namespaces = new HashMap<>();

        assertOpeningTag(dumpContents.readLine(), "namespaces");
        while (true) {
            String line = dumpContents.readLine();
            if (line == null) throw new EOFException();
            else if (isClosingTag(line, "namespaces")) break;

            assertOpeningTag(line, "namespace");
            int keyPos = line.indexOf("key=\"") + "key=\"".length();
            int namespaceKey = Integer.parseInt(line.substring(keyPos, line.indexOf("\"", keyPos)));
            if (line.endsWith("/>"))
                namespaces.put(namespaceKey, ""); // <namespace key="0" case="first-letter" />
            else {
                String namespaceName =
                        line.substring(
                                line.indexOf(">") + 1, line.length() - "</namespace>".length());
                namespaces.put(namespaceKey, namespaceName);
            }
        }
        assertClosingTag(dumpContents.readLine(), "siteinfo");

        return new SiteInfo(siteName, dbName, base, generator, case_, namespaces);
    }

    private void processPage(BufferedReader dumpContents) throws IOException {
        ++numPages;
        String prefixedTitle =
                XmlEscape.unescapeXml(extractValue(dumpContents.readLine(), "title"));
        int namespace = Integer.parseInt(extractValue(dumpContents.readLine(), "ns"));
        int pageId = Integer.parseInt(extractValue(dumpContents.readLine(), "id"));

        Optional<String> redirect = Optional.empty();
        String line = dumpContents.readLine();
        if (isOpeningTag(line, "redirect")) {
            int titlePos = line.indexOf("title=\"") + "title=\"".length();
            redirect = Optional.of(line.substring(titlePos, line.indexOf("\"", titlePos)));
            line = dumpContents.readLine();
        }

        while (true) {
            if (line == null) throw new EOFException();
            else if (isClosingTag(line, "page")) break;

            assertOpeningTag(line, "revision");
            processRevision(dumpContents, prefixedTitle, namespace, pageId, redirect);
            line = dumpContents.readLine();
        }
    }

    private void processRevision(
            BufferedReader dumpContents,
            String prefixedTitle,
            int namespace,
            int pageId,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<String> redirect)
            throws IOException {
        ++numRevision;
        long revisionId = Long.parseLong(extractValue(dumpContents.readLine(), "id"));

        String line = dumpContents.readLine();
        Optional<Long> parentRevisionId = Optional.empty();
        if (isOpeningTag(line, "parentid")) {
            parentRevisionId = Optional.of(Long.parseLong(extractValue(line, "parentid")));
            line = dumpContents.readLine();
        }

        String timeStamp = extractValue(line, "timestamp");

        Optional<String> contributor = Optional.empty();
        Optional<Integer> contributorId = Optional.empty();
        line = dumpContents.readLine();
        assertOpeningTag(line, "contributor");
        if (!line.contains("deleted=\"deleted\"")) {
            line = dumpContents.readLine();
            if (isOpeningTag(line, "ip")) {
                contributor = Optional.of(extractValue(line, "ip"));
            } else {
                contributor = Optional.of(extractValue(line, "username"));
                contributorId =
                        Optional.of(Integer.parseInt(extractValue(dumpContents.readLine(), "id")));
            }
            assertClosingTag(dumpContents.readLine(), "contributor");
        }

        line = dumpContents.readLine();
        boolean isMinor = false;
        if (isOpeningTag(line, "minor")) {
            isMinor = true;
            line = dumpContents.readLine();
        }

        Optional<String> comment = Optional.empty();
        if (isOpeningTag(line, "comment")) {
            if (!line.contains("deleted=\"deleted\""))
                comment = Optional.of(extractMultilineValue(dumpContents, line, "comment"));
            line = dumpContents.readLine();
        }
        String model = extractValue(line, "model");
        String format = extractValue(dumpContents.readLine(), "format");

        String text = XmlEscape.unescapeXml(extractMultilineValue(dumpContents, "text"));

        line = dumpContents.readLine();
        Optional<String> sha1 = Optional.empty();
        assertOpeningTag(line, "sha1");
        if (!line.endsWith("/>")) sha1 = Optional.of(extractValue(line, "sha1")); // <sha1 />

        assertClosingTag(dumpContents.readLine(), "revision");

        var revision =
                new FullRevision(
                        prefixedTitle,
                        namespace,
                        pageId,
                        redirect,
                        revisionId,
                        parentRevisionId,
                        timeStamp,
                        contributor,
                        contributorId,
                        isMinor,
                        comment,
                        model,
                        format,
                        text,
                        sha1);
        revisionProcessor.processRevision(revision);
    }
}

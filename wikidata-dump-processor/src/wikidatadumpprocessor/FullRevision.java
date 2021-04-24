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

import java.util.Optional;
import java.util.StringJoiner;
import org.wikidata.wdtk.dumpfiles.MwRevision;

/**
 * Similar to {@link org.wikidata.wdtk.dumpfiles.MwRevisionImpl} but carries more information.
 *
 * <p>In particular, this one has information about redirects, whether a revision is a minor
 * revision, and the revisions sha1.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class FullRevision implements MwRevision {
    private final String prefixedTitle;
    private final int namespace;
    private final int pageId;
    private final Optional<String> redirect;
    private final long revisionId;
    private final Optional<Long> parentRevisionId;
    private final String timeStamp;
    private final Optional<String> contributor;
    private final Optional<Integer> contributorId;
    private final boolean isMinor;
    private final Optional<String> comment;
    private final String model;
    private final String format;
    private final String text;
    private final Optional<String> sha1;

    public FullRevision(
            String prefixedTitle,
            int namespace,
            int pageId,
            Optional<String> redirect,
            long revisionId,
            Optional<Long> parentRevisionId,
            String timeStamp,
            Optional<String> contributor,
            Optional<Integer> contributorId,
            boolean isMinor,
            Optional<String> comment,
            String model,
            String format,
            String text,
            Optional<String> sha1) {
        this.prefixedTitle = prefixedTitle;
        this.namespace = namespace;
        this.pageId = pageId;
        this.redirect = redirect;
        this.revisionId = revisionId;
        this.parentRevisionId = parentRevisionId;
        this.timeStamp = timeStamp;
        this.contributor = contributor;
        this.contributorId = contributorId;
        this.isMinor = isMinor;
        this.comment = comment;
        this.model = model;
        this.format = format;
        this.text = text;
        this.sha1 = sha1;
    }

    @Override
    public String getPrefixedTitle() {
        return prefixedTitle;
    }

    @Override
    public String getTitle() {
        if (namespace == 0) return prefixedTitle;
        return prefixedTitle.substring(prefixedTitle.indexOf(":") + 1);
    }

    @Override
    public int getNamespace() {
        return namespace;
    }

    @Override
    public int getPageId() {
        return pageId;
    }

    @Override
    public long getRevisionId() {
        return revisionId;
    }

    @Override
    public long getParentRevisionId() {
        return parentRevisionId.orElse(-1L);
    }

    @Override
    public String getTimeStamp() {
        return timeStamp;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public String getModel() {
        return model;
    }

    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public String getComment() {
        return comment.orElse(null);
    }

    @Override
    public String getContributor() {
        return contributor.orElse(null);
    }

    @Override
    public int getContributorId() {
        return contributorId.orElse(-1);
    }

    @Override
    public boolean hasRegisteredContributor() {
        return contributorId.isPresent();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FullRevision.class.getSimpleName() + "[", "]")
                .add("prefixedTitle='" + prefixedTitle + "'")
                .add("namespace=" + namespace)
                .add("pageId=" + pageId)
                .add("redirect=" + redirect)
                .add("revisionId=" + revisionId)
                .add("parentRevisionId=" + parentRevisionId)
                .add("timeStamp='" + timeStamp + "'")
                .add("contributor=" + contributor)
                .add("contributorId=" + contributorId)
                .add("isMinor=" + isMinor)
                .add("comment=" + comment)
                .add("model='" + model + "'")
                .add("format='" + format + "'")
                .add("text='" + text + "'")
                .add("sha1=" + sha1)
                .toString();
    }
}

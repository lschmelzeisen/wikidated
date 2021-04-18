package com.lschmelzeisen.kgevolve;

import org.wikidata.wdtk.dumpfiles.MwRevision;

import java.util.Optional;

/**
 * Similar to {@link org.wikidata.wdtk.dumpfiles.MwRevisionImpl} but carries
 * more information.
 * <p>
 * In particular, this one has information about redirects, whether a revision
 * is a minor revision, and the revisions sha1.
 */
public record FullRevision(String prefixedTitle,
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
                           Optional<String> sha1) implements MwRevision {
    @Override
    public String getPrefixedTitle() {
        return prefixedTitle;
    }

    @Override
    public String getTitle() {
        if (namespace == 0)
            return prefixedTitle;
        else
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
}

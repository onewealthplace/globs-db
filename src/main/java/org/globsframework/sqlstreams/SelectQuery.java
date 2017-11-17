package org.globsframework.sqlstreams;

import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.streams.GlobStream;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;

import java.util.stream.Stream;

public interface SelectQuery {
    Stream<?> executeAsStream();

    GlobStream execute();

    GlobList executeAsGlobs();

    Glob executeUnique() throws ItemNotFound, TooManyItems;

    void close();
}

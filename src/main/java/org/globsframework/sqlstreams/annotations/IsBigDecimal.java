package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.model.Key;

public class IsBigDecimal {
    public static GlobType TYPE;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(IsBigDecimal.class).load();
    }

}

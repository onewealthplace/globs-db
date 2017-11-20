package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.BooleanField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Key;

public class IsDbRef {
    public static GlobType TYPE;

    public static BooleanField IS_DB_REF;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(IsDbRef.class, "IsDbRef").load();
    }

}

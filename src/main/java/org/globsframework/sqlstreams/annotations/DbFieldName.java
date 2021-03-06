package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Key;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.annotations.typed.TypedDbFieldName;

public class DbFieldName {
    public static GlobType TYPE;

    public static StringField NAME;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbFieldName.class, "DbFieldName")
              .register(GlobCreateFromAnnotation.class, annotation -> create((TypedDbFieldName) annotation))
              .load();
    }

    private static MutableGlob create(TypedDbFieldName annotation) {
        return TYPE.instantiate().set(NAME, annotation.value());
    }
}

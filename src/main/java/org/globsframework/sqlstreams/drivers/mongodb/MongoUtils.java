package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.async.client.ListIndexesIterable;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.index.Index;
import org.globsframework.metamodel.index.impl.IsUniqueIndexVisitor;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.annotations.DbFieldName;
import org.globsframework.sqlstreams.annotations.IsDbRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MongoUtils {
    private static Logger LOGGER = LoggerFactory.getLogger(MongoUtils.class);

    public static void createIndexIfNeeded(MongoCollection<Document> collection, Collection<Index> indices) {
        List<Document> documents = new ArrayList<>();
        ListIndexesIterable<Document> documentListIndexesIterable = collection.listIndexes();
        CompletableFuture<Throwable> future = new CompletableFuture<>();
        documentListIndexesIterable.into(documents, (result, t) -> {
            for (Index index : indices) {
                findOrCreateIndex(collection, index, documents);
            }
            future.complete(t);
        });
        Throwable throwable = null;
        try {
            throwable = future.get();
        } catch (Exception e) {
            LOGGER.error("timeout", e);
        }
        if (throwable != null) {
            LOGGER.error("while creating index ", throwable);
        }
    }

    private static void findOrCreateIndex(MongoCollection<Document> collection, Index functionalIndex, List<Document> documents) {
        for (Document document : documents) {
            if (contain(functionalIndex, document)) {
                return;
            }
        }
        LOGGER.info("create index " + functionalIndex.getName());
        Document document = new Document();
        functionalIndex.fields().forEach(field -> document.append(getDbName(field), 1));
        collection.createIndex(document, new IndexOptions()
              .unique(functionalIndex.visit(new IsUniqueIndexVisitor()).isUnique())
              .name(functionalIndex.getName()), (result, t) -> {
            if (t != null) {
                LOGGER.error("Fail to create index " + functionalIndex.getName(), t);
            }
        });
    }

    protected static boolean contain(Index functionalIndex, Document document) {
        Document key = document.get("key", Document.class);
        String name = document.getString("name");
        return name.equals(functionalIndex.getName()) && functionalIndex.fields()
              .allMatch(field -> key.containsKey(getDbName(field)));
    }

    public static String getDbName(Field field) {
        Glob name = field.findAnnotation(DbFieldName.KEY);
        if (name != null) {
            if (field.hasAnnotation(IsDbRef.KEY)) {
                return name.get(DbFieldName.NAME) + ".$id";
            }
            else {
                return name.get(DbFieldName.NAME);
            }
        }
        return field.getName();
    }
}

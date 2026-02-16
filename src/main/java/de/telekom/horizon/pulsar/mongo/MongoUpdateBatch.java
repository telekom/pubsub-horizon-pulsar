// Copyright 2026 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.pulsar.mongo;

import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import de.telekom.eni.pandora.horizon.model.event.Status;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.mongo.config.Databases;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * @TODO The batch isn't locked, so it can be used by multiple threads.
 * @TODO The batch should also be flushed as soon as no more records are returned by the database.
 */

@Slf4j
public class MongoUpdateBatch {

    private final String subscriptionId;

    private final MongoCollection<Document> collection;

    private final List<UpdateOneModel<Document>> batch;

    public MongoUpdateBatch(String subscriptionId, MongoCollection<Document> collection) {
        this.subscriptionId = subscriptionId;
        this.collection = collection;
        this.batch = new ArrayList<>();
    }

    public void updateStatus(SubscriptionEventMessage eventMessage, Status status) {
        updateStatus(eventMessage, status, null);
    }

    public void updateStatus(SubscriptionEventMessage eventMessage, Status status, Throwable error) {
        var filterDocument = new Document()
                .append("_id", eventMessage.getUuid())
                .append("event.id", eventMessage.getEvent().getId());

        var updateDocument = new Document("$set", new Document()
                .append("status", status.name())
        );

        if (error != null) {
            updateDocument = updateDocument
                    .append("errorType", error.getClass().getName())
                    .append("errorMessage", error.getMessage());
        }

        batch.add(new UpdateOneModel<>(filterDocument, updateDocument));
        log.debug("Added update to batch: {}", new Document()
                .append("filter", filterDocument)
                .append("update", updateDocument).toJson());
    }

    public void flush() throws MongoException {
        if (batch.isEmpty()) return;

        Instant start = Instant.now();
        BulkWriteResult result = collection.bulkWrite(batch);

        var latency = start.until(Instant.now(), ChronoUnit.MILLIS);
        log.debug("Updated {} of {} documents of subscription {} in {}ms", result.getModifiedCount(), batch.size(), subscriptionId, latency);
        batch.clear();
    }

    public int getSize() {
        return batch.size();
    }

}

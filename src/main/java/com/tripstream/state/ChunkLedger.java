package com.tripstream.state;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

/**
 * DynamoDB-backed exactly-once bookkeeping for chunks.
 *
 * <ul>
 *   <li>Idempotency table: one item per jobId#chunkId. A <i>claim</i> is a conditional put with a lease, so a
 *       crashed worker's claim expires and the redelivered message can take over.</li>
 *   <li>Job progress table: chunksTotal (set by the splitter) and chunksDone (atomic counter).</li>
 *   <li>{@link #completeChunk} flips the idempotency item to DONE <b>and</b> increments chunksDone in a single
 *       transaction. The increment can therefore happen at most once per chunk, however many times the message is
 *       delivered or the chunk is reprocessed.</li>
 * </ul>
 */
@Component
public class ChunkLedger {

    public enum ClaimResult { CLAIMED, ALREADY_DONE, BUSY }

    public record Claim(ClaimResult result, String owner) {
    }

    public record Progress(Long chunksTotal, long chunksDone) {
        public boolean allDone() {
            return chunksTotal != null && chunksDone >= chunksTotal;
        }
    }

    private static final Duration ITEM_TTL = Duration.ofDays(7);

    private final DynamoDbClient ddb;
    private final String idempotencyTable;
    private final String progressTable;

    public ChunkLedger(DynamoDbClient ddb,
            @Value("${tripstream.tables.idempotency}") String idempotencyTable,
            @Value("${tripstream.tables.job-progress}") String progressTable) {
        this.ddb = ddb;
        this.idempotencyTable = idempotencyTable;
        this.progressTable = progressTable;
    }

    public Claim claim(String pk, Duration lease) {
        String owner = UUID.randomUUID().toString();
        long now = Instant.now().getEpochSecond();
        try {
            ddb.putItem(b -> b.tableName(idempotencyTable)
                    .item(Map.of(
                            "pk", s(pk),
                            "status", s("IN_PROGRESS"),
                            "owner", s(owner),
                            "leaseExpiresAt", n(now + lease.toSeconds()),
                            "ttl", n(now + ITEM_TTL.toSeconds())))
                    .conditionExpression("attribute_not_exists(pk) OR (#s = :inprog AND leaseExpiresAt < :now)")
                    .expressionAttributeNames(Map.of("#s", "status"))
                    .expressionAttributeValues(Map.of(":inprog", s("IN_PROGRESS"), ":now", n(now))));
            return new Claim(ClaimResult.CLAIMED, owner);
        } catch (ConditionalCheckFailedException e) {
            var item = ddb.getItem(b -> b.tableName(idempotencyTable).key(Map.of("pk", s(pk))).consistentRead(true))
                    .item();
            boolean done = item.containsKey("status") && "DONE".equals(item.get("status").s());
            return new Claim(done ? ClaimResult.ALREADY_DONE : ClaimResult.BUSY, null);
        }
    }

    /** Heartbeat: pushes the lease out while we are still working. False if we no longer own the claim. */
    public boolean renewLease(String pk, String owner, Duration lease) {
        long expires = Instant.now().getEpochSecond() + lease.toSeconds();
        try {
            ddb.updateItem(b -> b.tableName(idempotencyTable).key(Map.of("pk", s(pk)))
                    .updateExpression("SET leaseExpiresAt = :exp")
                    .conditionExpression("#s = :inprog AND #o = :me")
                    .expressionAttributeNames(Map.of("#s", "status", "#o", "owner"))
                    .expressionAttributeValues(Map.of(":exp", n(expires), ":inprog", s("IN_PROGRESS"),
                            ":me", s(owner))));
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    /** Gives up our claim after a failure so the retry does not have to wait for the lease to expire. */
    public void release(String pk, String owner) {
        try {
            ddb.deleteItem(b -> b.tableName(idempotencyTable).key(Map.of("pk", s(pk)))
                    .conditionExpression("#s = :inprog AND #o = :me")
                    .expressionAttributeNames(Map.of("#s", "status", "#o", "owner"))
                    .expressionAttributeValues(Map.of(":inprog", s("IN_PROGRESS"), ":me", s(owner))));
        } catch (ConditionalCheckFailedException ignored) {
            // lease already expired and someone else owns it, or it is DONE: nothing to release
        }
    }

    /**
     * Marks the chunk DONE and increments chunksDone atomically.
     * @return true if this call completed the chunk, false if it was already DONE (nothing was counted)
     */
    public boolean completeChunk(String pk, UUID jobId) {
        try {
            ddb.transactWriteItems(b -> b.transactItems(
                    TransactWriteItem.builder().update(Update.builder()
                            .tableName(idempotencyTable).key(Map.of("pk", s(pk)))
                            .updateExpression("SET #s = :done")
                            .conditionExpression("attribute_exists(pk) AND #s <> :done")
                            .expressionAttributeNames(Map.of("#s", "status"))
                            .expressionAttributeValues(Map.of(":done", s("DONE")))
                            .build()).build(),
                    TransactWriteItem.builder().update(Update.builder()
                            .tableName(progressTable).key(Map.of("jobId", s(jobId.toString())))
                            .updateExpression("ADD chunksDone :one")
                            .expressionAttributeValues(Map.of(":one", n(1)))
                            .build()).build()));
            return true;
        } catch (TransactionCanceledException e) {
            var item = ddb.getItem(b -> b.tableName(idempotencyTable).key(Map.of("pk", s(pk)))
                    .consistentRead(true)).item();
            if (item.containsKey("status") && "DONE".equals(item.get("status").s())) {
                return false; // another delivery finished it first
            }
            throw e;
        }
    }

    /** Generic claim/finish for non-chunk work (e.g. notifications): marks the claimed item DONE. */
    public void markDone(String pk) {
        ddb.updateItem(b -> b.tableName(idempotencyTable).key(Map.of("pk", s(pk)))
                .updateExpression("SET #s = :done")
                .expressionAttributeNames(Map.of("#s", "status"))
                .expressionAttributeValues(Map.of(":done", s("DONE"))));
    }

    /** Idempotent: sets the total and makes sure chunksDone exists (ADD 0). */
    public void setTotal(UUID jobId, long chunksTotal) {
        ddb.updateItem(b -> b.tableName(progressTable).key(Map.of("jobId", s(jobId.toString())))
                .updateExpression("SET chunksTotal = :t ADD chunksDone :zero")
                .expressionAttributeValues(Map.of(":t", n(chunksTotal), ":zero", n(0))));
    }

    public Optional<Progress> progress(UUID jobId) {
        var item = ddb.getItem(b -> b.tableName(progressTable).key(Map.of("jobId", s(jobId.toString())))
                .consistentRead(true)).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        Long total = item.containsKey("chunksTotal") ? Long.valueOf(item.get("chunksTotal").n()) : null;
        long done = item.containsKey("chunksDone") ? Long.parseLong(item.get("chunksDone").n()) : 0;
        return Optional.of(new Progress(total, done));
    }

    private static AttributeValue s(String v) {
        return AttributeValue.builder().s(v).build();
    }

    private static AttributeValue n(long v) {
        return AttributeValue.builder().n(Long.toString(v)).build();
    }
}

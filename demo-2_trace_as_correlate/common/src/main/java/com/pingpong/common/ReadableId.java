package com.pingpong.common;

import java.util.UUID;

/**
 * Generates the human-readable business ids described in {@code distributed_tracing.md} §6.3:
 * {@code [origin-service].[aggregate-type].[unique-slug]}. Infrastructure ids (trace/span) stay
 * W3C-pure; these business ids make the originating domain obvious in any log or span.
 */
public final class ReadableId {

    private ReadableId() {
    }

    /** e.g. {@code service-ping.ping.a1b2c3d4}. */
    public static String create(String originService, String aggregateType) {
        return originService + "." + aggregateType + "." + short_slug();
    }

    private static String short_slug() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }
}

package com.example.usage.admin;

import java.util.List;

/** One page of a listing. {@code hasNext} instead of a total: counting a large ledger on every request is the expensive part. */
public record Page<T>(List<T> items, int page, int size, boolean hasNext) {
    static final int MAX_SIZE = 200;
    /** Deep OFFSET scans every row it skips: past this, narrow the filters (or ask for a time range) instead. */
    static final long MAX_OFFSET = 100_000;

    /** Validated page request; {@code size} is capped so a client cannot ask for the whole table. */
    record Request(int page, int size) {
        Request {
            if (page < 0) throw AdminException.invalid("INVALID_PAGE", "page must be >= 0");
            if (size < 1 || size > MAX_SIZE) throw AdminException.invalid("INVALID_PAGE", "size must be in 1.." + MAX_SIZE);
            if ((long) page * size > MAX_OFFSET) throw AdminException.invalid("INVALID_PAGE", "page * size must not pass " + MAX_OFFSET + ": narrow the filters");
        }

        long offset() { return (long) page * size; }
    }

    static <T> Page<T> of(List<T> fetchedOneExtra, Request r) {
        boolean more = fetchedOneExtra.size() > r.size();
        return new Page<>(more ? fetchedOneExtra.subList(0, r.size()) : fetchedOneExtra, r.page(), r.size(), more);
    }
}

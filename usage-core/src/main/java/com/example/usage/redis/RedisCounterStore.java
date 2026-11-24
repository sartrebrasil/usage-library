package com.example.usage.redis;

import com.example.usage.core.spi.CounterStore;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * CounterStore on Redis. Each operation is one Lua script, so read-check-write is atomic across
 * all gateway and processor instances. Redis is a decision cache: the ledger is the source of truth.
 * <p>
 * Counters are clamped at 2^53 (Lua numbers are doubles, exact up to there) and written with %.0f so the
 * value is always a plain integer string, never exponent notation. Every call renews the ttl. In Redis
 * Cluster every operation touches a single key, so no hash tag is needed here.
 * shortcut: EVAL sends the script text each call; switch to EVALSHA if the script grows.
 */
public final class RedisCounterStore implements CounterStore {
    private static final String ADD = """
            local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
            local delta = tonumber(ARGV[1])
            if delta > 0 and cur + delta > tonumber(ARGV[2]) then return {0, cur} end
            local nxt = cur + delta
            if nxt < 0 then nxt = 0 end
            if nxt > 9007199254740992 then nxt = 9007199254740992 end
            local value = string.format('%.0f', nxt)
            redis.call('SET', KEYS[1], value, 'PX', ARGV[3])
            return {1, nxt}
            """;

    // Per-holder leases. KEYS[1] = zset holder -> expiry (ms), KEYS[2] = hash holder -> weight. Time is Redis's own
    // clock, so application servers with skewed clocks cannot expire each other's slots. Both keys carry the same
    // {hash tag}, so they live in one Redis Cluster slot.
    private static final String NOW_MS = """
            local t = redis.call('TIME')
            local now = t[1] * 1000 + math.floor(t[2] / 1000)
            """;

    private static final String ACQUIRE_LEASE = NOW_MS + """
            local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
            for _, holder in ipairs(expired) do
                redis.call('ZREM', KEYS[1], holder)
                redis.call('HDEL', KEYS[2], holder)
            end
            redis.call('ZREM', KEYS[1], ARGV[1])   -- re-acquiring replaces the holder's own slot
            redis.call('HDEL', KEYS[2], ARGV[1])
            local used = 0
            for _, w in ipairs(redis.call('HVALS', KEYS[2])) do used = used + tonumber(w) end
            local weight = tonumber(ARGV[2])
            if weight > tonumber(ARGV[3]) or used > tonumber(ARGV[3]) - weight then return {0, used} end
            redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), ARGV[1])
            redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
            redis.call('PEXPIRE', KEYS[2], ARGV[4])
            return {1, used + weight}
            """;

    private static final String RELEASE_LEASE = """
            redis.call('ZREM', KEYS[1], ARGV[1])
            redis.call('HDEL', KEYS[2], ARGV[1])
            return 1
            """;

    private static final String LEASES_IN_USE = NOW_MS + """
            local used = 0
            for _, holder in ipairs(redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. now, '+inf')) do
                used = used + tonumber(redis.call('HGET', KEYS[2], holder) or '0')
            end
            return used
            """;

    private final RedisAsyncCommands<String, String> redis;

    public RedisCounterStore(RedisAsyncCommands<String, String> redis) {
        this.redis = redis;
    }

    @Override
    public CompletionStage<Result> add(String key, long delta, long cap, Duration ttl) {
        return redis.<List<Object>>eval(ADD, ScriptOutputType.MULTI, new String[]{key},
                        Long.toString(delta), Long.toString(cap), Long.toString(Math.max(1, ttl.toMillis())))
                .thenApply(r -> new Result(((Long) r.get(0)) == 1L, (Long) r.get(1)));
    }

    private static String[] leaseKeys(String key) {
        return new String[]{"{" + key + "}:z", "{" + key + "}:w"};
    }

    @Override
    public CompletionStage<Lease> acquireLease(String key, String holder, long weight, long cap, Duration lease) {
        return redis.<List<Object>>eval(ACQUIRE_LEASE, ScriptOutputType.MULTI, leaseKeys(key),
                        holder, Long.toString(weight), Long.toString(cap), Long.toString(Math.max(1, lease.toMillis())))
                .thenApply(r -> new Lease(((Long) r.get(0)) == 1L, (Long) r.get(1)));
    }

    @Override
    public CompletionStage<Void> releaseLease(String key, String holder) {
        return redis.<Long>eval(RELEASE_LEASE, ScriptOutputType.INTEGER, leaseKeys(key), holder).thenApply(x -> (Void) null);
    }

    @Override
    public CompletionStage<Long> leasesInUse(String key) {
        return redis.<Long>eval(LEASES_IN_USE, ScriptOutputType.INTEGER, leaseKeys(key));
    }

    @Override
    public CompletionStage<Long> get(String key) {
        return redis.get(key).thenApply(v -> v == null ? 0L : Long.parseLong(v));
    }
}

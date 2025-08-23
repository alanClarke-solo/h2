Here’s a clear, high-level explanation of the logic and how the classes in ac.h2 work together.

Overview: a 3-level hierarchical cache
- L1: Local in-memory cache (fastest, process-local).
- L2: Redis (remote, shared across instances).
- L3: Database (persistent fallback and searchable by parameters).
- Keys can be addressed in multiple ways:
    - stringKey (e.g., “user:profile”)
    - longKey (numeric ID)
    - parameters (a hierarchical list that supports partial matches)
- A unique cache identity is formed by combining stringKey + optional longKey. Parameters are indexed separately for lookup.

Core data model
- CachedItem<T>
    - Holds the payload (value), addressing info (stringKey, longKey), parameters, createdAt, TTL.
    - Knows if it is expired and can generate a unique ID from key parts.
- SearchParameter
    - A single search criterion with name, value, and a level (int).
    - Comparable by level so lists can be ordered.
    - toKey() yields a canonical token like L2:status=ACTIVE. These tokens are used to build hierarchical search patterns and indexes.

Configuration and behavior flags
- CacheConfiguration
    - Controls TTLs for local (L1), remote (L2), and DB (L3).
    - Enables/disables each level.
    - Read-through: if enabled, a miss on L1 will try L2/L3 (based on FallbackStrategy).
    - Write-through: if enabled, writes/updates propagate to remote stores.
    - FallbackStrategy controls the read order and which tiers are consulted:
        - REDIS_ONLY
        - DATABASE_ONLY
        - REDIS_THEN_DATABASE
        - DATABASE_THEN_REDIS
    - You can also enable database-backed caching with JDBC details and a separate DB TTL.

Statistics and observability
- CacheStatistics
    - Tracks hit/miss counts globally and per level (L1, L2, L3).
    - Tracks puts, evictions, and error counters for remote tiers.
    - Computes hit rates and can be reset.
    - Useful to verify whether your chosen fallback strategy and TTLs are effective.

Database tier (L3): storage, lookup, and parameter indexing
- DatabaseCacheProvider<T>
    - Manages an Oracle-backed cache table with:
        - unique_string_id (computed from stringKey + longKey) and columns for key parts, binary value (BLOB), serialized parameters (CLOB), created time, TTL, and expires_at (computed in DB).
    - A separate index table maps parameter patterns to unique_string_id to enable fast reverse lookups by hierarchical parameters.
    - Serialization
        - Values stored as binary (BLOB), typically using Kryo with a per-thread serializer (ThreadLocal) for speed.
        - Parameters serialized (e.g., JSON) and also expanded into normalized pattern strings for indexing.
    - Put
        - Upserts into the cache table (MERGE).
        - Computes expires_at from TTL.
        - Regenerates parameter pattern rows to reflect current parameters.
    - Get
        - By unique_string_id (derived from stringKey + optional longKey).
        - By stringKey only or longKey only.
        - By parameters: expands the parameter list into hierarchical patterns, then queries the parameter index table to retrieve matching unique_string_id rows and then values.
    - Link
        - Binds existing entries to new identifiers (e.g., attach a longKey to a stringKey-based record) without re-serializing the value.
    - Invalidate
        - By unique id or by key parts; removes the cache row and its parameter index rows.
        - Invalidate-all clears both cache and parameter index tables.
    - Get-or-compute
        - If not found (or expired), compute via Supplier, store, return.
    - Statistics
        - Updates L3 counters for hits, misses, puts, errors.

Coordinator service: L1+L2(+L3) orchestration
- TransparentHierarchicalCacheService<T>
    - The façade you’ll use in application code. It ties everything together:
        - Local caches:
            - localPrimaryCache: maps unique_string_id -> CachedItem<T>.
            - localLongKeyCache: maps longKey -> unique_string_id (fast reverse lookup).
            - localParamCache: maps parameter pattern -> set of unique_string_id.
        - Remote providers:
            - Redis cache via a HierarchicalCacheService<T> (L2).
            - DatabaseCacheProvider<T> (L3).
        - Configuration and statistics are injected and used for decisions.
    - Put
        - Creates a CachedItem and places it into L1 (if enabled).
        - If write-through is enabled, writes to Redis and/or DB (async or sync depending on implementation detail).
        - Updates in-memory parameter indexes for efficient local param search (pattern -> unique IDs).
    - Get
        - Fast path: look in L1 by unique ID; check TTL at the item level.
        - If L1 misses and read-through is enabled, consult remote tiers according to FallbackStrategy:
            - For REDIS_THEN_DATABASE:
                - Try Redis. If hit, record L2 hit and backfill L1.
                - Else try DB. If hit, record L3 hit, optionally write-through to L2, and backfill L1.
            - For DATABASE_THEN_REDIS: the reverse order.
            - For REDIS_ONLY or DATABASE_ONLY: consult only that tier.
        - All successes backfill L1 if local caching is enabled.
    - Get by parameters
        - First tries local parameter index: convert the input parameters into hierarchical patterns, then gather matching unique IDs and return values from L1.
        - If not all are in L1 and read-through is enabled, queries remote tiers for parameter-based results and then caches locally.
    - Get-or-compute
        - If not found in the chosen tiers, calls the supplied Supplier to compute the value(s), then writes through to configured tiers and fills L1.
    - Link
        - Associates an existing cache entry with a new key (string or long) or with parameter lists, updating all indexes locally and remotely.
    - Invalidate
        - Can invalidate by stringKey, longKey, or the composite pair.
        - Removes from L1 primary cache, local reverse mappings, and parameter indexes.
        - Also propagates invalidation to Redis and DB if configured.
    - Stats
        - Updates L1/L2/L3 hit/miss/put/eviction counters as operations occur.
    - TTL handling
        - L1 honors the item’s TTL. Redis and DB TTLs are handled by those tiers using their respective expiry mechanisms.

How hierarchical parameter matching works
- Each SearchParameter contributes a normalized token (e.g., L1:category=Books).
- A list of parameters is turned into a set of hierarchical “patterns” (prefixes), for example:
    - [L1:category=Books, L2:status=ACTIVE] might generate:
        - L1:category=Books
        - L1:category=Books|L2:status=ACTIVE
- These patterns are:
    - Indexed in DB (parameter index table) for fast reverse lookup.
    - Cached locally in localParamCache (pattern -> set of unique IDs).
- Searching by parameters uses these patterns to find candidate items quickly, supporting partial matches by level order.

Typical request flows
- Put(key, id, params, value)
    1) Store in L1 with TTL.
    2) Update local indexes (longKey mapping, param patterns).
    3) If write-through enabled:
        - Write to Redis and/or DB with their own TTL.
        - Update parameter index table in DB for param queries.

- Get(key, id)
    1) Check L1 by unique ID (L1 hit).
    2) If miss and read-through enabled:
        - Try L2 or L3 according to FallbackStrategy.
        - On hit, backfill L1 and update stats.
    3) Return Optional<T>.

- Get(params)
    1) Convert params to patterns and try localParamCache to resolve candidate unique IDs.
    2) Fetch any found values from L1.
    3) If incomplete and read-through enabled:
        - Query Redis/DB by params.
        - Backfill L1 and update local param indexes.

- Invalidate(key/id)
    1) Remove from L1, longKey map, and param indexes.
    2) Propagate invalidation to Redis and DB if enabled.

When to adjust configuration
- Use REDIS_THEN_DATABASE if Redis is reliable/cheap and DB is the final fallback.
- Use DATABASE_THEN_REDIS if DB is more authoritative and Redis is optional.
- Disable read-through if you only want L1 hits and don’t want remote lookups on misses.
- Disable write-through if you only want L1-cached copies without mutating shared stores.
- Tune local TTL and max size to manage memory and eviction pressure.

Thread safety and performance notes
- L1 typically uses a high-performance cache (e.g., with size bounds and eviction metrics).
- Stats use AtomicLong to avoid locks.
- DB serialization uses a thread-local serializer to reduce allocation and contention.
- Parameter indexing ensures parameter-based queries scale efficiently.

In short, ac.h2 provides a robust, transparent multi-level cache with:
- Multiple addressing modes (string, long, parameters).
- Hierarchical parameter matching and indexing.
- Configurable read/write-through and fallback.
- Comprehensive statistics for each cache layer.
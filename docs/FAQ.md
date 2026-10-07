この文書を日本語で[Open Match 2 よくある質問とチューニングガイド](jp/FAQ.md)

# Open Match \- Frequently Asked Questions & Tuning Guide

Here are answers to common questions regarding the tuning, configuration, and best practices for Open Match.

#### **Tuning & Performance**

**Q: How do I know if my Open Match core instances are keeping up with state replication, and how do I tune the replication waits?**

A: Start by monitoring `om_core.cache.replication.lag` (the time in milliseconds between when an update is written to state storage and when it is applied to the local cache). By default (`OM_CACHE_PACK_TICKET_STATE_UPDATES=false`), each ticket state change (`CreateTicket`, each ticket in `ActivateTickets` or `DeactivateTickets`, and each ticket deactivated in a returned `Match`) is written as a separate entry in the replication stream. Setting `OM_CACHE_PACK_TICKET_STATE_UPDATES=true` packs all ticket activations or deactivations from a single call or returned `Match` into one update request, and coalesces contiguous same-command (`Activate` or `Deactivate`) requests in the outgoing batch queue (up to `OM_MAX_STATE_UPDATES_PER_CALL` ticket IDs per stream entry) while preserving strict causal order across interleaved commands. Because packed entries use a multi-field Redis Stream format, ensure all `om-core` instances reading from the same Redis instance are upgraded to a version that supports packed updates before enabling `OM_CACHE_PACK_TICKET_STATE_UPDATES=true`.

If `om_core.cache.replication.lag` is climbing or you see `Timeout while waiting for ticket ... deactivation to be replicated to local cache` errors (tracked by `om_core.mmf.deactivation.timeouts` and `om_core.mmf.deactivation.wait`), compare the following metrics to pinpoint the bottleneck and see the effect of your tuning:

1. **Updates are queued in Redis because poller waits are too long**:
   * **Symptoms**: `om_core.cache.replication.lag` is climbing while `om_core.cache.incoming.backlog` is low, `om_core.cache.incoming.polls.full` is incrementing (when polls hit `OM_CACHE_IN_MAX_UPDATES_PER_POLL`), and `om_core.cache.incoming.poll.wait` is much larger than `om_core.cache.incoming.poll.duration`.
   * **How to tune**: Open Match separates incoming poller waits into three settings:
     * `OM_CACHE_IN_WAIT_TIMEOUT_MS` (default `1500` ms): how long `GetUpdates()` blocks waiting for an update when the replication stream is empty (`XREAD BLOCK`).
     * `OM_CACHE_IN_POLL_WAIT_MS` (default `250` ms): the post-poll pause after a non-empty partial poll (`0 < len(results) < OM_CACHE_IN_MAX_UPDATES_PER_POLL`) so trickling updates coalesce into batches in state storage without stalling for the full idle timeout.
     * `OM_CACHE_IN_FULL_POLL_WAIT_MS` (default `100` ms): the shorter post-poll pause after a full poll (`len(results) >= OM_CACHE_IN_MAX_UPDATES_PER_POLL`) to yield CPU while draining a backlog.

     If `om_core.cache.incoming.poll.wait` is holding back replication throughput, enable `OM_CACHE_PACK_TICKET_STATE_UPDATES=true` to pack and coalesce multi-ticket activations/deactivations into fewer stream entries. You can also lower `OM_CACHE_IN_POLL_WAIT_MS` (when polls return partial batches), lower `OM_CACHE_IN_FULL_POLL_WAIT_MS` (when `om_core.cache.incoming.polls.full` is incrementing), or increase `OM_CACHE_IN_MAX_UPDATES_PER_POLL`.
2. **Updates are queued in `om-core` memory because applier sleeps are too long**:
   * **Symptoms**: `om_core.cache.replication.lag` is climbing, `om_core.cache.incoming.backlog` is high (and `om_core.cache.incoming.queue.wait` increases as the poller waits for room in `OM_CACHE_IN_QUEUE_BUFFER_SIZE`), and `om_core.cache.incoming.timeouts.full` is incrementing while `om_core.cache.incoming.apply.sleep` is high relative to `om_core.cache.incoming.apply.duration`.
   * **How to tune**: Open Match controls local cache apply cycles with three settings:
     * `OM_CACHE_IN_MAX_APPLY_DURATION_MS` (default `500` ms): the maximum time spent applying buffered updates to the local cache in a single cycle before yielding.
     * `OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS` (default `500` ms): the sleep between apply cycles when the previous cycle drained all buffered updates.
     * `OM_CACHE_IN_FULL_APPLY_SLEEP_MS` (default `100` ms): the shorter yield sleep after an apply cycle is stopped early by `OM_CACHE_IN_MAX_APPLY_DURATION_MS` with updates still pending.

     To drain buffered updates faster, lower `OM_CACHE_IN_FULL_APPLY_SLEEP_MS` (when `om_core.cache.incoming.timeouts.full` is incrementing) or `OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS`, or increase `OM_CACHE_IN_MAX_APPLY_DURATION_MS`.
3. **Outgoing updates are waiting too long before being written to state storage**:
   * **Symptoms**: `om_core.cache.outgoing.queue.wait` is high relative to `om_core.cache.outgoing.send.duration`, or `om_core.cache.outgoing.backlog` is elevated.
   * **How to tune**: If `om_core.cache.outgoing.timeouts` is high under moderate traffic, lower `OM_CACHE_OUT_WAIT_TIMEOUT_MS` or `OM_CACHE_OUT_MAX_QUEUE_THRESHOLD` so batches flush sooner. Under heavy burst traffic, enable `OM_CACHE_PACK_TICKET_STATE_UPDATES=true` so contiguous `Activate`/`Deactivate` requests across concurrent callers are coalesced into fewer Redis stream entries, and adjust `OM_CACHE_OUT_QUEUE_BUFFER_SIZE` if callers block enqueuing requests.
4. **Reading/applying updates is starving matchmaking or ticket update work**:
   * **Symptoms**: `om_core.cache.incoming.poll.wait` and `om_core.cache.incoming.apply.sleep` are very small relative to `poll.duration` and `apply.duration`, while `om_core.mmf.prep.duration` (local ticket snapshotting and pool filtering before calling MMFs) or `om_core.cache.outgoing.queue.wait` (time ticket updates spend waiting to be batched and sent, compared to `om_core.cache.outgoing.send.duration`) begins to spike.
   * **How to tune**: Increase `OM_CACHE_IN_POLL_WAIT_MS`, `OM_CACHE_IN_FULL_POLL_WAIT_MS`, or `OM_CACHE_IN_FULL_APPLY_SLEEP_MS`, or lower `OM_CACHE_IN_MAX_APPLY_DURATION_MS`, to guarantee `om-core` yields enough time to matchmaking and outgoing ticket updates.

**Q: Should I wait for ticket deactivation before returning a match (`OM_MATCH_TICKET_DEACTIVATION_WAIT`)?**

A: This setting controls a trade-off between speed and consistency.

* **`true` (Default & Recommended)**: Open Match validates all ticket IDs in a returned match's rosters and waits (up to `OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS`) to confirm that all valid tickets in the match have been marked as `inactive` in its local cache before returning the match to your Director. This is safer and significantly reduces the chance that the same ticket will be proposed in another match by a different MMF. If waiting for replication times out before the match is returned (or if all ticket deactivation writes for the match fail to produce a replication ID), Open Match increments `om_core.mmf.deactivation.timeouts` and annotates the returned `Match` with `extensions["deactivation_timeout"]` (a `google.protobuf.BoolValue` set to `true`) so your Director can optionally detect that local replication was not confirmed when the match was streamed back.
* **`false`**: Open Match returns the match immediately and *then* attempts to deactivate the tickets. This is faster but less safe. It's possible that before the deactivation is replicated, another matchmaking cycle could begin and propose the same tickets in a new match.

**Q: What happens if my matchmaker cancels the context during `InvokeMatchmakingFunctions`?**

A: Cancelling the context puts the system into an unknown state and should be avoided. The `InvokeMatchmakingFunctions` call may have already sent matches back to your Director, and the deactivation of tickets for those matches might be in progress. If you do cancel and need to recover, you are responsible for manually activating or deactivating tickets as needed to restore the system to a state your matchmaker expects.

---

#### **State Storage (Redis)**

**Q: Do I need Redis persistence and clustering? What happens if my Redis instance goes down?**

A: While Redis persistence (like AOF or RDB) is an option, you should carefully consider if it aligns with your game's needs. If your primary Redis instance goes down, a typical failover can take up to 30 seconds. In that time, most of the matchmaking tickets would have become stale anyway. For many games, it's a better player experience to have clients simply retry their matchmaking request after a short delay rather than waiting for a Redis failover and resuming with potentially old data.

**Q: Can I configure Open Match to use multiple Redis instances for scaling?**

A: Yes and no.

* **Read Replicas (Yes)**: Open Match has built-in support for using Redis read replicas. You can configure a separate host for read operations (`OM_REDIS_READ_HOST`) and another for write operations (`OM_REDIS_WRITE_HOST`). This is a common and effective way to scale read-heavy workloads.  
* **Sharding (No)**: Open Match does not support Redis sharding. It assumes that the single write instance can accept writes for any key, and that all read instances can see all the data that was written.

---

#### **Configuration & Best Practices**

**Q: Why doesn't the OpenTelemetry connection use TLS by default?**

A: The default configuration assumes Open Match core and the OpenTelemetry collector are running as sidecars within the same secure execution context (like a Kubernetes Pod or a Cloud Run service). In this environment, traffic does not leave the secure boundary, and TLS is not typically necessary. If your deployment requires it, you are responsible for securing this connection.

**Q: How should I handle "backfill" profiles versus regular profiles?**

A: If you have profiles designed to fill partially-empty, running game servers, you should generally invoke the MMFs for these profiles *before* you invoke the MMFs for creating brand-new matches. This is because regular profiles can drain the pool of available players very quickly. It is your Director's responsibility to decide the order of MMF invocations to best serve your game's logic.

**Q: My ticket expired immediately after I created it. What happened?**

A: Open Match always calculates a ticket's expiration time based on its `creation_time`. If you manually provide a `creation_time` that is very old (e.g., the Unix epoch of January 1st, 1970\) and the default TTL is 10 minutes, the ticket will be considered expired the moment it is created.

**Q: Can I just set a really long TTL to prevent tickets from expiring? How should I calculate the correct value for `OM_CACHE_TICKET_TTL_MS`?**

A: Do not use this setting to handle edge cases or "forever" queues. A TTL that is too long will cause successfully matched (inactive) tickets to accumulate in the system memory until their TTL expires, which increases load and memory pressure. You should set this value to be slightly longer than the time it takes to find a match for 95% of your player base (assuming a normal distribution), but no longer.

Open Match handles `OM_CACHE_TICKET_TTL_MS` expiration across two layers:

* **Local cache expiration (`ReplicatedTicketCache`)**: Each `om-core` instance enforces exact per-ticket, inactive-state, and assignment deadlines using local min-heaps ordered by expiration time. Every `OM_CACHE_EXPIRATION_INTERVAL_MS` (default `1000` ms), `om-core` pops only the entries that have actually expired. To prevent mass-expiration bursts from delaying incoming replication updates, each cycle deletes at most `OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE` entries (default `5000`, processed in small lock chunks) and leaves any remaining expired entries for subsequent cycles. You can monitor local expiration activity via `om_core.cache.expiration.duration`, `om_core.cache.ticket.expirations`, and `om_core.cache.ticket.inactive.expirations`.
* **Redis stream trimming (`om-replication`)**: On each outgoing write batch, `om-core` appends `XTRIM om-replication MINID ~ <threshold>` (where `<threshold>` is `now - OM_CACHE_TICKET_TTL_MS`). Using approximate trimming (`~`) lets Redis evict whole radix-tree macro-nodes in O(1) time without per-entry rewrite overhead, while `om-core`'s local min-heaps still guarantee exact per-ticket expiration.

A practical example of how to derive `OM_CACHE_TICKET_TTL_MS`:

1. Analyze your matchmaking data: Determine how long it takes to find a match for the vast majority of your players. For example, if your player skill distribution is normal, you might find that 95% of players are matched within 5 minutes.
1. Define your "Hard Deadline": Decide on the maximum time a player should wait before your game client takes drastic action (like restarting the search with wider parameters or telling the player to try again later). Let's say this is 7 minutes.
1. Set the TTL: Set `OM_CACHE_TICKET_TTL_MS` to slightly more than that hard deadline (e.g., 7 minutes and 30 seconds).

Any player waiting longer than 7 minutes is an edge case. You should handle them using the [Long-Lived Tickets pattern from the Advanced topics documentation](ADVANCED.md#handling-long-lived-tickets): detect the approaching expiration on the client/frontend, and transparently re-create a new ticket with high-priority flags. This keeps the Open Match core lean and performant while still supporting long wait times for specific players.

**Q: Why doesn't `InvokeMatchmakingFunctions` tell me how many tickets were in each pool?**

A: Open Match assumes that if this information is important to you, your MMF will be responsible for tracking it. Your MMF can count the tickets in the pools it receives and include this data in the `extensions` field of the `Match` object it returns.

**Q: Why should I use reverse-DNS notation for profile names?**

A: This is a best practice for managing metric cardinality. Telemetry systems can be overwhelmed if they receive too many unique labels. By using a format like `com.mygame.gamemode.ranked-4v4.us-west1.2025-09-2718:00:00.000`, monitoring tools can easily aggregate metrics by stripping the most specific parts of the name (e.g., aggregating all metrics under `com.mygame.gamemode.ranked-4v4.us-west1`). For a deeper dive on this topic, [the Grafana blog has an excellent article on cardinality spikes](https://grafana.com/blog/2022/02/15/what-are-cardinality-spikes-and-why-do-they-matter/).

**Can I prevent Open Match from deactivating tickets in a match returned by my MMF? I want to evaluate the match quality in my Director first.**

A: No, you cannot disable this behavior directly. Open Match will always deactivate the tickets found in the `roster` of any matches your MMF returns.

However, you can work around this by having your MMF leave the `roster` field empty and instead place the proposed players into the `extensions` field of the match. Your Director can then be coded to inspect the `extensions`. If it approves the match, it is then responsible for manually calling `DeactivateTickets` for the players in that match.

Be aware of the trade-off: until your Director deactivates those tickets, they remain active in Open Match and will be included in subsequent matchmaking cycles!

---

#### **Troubleshooting**

**Q: I'm seeing `Redis Failure; exiting: dial tcp <host:port>: i/o timeout` in my logs.**

A: This is almost always a networking issue. Check to ensure that the service where Open Match is running is deployed on the correct network and that it has the necessary network egress rules and VPC peering configurations to reach your Redis instance.

**Q: I'm using the gRPC-gateway and my client is failing to unmarshal the response from `InvokeMMFs`.**

A: This is an easy-to-miss detail of how the gRPC-gateway works. It wraps the actual response payload inside a parent JSON object under the key `"result"`. When you are unmarshalling the response, you need to extract the content of the `"result"` key first before passing it to your protobuf JSON unmarshaller.

---

#### **Division of responsibility**

**Q: There's an edge case that sometimes happens when matching. Can Open Match handle it for me?**

A: The design philosophy of Open Match is to remain unopinionated about how to handle specific edge cases. If a remediation for an edge case _could_ be handled by Open Match but would also likely need to be handled by most matchmakers, the responsibility is left to the matchmaker. This approach keeps the behavior of Open Match consistent and avoids being prescriptive. For example, one game's discarded match might be another game's backfill opportunity.

For more examples of failure states your matchmaker is expected to handle, see the **Failure Handling** section of this FAQ.

---

#### **Failure Handling**

**Q: What failure states does my matchmaker need to handle?**

A: Your matchmaker must be resilient to the realities of a distributed system. The eventual consistency model of Open Match means your code already needs to be robust against a number of scenarios. Key failure states you must handle include:

* **The Disappearing Player**: A player may disconnect or cancel matchmaking at any time. Your system needs a way to detect this and call `DeactivateTickets` to remove them from the pool.  
* **The Disappearing Server**: A game server that was available may become unhealthy or have a networking failure. What is the solution when a server allocation fails, or the allocation succeeds but players are never able to connect?  
* **Competing Match Proposals**: As discussed in the advanced MMF patterns, your Director will receive competing match proposals for the same tickets and must have logic to de-collide them and re-activate the tickets from discarded matches.


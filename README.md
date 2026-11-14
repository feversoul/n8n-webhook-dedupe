# n8n webhook dedupe

[![test](https://github.com/feversoul/n8n-webhook-dedupe/actions/workflows/test.yml/badge.svg)](https://github.com/feversoul/n8n-webhook-dedupe/actions/workflows/test.yml)

A webhook sender delivers the same event twice, sometimes in the same millisecond. This repo measures which ways of claiming an event in n8n really let only one execution through, and ships two receiver workflows that do: one on PostgreSQL, one on n8n **Data Tables** only (no database of your own, so it can run on n8n Cloud).

Everything here was run on a real n8n 2.47.7, with the same event sent 40 times at once.

| Question                                | Answer in this repo                                                                                                                                                                   |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Does "look up, then write" leave a gap? | Yes, and it is wide. With a Data Table, up to 15 of 40 copies got through in a round on SQLite, and all 40 on PostgreSQL. See [What was measured](#what-was-measured).                |
| Database unique constraint?             | Holds. `INSERT ... ON CONFLICT DO NOTHING RETURNING` let exactly one through in every round. So did a plain `INSERT` with the error output, with one setting to watch.                |
| Data Tables?                            | They have no unique constraint. "If row does not exist, then insert" races and Upsert races on PostgreSQL. **Insert first, then read the first row for the key** held in every round. |
| Remove Duplicates node?                 | Not for this. Up to 40 of 40 copies got through, and it also forgot events it had already seen.                                                                                       |
| How do you handle the conflict path?    | `200` when the event is already done, `409` while another run still has it, `500` when the work failed (the claim is freed). See [The conflict path](#the-conflict-path).             |

## What was measured

The lab workflow (`workflows/9-lab.json`) has six webhooks, one per method, each reduced to the claim alone. The test sends one event key 40 times at the same instant, 15 rounds, and counts how many calls were let through. One per round is correct, so the total should be 15. For the methods that race, the table gives the range over the runs so far (15 rounds each, on my machine and in GitHub Actions), because the count is different every time.

| Method                                                            | n8n on SQLite                                                                       | n8n on PostgreSQL                                                                    |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Data Table: If Row Does Not Exist, then Insert                    | **races**: 110 to 172 let through, worst round 13 to 15 of 40                       | **races**: 600 let through, all 40 in every round                                    |
| Data Table: Upsert                                                | 15, one per round                                                                   | **races**: 24 to 33 let through, one to four per round                               |
| Data Table: Insert, then read the first row for the key           | 15, one per round                                                                   | 15, one per round                                                                    |
| Remove Duplicates (Remove Items Processed in Previous Executions) | **races**: 316 to 464 let through, worst round 38 to 40 of 40, 1 to 37 calls failed | **races**: 430 to 475 let through, worst round 34 to 39 of 40, 32 to 37 calls failed |
| Postgres: `INSERT ... ON CONFLICT DO NOTHING RETURNING`           | 15, one per round                                                                   | 15, one per round                                                                    |
| Postgres: plain `INSERT`, unique violation on the error output    | 15, one per round                                                                   | 15, one per round                                                                    |

"n8n on SQLite / PostgreSQL" is the database n8n itself runs on. It decides how Data Tables and the Remove Duplicates history behave. The two Postgres methods use a separate claims table in both runs.

Notes on the rows:

- **If Row Does Not Exist, then Insert** is the lookup-then-write gap from the question. It is not a rare race: almost every round let several copies through.
- **Upsert** cannot tell you whether it inserted or updated, so the lab lets a call through when the returned row has the same `createdAt` and `updatedAt`. On SQLite it held in every round. On PostgreSQL more than one copy inserted in most rounds (one to four were let through), leaving two rows for one key. Do not build a claim on it.
- **Remove Duplicates** reads its history, adds the new value and writes the whole history back. Concurrent runs overwrite each other. A second test sent 40 different events at once and then each one again, one by one: 38 of the 40 second deliveries were let through as new, on both databases.
- **Plain `INSERT` with the error output**: turn **Always Output Data** off on that Postgres node. With it on, a failed insert also sends an empty item down the success output, and the duplicate runs the work.
- **Insert, then first row** works because every run can see the same order: row ids only go up, and each run reads after its own insert. One limit, below, for n8n on PostgreSQL with several workers.

## The two receivers

Both take `POST` with a JSON body that has an `id`, and both give the same answers.

**1 Receiver (Postgres)**, `workflows/1-receiver-postgres.json`. The claim is one statement on a table where `event_key` is the primary key:

```sql
INSERT INTO webhook_claims (event_key, status, owner, claimed_at)
VALUES ($1, 'processing', $2, now())
ON CONFLICT (event_key) DO UPDATE
  SET status = 'processing', owner = EXCLUDED.owner, claimed_at = now(), attempts = webhook_claims.attempts + 1
  WHERE webhook_claims.status = 'failed'
     OR (webhook_claims.status = 'processing'
         AND webhook_claims.claimed_at < now() - make_interval(secs => $3::int))
RETURNING owner, attempts;
```

`owner` is a token made up by the execution. The run that gets its own token back has the claim. A failed claim, or one still `processing` after the lease, is taken over by the next delivery in the same statement.

**2 Receiver (Data Tables)**, `workflows/2-receiver-data-table.json`. Insert first, then look:

1. Insert a row for the event with this run's token.
2. Read the row with the lowest `id` for that key.
3. If it is this run's row, it has the claim. If not, delete the own row again.
4. If the first row is `failed` or stale, take it over with one conditional update (`id`, `owner` and `status` must still match) and check the `owner` of the row that comes back.

Step 4 is the conditional-update claim from [n8n-job-leases](https://github.com/feversoul/n8n-job-leases), which tests that pattern on its own.

In both receivers, **Mark done** and **Mark failed** only change the row while it still carries this run's token.

### The conflict path

| Situation                                                | Answer                                                     | Why                                                                                                                                          |
| -------------------------------------------------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| First delivery, work succeeded                           | `200 { ok: true, duplicate: false, status: "done" }`       |                                                                                                                                              |
| Duplicate, event already done                            | `200 { ok: true, duplicate: true, status: "done" }`        | The sender can stop.                                                                                                                         |
| Duplicate, another run still has the claim               | `409 { ok: false, duplicate: true, status: "processing" }` | A `2xx` here tells the sender to stop while that run can still crash. With `409` it retries later and finds the event done, failed or stale. |
| Work failed                                              | `500 { ok: false, duplicate: false, status: "failed" }`    | The claim is freed, the next delivery runs the work again.                                                                                   |
| This run was slower than the lease and a retry took over | `409 { ok: false, duplicate: true, status: "superseded" }` | It may not mark the event done.                                                                                                              |
| No `id` in the body                                      | `400 { ok: false, error: "missing event id" }`             | An event without a key cannot be deduplicated, so it is refused, not processed.                                                              |

If your sender gives up after a few non-2xx answers, change the `409` of "in progress" to `200` in the workflow.

### The lease

`LEASE_SECONDS` at the top of the **Event** node (default 900) is how long a claim may stay `processing` before a retry may take it. Set it longer than your slowest run. If a run is slower than that, both runs reach **Do the work**: the token stops the slow one from marking the event done, it cannot stop the call it already made. That call gets the event key as `Idempotency-Key`; use it on the other side when the API supports it.

## Add it to your n8n

For n8n Cloud or an n8n you already run (2.x).

1. Create a credential of type **Header Auth** named exactly `Webhook Dedupe Key`, with the name `X-Webhook-Key` and a long random value. If your sender signs its requests instead, replace the authentication on the Webhook node.
2. Pick one receiver:
   - **Postgres**: create a **Postgres** credential named exactly `Webhook Dedupe Postgres`. Import `0-setup-postgres.json` and `1-receiver-postgres.json`.
   - **Data Tables**: import `0-setup-data-tables.json` and `2-receiver-data-table.json`.
3. Open the setup workflow and press **Execute workflow**. It creates the table.
4. In the receiver, open **Event** and set `SOURCE`, `LEASE_SECONDS` and the line that reads the event id. Replace the URL in **Do the work** with your own call, or replace the node with your own steps and keep **Worked?** pointed at their result.
5. Publish the receiver. Its address is `/webhook/dedupe/postgres` or `/webhook/dedupe/data-table`.

`9-lab.json` is only needed to repeat the measurements.

## Try it with Docker

```bash
git clone https://github.com/feversoul/n8n-webhook-dedupe.git
cd n8n-webhook-dedupe
cp .env.example .env      # fill in three random strings
docker compose up -d

# the same event 20 times at once
for i in $(seq 20); do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:5678/webhook/dedupe/data-table \
    -H "X-Webhook-Key: <your key>" -H "Content-Type: application/json" \
    -d '{"id":"evt_1","amount":10}' &
done; wait

curl "localhost:8080/calls?key=demo:evt_1"    # {"calls":1,"ok":1}
```

The compose file starts n8n, a PostgreSQL for the claims table and a small stand-in for "the work" that counts the calls it gets per event.

## What was tested

`npm run test:e2e` starts a real n8n 2.47.7, imports these workflows and calls them over HTTP. `npm run test:e2e:postgres` does the same with n8n itself on PostgreSQL. Both pass: 18 tests each, 6 for the lab and 6 for each receiver.

| Test, for both receivers                           | Result with n8n on SQLite and on PostgreSQL                                                                                                     |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| 40 copies of one event at once, 10 rounds          | the work ran once per event, one delivery answered "processed", a later duplicate got `200 done`                                                |
| 25 different events, 4 copies each, all at once    | every event ran once                                                                                                                            |
| Duplicate while the first run is still working     | `409 processing`, the work ran once                                                                                                             |
| Work fails, then 20 retries at once                | first answer `500`, exactly one retry runs the work again                                                                                       |
| Run slower than the lease, then 20 retries at once | exactly one retry takes over; the slow run comes back while that retry is still working, gets `409 superseded` and does not mark the event done |
| No key, no event id                                | `403`, `400`, nothing reaches the work                                                                                                          |

"The work ran once" is counted by the stand-in that receives the call, not by the workflow's own answer.

What was not tested, and other limits:

- **n8n Cloud.** The tests ran on self-hosted n8n in Docker. To run them against your own instance: `N8N_URL=https://you.app.n8n.cloud WEBHOOK_KEY=... DOWNSTREAM_URL=... node --test test/e2e.test.mjs`, with **Do the work** pointed at a reachable copy of `docker/downstream.mjs`.
- **Queue mode with several workers.** Every test ran on one n8n process. For the Data Table receiver this matters when n8n runs on PostgreSQL: a row id is handed out before the insert commits, so with several workers a run could read before a lower id becomes visible, and two runs would both think they are first. I did not see it in 100 rounds of 40, 300 rounds of 4 and 30 rounds of 150 on one process. If you run several workers you have PostgreSQL anyway: use the Postgres receiver, where the database decides.
- **A real crash of n8n mid-run** was not staged. The stale-claim path was tested with a run that is slower than the lease, which takes the same branch.
- **The measurements are counts from these runs, not rates.** A racing method lets a different number through each time. A method that held 15 rounds here is not proven by that alone; the two Postgres methods rest on the unique constraint, the Data Table one on the id order described above.
- **Old claims are never deleted.** Add a scheduled cleanup for rows older than your sender's retry window.
- **Exactly once ends at the claim.** If the work succeeds and n8n dies before "Mark done", the retry runs the work again after the lease. The `Idempotency-Key` on the downstream call is the guard for that.

## Change the workflows

The files in `workflows/` are generated by [`scripts/build.mjs`](scripts/build.mjs), because the lab endpoints and the two receivers share most of their shape. Edit them in the n8n editor like any workflow, or change the script and run `npm run build`.

MIT licence.

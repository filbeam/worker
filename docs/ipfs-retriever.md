# ipfs-retriever

## Overview

`ipfs-retriever` is a Cloudflare Worker that serves IPFS content stored by Filecoin service providers (SPs). It receives a request identifying a specific dataset and piece, fetches the corresponding CAR archive from an SP, validates every block, and streams the raw file bytes to the client.

It depends on the shared `@filbeam/retrieval` library for authorization, candidate selection, egress quota tracking, and the fetch lifecycle.

---

## URL formats

There are two entry points:

### Slug subdomain (primary)

```
https://1-{base32(dataSetId)}-{base32(pieceId)}.ipfs.calibration.filbeam.io/{subpath}
```

The slug encodes both the on-chain `dataSetId` and `pieceId` as base32 bigints, prefixed with a version (`1`). Parsed in `ipfs-retriever/lib/request.js`.

Examples:

- `https://1-abc123-def456.ipfs.calibration.filbeam.io/` — root of the piece
- `https://1-abc123-def456.ipfs.calibration.filbeam.io/path/to/file.jpg` — specific file

#### Why identifiers are in the subdomain

Static websites served over IPFS often load sub-resources at absolute paths (e.g. `<link href="/style.css">`). For these paths to resolve correctly, the dataset/piece identity must live in the subdomain, not the URL path, so that `/style.css` naturally maps to the right SP origin without any path rewriting. (see [original proposal comment, issue #297](https://github.com/filbeam/worker/issues/297#issuecomment-3346046091))

#### Why CID + wallet address can't both be in the subdomain

The initial design proposed combining the IPFS CID and wallet address into a single subdomain component (e.g. `bafk123-0xabc.filbeam.io`). This was ruled out because a single DNS label is limited to 63 characters, too short to fit both a base58 CID and a 42-character Ethereum address together. (see [follow-up comment, issue #297](https://github.com/filbeam/worker/issues/297#issuecomment-3352714474))

#### Why the wallet address is not in the slug at all

The wallet address is not needed in the slug because `dataSetId` alone is sufficient to look it up in D1. Omitting it keeps the subdomain short enough to be valid. The worker resolves the wallet from the dataset record at query time. (see [review comment, PR #312](https://github.com/filbeam/worker/pull/312#issuecomment-4797500253))

### Bare domain redirect (convenience)

```
https://ipfs.calibration.filbeam.io/{walletAddress}/{ipfsRootCid}/{subpath}
```

Handled by `handleDnsRootRequest` in `ipfs-retriever/bin/ipfs-retriever.js`. Looks up the slug for the given wallet and CID in D1, then issues a 302 redirect to the slug subdomain URL. Useful for constructing shareable links without knowing the on-chain IDs upfront.

If no path is provided (`/`), redirects to `https://filbeam.com`.

### Query parameters

- `?format=car` — skip CAR-to-raw conversion and serve the raw CAR archive to the client
- `?format=raw` — not yet implemented (returns 400); tracked in [issue #295](https://github.com/filbeam/worker/issues/295)
- No `?format` — default; converts CAR to raw file bytes

---

## Request flow

```
1. handleFetchRequest()       — shared fetch lifecycle (auth, error handling, egress logging)
2. parseRequest()             — decode slug → dataSetId + pieceId + subpath + format
3. getRetrievalCandidatesByDataSetAndPiece() — query D1 for SP candidates
4. assertCidNotDenied()       — check Bad Bits denylist
5. selectRetrievalCandidate() — try candidates in order, retry on failure
6. retrieveIpfsContent()      — fetch CAR from SP (streaming, not buffered)
7. processIpfsResponse()      — validate and convert CAR → raw bytes
8. Return streaming response  — client receives raw file bytes
```

Authorization (payment rail validity, egress quota, sanctions check) is handled inside `handleFetchRequest` via the shared `@filbeam/retrieval` library before any SP fetch occurs.

---

## CAR streaming pipeline

The core of the worker is a fully lazy, end-to-end streaming pipeline. Nothing is buffered in memory. The only allocation at any point is one block at a time.

```
SP (frisbii/Curio)
  └─ CAR stream (HTTP response body, streaming)
       └─ countingBody — async generator counting SP egress bytes
            └─ CarBlockIterator.fromIterable() — parses CAR header upfront, yields blocks lazily
                 └─ blockstore.get(cid) — called by the exporter per block
                      ├─ blocksReader.next() — pulls next block from the CAR stream
                      ├─ multihash comparison — verifies the block CID matches what the exporter asked for
                      └─ validateBlock() — hashes the bytes, confirms they match the multihash
                           └─ ipfs-unixfs-exporter (recursive())
                                └─ entry.content() — yields leaf block bytes
                                     └─ ReadableStream → HTTP response to client
```

### Why `blockReadConcurrency: 1`

The exporter is called with `{ blockReadConcurrency: 1 }`. This forces it to request blocks strictly one at a time in DFS traversal order. The blockstore's `get(cid)` does not look blocks up by CID, it calls `blocksReader.next()` and expects that the next block in the CAR is always the one being requested. This works because the SP guarantees DFS-ordered delivery (see [SP integration contract](#sp-integration-contract)). If `blockReadConcurrency` were greater than 1, the exporter would request blocks in parallel and the sequential CAR reader would return the wrong block for each.

### `CarBlockIterator` vs `CarReader`

`CarBlockIterator.fromIterable` reads only the CAR header (roots + version) upfront. Block data is pulled lazily as the iterator is consumed.

### CAR-to-raw conversion

`processIpfsResponse` uses the `recursive` export from `ipfs-unixfs-exporter` aliased as `exporter`. Despite the name, it is used here only to resolve the first entry (the requested path) and stream its bytes, the loop exits after the first iteration intentionally (`// eslint-disable-next-line no-unreachable-loop`). The actual `exporter()` function would be semantically cleaner for this use case.

When converting CAR to raw:

- `content-disposition: inline` is set so browsers display the content instead of downloading it
- `content-type` and `x-content-type-options` are removed so the browser sniffs the raw bytes

### Directory entries

If the resolved path is a UnixFS directory, the worker returns **404 Not Found** (`retrieval.js:195`). Directory listing is not implemented. Since the SP returns a path-scoped CAR with `dag-scope=all`, the directory block and immediate child blocks are present in the CAR. The 404 is a choice, not an architectural limit. This is tracked in [issue #696](https://github.com/filbeam/worker/issues/696).

---

## SP integration contract

The pipeline makes specific assumptions about how the SP delivers the CAR. These are satisfied by **Curio**, which implements the [frisbii](https://github.com/ipld/frisbii) trustless HTTP gateway:

1. **Path-scoped CAR** — the SP is called as `GET /ipfs/{rootCid}{subpath}?format=car`. It returns a CAR containing exactly the blocks needed to walk from `rootCid` to `subpath` and read the file. No more, no less. There are no wasted bytes for single-asset requests regardless of dataset size.

2. **DFS-ordered blocks** — blocks are written to the CAR in the same order the DAG traversal engine requests them (depth-first). This is enforced in frisbii via `carPipe`, which hooks into the IPLD link system and writes each block to the CAR immediately as it is loaded during traversal. This is what makes the sequential `blocksReader.next()` blockstore safe.

3. **Complete or fail loudly** — if any block is missing from SP storage, the traversal fails before any bytes are written to the CAR. The SP returns an HTTP error, not a partial CAR. The worker either gets a complete, valid CAR or an error response, never a silently truncated one.

**If a non-frisbii SP is ever onboarded**, all three guarantees must be verified before integration (tracked in [issue #692](https://github.com/filbeam/worker/issues/692)). The CID mismatch error (`Unexpected block CID`) is the failure mode if ordering is violated, it is not obvious without this context.

### Alternative: block-by-block retrieval (Discarded)

An alternative design (used by IPFS Shipyard tooling like Boxo and verified-fetch) drives the exporter with a blockstore whose `get(cid)` makes individual HTTP requests: `GET /ipfs/{blockCid}?format=raw`. This eliminates the ordering dependency and enables per-block Cloudflare caching.

The trade-off: a single file goes from 1 SP fetch to potentially 20–80 individual fetches. For FilBeam, which controls both ends and knows all blocks are at the same SP, the CAR approach is currently a good solution, one round-trip, any file size, fully streaming. The block-by-block design exists to solve distributed-network problems (blocks scattered across unknown peers) that FilBeam does not have.

[Slack discussion Reference](https://filecoinproject.slack.com/archives/C08TVNKJV7C/p1779163683412569?thread_ts=1778671570.686079&cid=C08TVNKJV7C)

---

## Caching

There are two independent cache layers.

### Layer 1 — Cloudflare edge cache (origin fetch)

Configured via the `cf` option on the SP `fetch` call (`retrieval/lib/origin-cache.js`):

```js
{
  cacheEverything: true,
  cacheTtlByStatus: { '200-299': ORIGIN_CACHE_TTL, 404: 0, '500-599': 0 }
}
```

- `cacheEverything: true` — caches the SP response regardless of its `Cache-Control` header
- 2xx responses are cached for `ORIGIN_CACHE_TTL` seconds (currently **86400, 1 day**)
- 404 and 5xx responses are never cached

**Cache key:** `{spBaseUrl}/ipfs/{ipfsRootCid}{ipfsSubpath}?format=car`

Since CIDs are content-addressed and immutable, the same key always resolves to the same bytes. `ORIGIN_CACHE_TTL` could theoretically be much longer, but a very large TTL risks filling PoP cache storage with large CARs. The current 1-day value is a reasonable starting point and should be revisited once cache hit rates and storage pressure are measurable.

**Cache miss detection:** after the fetch, `CF-Cache-Status: HIT` means the edge served a cached copy; anything else is treated as a cache miss. Cache misses are what drive egress quota billing.

### Layer 2 — Client/browser cache

Set on every successful response to the client (`retrieval/lib/fetch-handler.js`):

```
Cache-Control: public, max-age=31536000
```

1 year. CID-addressed content is immutable, so this is correct.

### Where cache data is stored

Both layers use Cloudflare's edge network. Data is cached at the **PoP (Point of Presence)** that handled the request, whichever of Cloudflare's ~300 global data centers is geographically closest to the client. Each PoP maintains its own independent cache. A cache hit in Los Angeles does not warm the Frankfurt PoP. Enabling **Cloudflare Tiered Cache** at the account level would add a regional upper-tier cache, reducing cold-PoP fetches from Storage Providers for popular content if this becomes necessary in the future (it's not needed today).

### Cache hit cost

On a cache hit, the SP round-trip is eliminated but the worker still receives the full CAR body and runs the complete pipeline (CAR parsing, block validation, unixfs traversal). The cache saves network bytes from the SP but not CPU work in the worker.

---

## Related

- `retrieval/` — shared library providing `handleFetchRequest`, `selectRetrievalCandidate`, `assertCidNotDenied`, egress quota tracking, and authorization

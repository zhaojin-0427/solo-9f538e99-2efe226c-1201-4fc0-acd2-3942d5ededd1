# Rainfall Mosaic Service

Combines two same-day, adjacent single-band rainfall GeoTIFF blocks into one
research-area mosaic that models can read, with versioned publication over a
FastAPI API, Rasterio for raster work and SQLite for metadata.

## Run with Docker Compose

```bash
docker compose up --build -d
# Swagger UI:   http://localhost:8000/docs
# Health:       http://localhost:8000/healthz
docker compose logs -f api
```

Published images and the SQLite database live in the named volume
`rainmosaic-data`, so versions and the current pointer survive
`docker compose restart`, `down` and container upgrades (`down -v` deletes
them). Change the host port with `RAINMOSAIC_PUBLISHED_PORT=9000 docker compose up -d`.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/api/v1/mosaics` | Submit two blocks; publishes a new version |
| GET | `/api/v1/mosaics` | List versions, newest first (`day`, `limit`, `offset`) |
| GET | `/api/v1/mosaics/current` | Current version metadata |
| GET | `/api/v1/mosaics/{version}` | Metadata for a fixed version |
| GET | `/api/v1/mosaics/{version}/download` | GeoTIFF for a fixed version |
| GET | `/api/v1/download/current` | GeoTIFF for the current version |
| POST | `/api/v1/subscriptions` | Register a subscriber (starts at version 0) |
| GET | `/api/v1/subscriptions/me` | A subscriber's progress |
| POST | `/api/v1/subscriptions/claims` | Claim the next image notification (lease 60 s) |
| POST | `/api/v1/subscriptions/confirmations` | Confirm a claimed version after ingesting it |
| GET | `/healthz` | Liveness, database and current-version check |

`/docs` supports submitting (multipart form), browsing and downloading, and
exercising the full subscription flow (set the two `X-Subscriber-*` headers on
the claim/confirm calls).

### Submitting

`multipart/form-data` fields:

| Field | Meaning |
| --- | --- |
| `block_a`, `block_b` | The two GeoTIFF files |
| `block_a_no`, `block_b_no` | Distinct block numbers |
| `block_a_captured_at`, `block_b_captured_at` | UTC acquisition instants, ISO-8601 with offset |
| `expected_version` | Version the submitter last saw; `0` if none is published |
| `day` | Optional `YYYY-MM-DD`; taken from the earlier capture instant if omitted |

Raster contract, enforced per block:

* GeoTIFF, exactly one band, `int16`, NoData declared as `-32768`;
* at most 64×64 pixels;
* north-up grid (no rotation/shear);
* both blocks: same CRS (compared semantically, so an equivalent WKT/PROJ
  spelling is fine), same pixel size, origins offset by whole pixels.

```bash
curl -X POST http://localhost:8000/api/v1/mosaics \
  -F day=2026-01-01 \
  -F expected_version=0 \
  -F block_a_no=101 -F block_a_captured_at=2026-01-01T01:00:00Z \
  -F block_b_no=102 -F block_b_captured_at=2026-01-01T02:00:00Z \
  -F block_a=@block_a.tif -F block_b=@block_b.tif
```

Response `201` carries `version`, `day`, `image` (size, CRS, transform, bounds,
resolution, dtype, NoData, SHA-256, byte size), both block descriptors and a
`download_url`.

### Mosaic rule

The output covers the union bounding rectangle of the two blocks. Each
overlapping pixel takes the value of the block acquired later (UTC); where that
block is NoData the other block's value is used; equal instants are broken by
the larger block number. Pixels covered by neither block stay `-32768`. Output
is a single-band `int16` GeoTIFF with the block CRS/resolution and NoData
declared as `-32768`.

### Versioning and concurrency

`expected_version` makes publication a compare-and-swap. Submissions that
started from the same expected version result in exactly one new version; the
others get `409 version_conflict` with the winning version embedded. Raster
validation errors (`422`), missing/oversized uploads (`413`) and failed
generation (`500 publish_failed`) never create a version and never disturb the
current one.

A new version is rendered to a temporary file, verified by re-opening it,
fsynced and atomically renamed, and only then inserted into SQLite and pointed
to as current, all inside one `BEGIN IMMEDIATE` transaction. The current pointer
therefore never references a partial image; a crash simply leaves the previous
version current. Readers use WAL snapshots and keep downloading the old version
while a publication is in flight.

Every version keeps its own immutable file, so `GET /api/v1/mosaics/{version}/download`
always returns exactly the bytes registered under that version (with a stable
SHA-256 `ETag`), including after restarts.

### Errors

Errors use `{"detail": {...}}` with `code` and `message`; raster errors also
carry `slot` (`a`/`b`) and, where applicable, `field` (`crs`, `resolution`,
`grid_alignment`, `dimensions`, `block_no`).

## Subscriptions (hydrology model ingest)

A subscriber represents one model input database. After registering it starts
at version **0** and pulls the published images strictly in publication order,
one notification at a time:

```
POST /api/v1/subscriptions                 -> subscriber_id + secret (shown once)
POST /api/v1/subscriptions/claims          -> one notification + a 60 s claim token
GET  /api/v1/mosaics/{version}/download    -> download the pinned image
POST /api/v1/subscriptions/confirmations   -> confirm ingestion; progress advances
```

### Registering

`POST /api/v1/subscriptions` with an optional `{"name": "..."}` returns a
`subscriber_id` and a `secret`. The secret is returned only once; claim and
confirm calls must send both as the `X-Subscriber-Id` and
`X-Subscriber-Secret` headers.

### Claim notifications

`POST /api/v1/subscriptions/claims` returns the smallest version the
subscriber has not yet acknowledged. Each notification contains:

| Field | Meaning |
| --- | --- |
| `version` | Publication sequence number, starting at 1 |
| `day` | Observation day of the image (`YYYY-MM-DD`) |
| `replaced_version` | Version of the image published earlier for that same day that this one supersedes; `null` for the day's first publication |
| `sha256` | SHA-256 of the GeoTIFF bytes |
| `download_url` | Fixed, immutable download address for this version |
| `claim.token` | Opaque credential required to confirm this version |
| `claim.lease_expires_at` | UTC instant at which the 60 s lease expires |

Delivery and locking rules:

* Only **committed** publications are ever returned; a publication still in
  flight is invisible to subscribers.
* While a claim is open, repeat calls return the *same* notification and the
  *same* token. After the 60 second lease expires the same version may be
  re-claimed, which invalidates the old token.
* If the image file is temporarily unavailable the claim **stops at that
  version** with `503 image_unavailable` and opens no lease, so later versions
  can never be reached out of order; retrying once the file is back resumes at
  exactly that version.
* A confirm must present the active token of the exact next version. Repeating
  a completed confirmation is idempotent (`"repeated": true`) and never skips a
  version; a token from an expired or re-claimed lease is rejected
  (`409 lease_expired` / `409 claim_superseded`), so an old credential cannot
  move progress. Out-of-order confirms get `409 invalid_claim_token`.
* When no new version exists, the claim returns `404 no_notification` without
  creating any lease.
* All state is transactional and persisted: a publication and a confirmation
  racing each other, or a service restart, never lose, duplicate or reorder a
  version. Subscribers are independent rows and never affect one another.
* Reads run on WAL snapshots and take the write lock only for the short
  lease/confirm updates, so subscription traffic does not block publishing or
  downloads.

A minimal ingest loop:

```bash
BASE=http://localhost:8000/api/v1
AUTH=(-H "X-Subscriber-Id: $SID" -H "X-Subscriber-Secret: $SECRET")

while true; do
  note=$(curl -sS -X POST "${AUTH[@]}" "$BASE/subscriptions/claims") || { sleep 5; continue; }
  echo "$note" | grep -q no_notification && { sleep 60; continue; }
  ver=$(echo "$note" | python -c 'import sys,json;print(json.load(sys.stdin)["version"])')
  token=$(echo "$note" | python -c 'import sys,json;print(json.load(sys.stdin)["claim"]["token"])')
  url=$(echo "$note" | python -c 'import sys,json;print(json.load(sys.stdin)["download_url"])')
  curl -sSf "http://localhost:8000$url" -o "v$ver.tif"
  # ... verify the SHA-256 and write v$ver.tif into the model input store ...
  curl -sS -X POST "${AUTH[@]}" -H 'Content-Type: application/json' \
       -d "{\"version\": $ver, \"claim_token\": \"$token\"}" \
       "$BASE/subscriptions/confirmations"
done
```

`GET /api/v1/subscriptions/me` reports `last_ack_version`,
`latest_published_version` and the number of `pending_versions`.

### Same-day republishing

Submitting a new mosaic for an observation day that already has a version does
not delete anything: the new version is appended, becomes current, and records
the previous same-day version as `supersedes_version` (exposed as
`replaced_version` in notifications and in the version metadata). Every old
version remains downloadable at its fixed URL.

## Design notes

* SQLite runs in WAL mode with `synchronous=FULL`; state lives in
  `$RAINMOSAIC_DATA_DIR/rainmosaic.sqlite3`, images in `versions/`, scratch files
  in `tmp/`. Subscriber, lease and claim-token tables live in the same database
  and are created (and, where needed, migrated) at startup.
* Startup validates the schema, repairs a dangling current pointer and removes
  old orphan files left by interrupted publications.
* Generation and file I/O run in a worker thread so other requests, including
  downloads, keep flowing. Subscription claims and confirms do all their read
  checks on a WAL snapshot and only take the write lock for the short lease or
  progress update.
* Only one API worker is used by default: SQLite plus a local disk volume make
  horizontal scaling a deployment concern rather than a code feature.

## Local development and tests

```bash
python -m pip install -r requirements.txt -r requirements-dev.txt
RAINMOSAIC_DATA_DIR=./data python main.py          # http://127.0.0.1:8000/docs
python -m pytest -q
```

The test suite covers the mosaic rules (overlap priority, NoData fallback, tie
break, gaps, alignment/CRS/size validation), publishing, version pinning,
conflicts, concurrent submissions, failed generation, downloads during
publication, restart persistence, listing/pagination and error handling, plus
the full subscription state machine: ordered delivery, repeated and idempotent
confirms, 60 second lease expiry and re-claim, stale-token rejection,
unavailable-image blocking, concurrent claim/confirm races against a real
uvicorn server, in-flight publication isolation, subscriber independence and
schema migration from a release-1 database.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RAINMOSAIC_DATA_DIR` | `/data` in the image, `./data` locally | Writable persistent directory |
| `RAINMOSAIC_MAX_UPLOAD_BYTES` | `2097152` | Per-file upload cap |
| `RAINMOSAIC_CLAIM_LEASE_SECONDS` | `60` | Subscription claim lease duration |
| `RAINMOSAIC_PORT` | `8000` | Listen port |
| `RAINMOSAIC_LOG_LEVEL` | `INFO` | Log level |

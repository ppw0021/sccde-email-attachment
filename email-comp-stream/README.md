# Email Export Attachment — SpeedBee Synapse Component

A SpeedBee Synapse dataflow component that collects streaming data into an in-memory CSV buffer and sends it as an email attachment, either on demand via a trigger signal or automatically when a row count threshold is reached.

| Name | UUID |
|---|---|
| Email Export Attachment (Dataflow) | `6e56afe5-c2c9-49f1-9b71-1220e7d4add9` |

---

## Ports

| Port | Direction | Purpose |
|---|---|---|
| `in_port1` | Input | Trigger signal — any truthy value flushes the buffer and sends the email |
| `in_port2` | Input | Data stream — records are collected into the CSV buffer |
| `out_port1` | Output | Log column — emits status messages and error details |

---

## Parameters

| Parameter | Label | Type | Required | Default | Description |
|---|---|---|---|---|---|
| `smtphost` | SMTP Host | string | Yes | — | Hostname of the SMTP server |
| `smtpport` | SMTP Port | number | Yes | `587` | Port number (1–65535) |
| `smtpusername` | SMTP User | string | Yes | — | SMTP login username |
| `smtppassword` | SMTP Password | string | Yes | — | SMTP login password (masked in logs) |
| `timeout` | Timeout (seconds) | number | Yes | `5` | SMTP connection timeout in seconds |
| `fromaddress` | From | string | Yes | — | Sender email address |
| `toaddress` | To (comma-separated) | string | Yes | — | One or more recipient addresses, comma-separated |
| `subjectemail` | Subject | string | Yes | — | Email subject line |
| `bodyemail` | Message Body | string | No | `""` | Plain text email body |
| `rowthreshold` | Row Threshold | number | No | `0` | Send automatically when buffer reaches this many rows. `0` disables threshold-based sending |
| `maxbufferrows` | Max Buffer Rows | number | No | `0` | Cap the in-memory buffer at this many rows. Oldest rows are dropped when full. `0` = unlimited |

---

## Send Conditions

An email is sent when **either** of the following conditions is met, subject to the shared rules below.

### 1. Manual trigger (`in_port1`)
- A truthy value arrives on `in_port1`
- The buffer contains at least one row

### 2. Row threshold (`in_port2` data reader)
- `rowthreshold` is greater than `0`
- The number of buffered rows reaches or exceeds `rowthreshold`

### Shared rules (both conditions)
- At least **1 second** must have elapsed since the last send. Triggers arriving within that window are discarded with a warning — this prevents burst signals from firing multiple emails simultaneously.
- If both conditions fire at the same moment, whichever acquires the send lock first will proceed; the other will be dropped as a burst duplicate.
- On send, the buffer is **snapshotted and cleared**. The next window of data starts accumulating immediately after.

---

## Buffer Behaviour

- The buffer accumulates rows from `in_port2` continuously between sends.
- Each row includes a `timestamp` column (derived from the record's nanosecond timestamp) followed by all data columns from that record.
- Column headers are captured from the first row received and preserved across sends. They are never reset mid-run.
- If `maxbufferrows` is set and the buffer is full, the **oldest row is silently dropped** to make room for the new one. A warning is logged each time this happens.
- The buffer is cleared after every successful send — triggers and threshold sends both reset it.

---

## Error Handling

- If the SMTP send fails for any reason, the error is logged via `self.log.error` and also written to `out_port1` so downstream components can react.
- The buffered data is **discarded** on failure — there is no retry.
- The SMTP connection is always closed cleanly, even if the send throws an exception.

---

## Threading Model

The component runs two concurrent threads:

- **Data reader thread** — continuously reads from `in_port2` and appends rows to the shared buffer. Also checks the row threshold after each append.
- **Trigger reader (main thread)** — reads from `in_port1` and calls `try_flush()` on each truthy value.

All buffer access is protected by `csv_lock`. SMTP I/O is always performed **outside** this lock so a slow or timing-out email send never blocks incoming data from being collected.

On shutdown, a `stop_event` signals the data reader thread to exit. The main thread waits up to 5 seconds for it to finish before logging a warning if it hasn't stopped cleanly.

---

## Output Column

Every send attempt writes a message to the `log` column on `out_port1`:

| Outcome | Example message |
|---|---|
| Success (trigger) | `Email sent (trigger): 42 rows` |
| Success (threshold) | `Email sent (threshold=100): 100 rows` |
| Failure | `Email FAILED (trigger): [SMTPException details]` |

---

## Notes

- SMTP communication uses STARTTLS (`server.starttls()`). Implicit SSL (port 465) is not currently supported — use port 587 or another STARTTLS-capable port.
- The `toaddress` field supports multiple recipients separated by commas, e.g. `alice@example.com, bob@example.com`.
- The attached CSV filename is auto-generated as `data_YYYYMMDD_HHMMSS.csv` based on the time of sending.
- The password is never written to logs in plaintext — it is masked as asterisks.
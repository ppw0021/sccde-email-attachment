# Email Attachment from CSV — SpeedBee Synapse Component

A SpeedBee Synapse dataflow component that collects CSV files from a time-partitioned directory, merges them into a single attachment, and sends it via email. Triggered by a signal on PORT1 or an optional periodic schedule.

| Name | UUID |
|---|---|
| Email Attachment from CSV (Dataflow) | `a009b7cd-a2bb-438d-a450-b1640b7fa5b3` |

---

## Ports

| Port | Direction | Purpose |
|---|---|---|
| `in_port1` | Input | Trigger signal — any truthy value starts collection and sends the email |
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
| `savelocation` | Save Location | string | Yes | — | Root directory where CSV files are stored |
| `savelocationcont` | Save Location Cont | string | Yes | — | Subdirectory appended to the root path |
| `section` | Section | string | Yes | — | Data source label used in the folder path and attachment filename prefix |
| `sendhours` | Window / Send Interval (Hours) | number | No | `0` | How many hours back from now to collect files. Also used as the periodic send interval when `enableperiodic` is on |
| `enableperiodic` | Enable Periodic Sending | number | No | `0` | Set to `1` to send automatically on a repeating schedule. Set to `0` to only send on a PORT1 trigger |

---

## Directory Structure

The component walks a time-partitioned folder tree rooted at `savelocation/savelocationcont/section/`. Each hour of data is expected under:

```
<savelocation>/<savelocationcont>/<section>/<YYYYMM>/<DD>/<HH>/
```

When triggered, the component iterates over every hour folder from `(now - sendhours)` to `now`, collects all `.csv` files found, and merges them into a single output CSV. The header is taken from the first file encountered; all subsequent files have their header row stripped. Data rows are sorted by the first column before sending.

---

## Send Conditions

An email is sent when **either** of the following conditions is met:

1. **Manual trigger** — a truthy value arrives on `in_port1`
2. **Periodic schedule** — `enableperiodic` is `1` and the interval (`sendhours`) has elapsed

If no CSV files are found in the time window, the send is skipped and a message is logged to `out_port1`.

---

## Threading Model

When `enableperiodic` is enabled, a background thread fires the send at each interval independently of PORT1. The main thread reads PORT1 triggers concurrently. A `stop_event` coordinates shutdown — the periodic thread exits cleanly when the component stops.

---

## Error Handling

- If the SMTP send fails, the error is logged via `self.log.error` and written to `out_port1`.
- Individual CSV files that cannot be read are skipped with a warning; the rest of the collection proceeds.
- The SMTP connection is always closed cleanly, even if the send throws an exception.

---

## Output Column

Every send attempt writes a message to the `log` column on `out_port1`:

| Outcome | Example message |
|---|---|
| Success | `Email sent: SectionTest_20240115_143000.csv (250 rows)` |
| No data | `No data found — email skipped` |
| Failure | `Email FAILED: [SMTPException details]` |

---

## Notes

- SMTP communication uses STARTTLS (`server.starttls()`). Implicit SSL (port 465) is not supported — use port 587 or another STARTTLS-capable port.
- The `toaddress` field supports multiple recipients separated by commas, e.g. `alice@example.com, bob@example.com`.
- The attachment filename is auto-generated as `<section>_YYYYMMDD_HHMMSS.csv`.
- The password is never written to logs in plaintext — it is masked as asterisks.

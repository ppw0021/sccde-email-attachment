# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains two **SpeedBee Synapse Component Package** (`.sccpkg`) components that send CSV data via email using SMTP. Both are Python-based dataflow components for the SpeedBee Synapse engine.

- **email-comp-stream**: Collects a live data stream into an in-memory CSV buffer and emails it as an attachment, triggered by a signal or row-count threshold.
- **email-comp-fromcsv**: Reads CSV files from a time-ranged directory structure and emails them as a merged attachment, triggered by a signal or periodic timer.

## Development Setup

```bash
uv sync          # install dependencies (Python 3.12, uses uv)
```

No test suite or build script exists. Components are packaged as `.sccpkg` files distributed from each component's directory.

## Architecture

Both components follow the same SpeedBee pattern via `HiveComponentBase`:

1. `main(param)` is the entry point — receives all config as a dict.
2. A `HiveComponentOutput` column is created on `out_port1` for structured logging.
3. Input ports are read concurrently: `in_port1` (trigger), `in_port2` (data stream, stream component only).
4. Business logic runs on threads (data collection, optional periodic send).
5. Shutdown is signalled via `notify_stop()` / a threading `Event`.

### email-comp-stream (`email_attachment.py`)

```
in_port2 (data rows) ──> in-memory CSV buffer (lock-protected)
in_port1 (trigger)   ──> TryFlush() ──> SMTP send ──> out_port1 (log)
row count >= threshold ──────────────────────────────> SMTP send
```

- Buffer is bounded by `maxbufferrows`; oldest rows are dropped when full.
- A 1-second minimum interval between sends prevents burst triggers.
- SMTP I/O happens **outside** the buffer lock to avoid blocking data collection.
- The CSV is built in-memory (`io.StringIO`) and sent as a `MIMEBase` attachment.
- Buffer is cleared **only on successful send**.

### email-comp-fromcsv (`sample_collector_1.py`)

```
in_port1 (trigger) ──> walk FS by minute folders (YYYY/MM/DD/HH/MM)
                        merge CSVs (keep first header only)
                        SMTP send ──> out_port1 (log)
optional periodic thread ──────────────────────────────────────────>
```

- Two directory roots: `savelocation` (primary) and `savelocationcont` (continuation/overflow).
- Folder traversal covers a configurable time range back from now.
- `enableperiodic=1` starts a background thread that fires every `sendhours`h `sendminutes`m.

### Shared SMTP Config (both components)

| Parameter | Notes |
|-----------|-------|
| `smtphost`, `smtpport` | SMTP server; uses STARTTLS (port 587 typical) |
| `smtpusername`, `smtppassword` | Credentials; password is masked in logs |
| `fromaddress`, `toaddress` | Sender/recipient |
| `subjectemail`, `bodyemail` | Email content |
| `timeout` | SMTP connection timeout (seconds) |

### Component Registration

Each component directory contains:
- `scc-info.json` — UUID, name, author, parameter UI path mapping.
- `parameter_ui/<name>/custom_ui.json` — JSON schema defining the UI form shown in SpeedBee Synapse when configuring the component.

The UUID in `scc-info.json` is the stable identifier used by the Synapse engine:
- stream: `6e56afe5-c2c9-49f1-9b71-1220e7d4add9`
- fromcsv: `a009b7cd-a2bb-438d-a450-b1640b7fa5b3`

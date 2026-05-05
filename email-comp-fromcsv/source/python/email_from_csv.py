import smtplib
import datetime
import os
import csv
import io
import threading
from speedbeesynapse.component.base import HiveComponentBase, HiveComponentInfo, DataType
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate

@HiveComponentInfo(
    uuid='a009b7cd-a2bb-438d-a450-b1640b7fa5b3',
    name='Email Attachment from CSV (Dataflow)',
    inports=1,
    outports=1
)
class HiveComponent(HiveComponentBase):

    def main(self, param):
        smtp_host          = param['smtphost']
        smtp_port          = param['smtpport']
        smtp_user          = param['smtpusername']
        smtp_password      = param['smtppassword']
        smtp_timeout       = param['timeout']
        smtp_from          = param['fromaddress']
        smtp_to            = param['toaddress']
        smtp_subject       = param['subjectemail']
        smtp_body          = param.get('bodyemail', '')
        save_location      = param['savelocation']
        save_location_cont = param['savelocationcont']
        section            = param['section']
        send_hours         = int(param.get('sendhours', 0))
        enable_periodic    = int(param.get('enableperiodic', 0)) == 1

        self.log.info(f"Config: smtp={smtp_host}:{smtp_port}, user={smtp_user}, from={smtp_from}, to={smtp_to}")
        self.log.info(f"Config: save_location={save_location}, cont={save_location_cont}, section={section}")
        self.log.info(f"Config: send_hours={send_hours}, enable_periodic={enable_periodic}")

        logclm = self.out_port1.Column('log', DataType.STRING)
        to_list = [addr.strip() for addr in smtp_to.split(",")]
        stop_event = threading.Event()

        def collect_csvs():
            """Walk hour folders from (now - send_hours) to now, merge all CSVs sorted by first column."""
            end_time   = datetime.datetime.now()
            start_time = end_time - datetime.timedelta(hours=send_hours)
            self.log.info(f"Collecting CSVs: window={send_hours}h, from={start_time} to={end_time}")

            header = None
            all_rows = []
            total_files = 0

            current = start_time.replace(minute=0, second=0, microsecond=0)
            while current <= end_time:
                hour_folder = os.path.join(
                    save_location,
                    save_location_cont,
                    section,
                    current.strftime("%Y%m"),
                    current.strftime("%d"),
                    current.strftime("%H")
                )
                self.log.info(f"Checking hour folder: {hour_folder}")

                if not os.path.isdir(hour_folder):
                    self.log.info(f"  Not found, skipping.")
                    current += datetime.timedelta(hours=1)
                    continue

                for root, dirs, files in os.walk(hour_folder):
                    dirs.sort()
                    csv_files = sorted(f for f in files if f.lower().endswith('.csv'))
                    if not csv_files:
                        self.log.info(f"  No CSV files in {root}")
                        continue
                    self.log.info(f"  {len(csv_files)} CSV file(s) in {root}")
                    for fname in csv_files:
                        fpath = os.path.join(root, fname)
                        try:
                            with open(fpath, 'r', encoding='utf-8') as f:
                                rows = list(csv.reader(f))
                            if not rows:
                                self.log.info(f"    {fname}: empty, skipping")
                                continue
                            if header is None:
                                header = rows[0]
                                self.log.info(f"    {fname}: header captured ({len(header)} columns): {header}")
                                data_rows = rows[1:]
                            else:
                                data_rows = rows[1:]
                            self.log.info(f"    {fname}: {len(data_rows)} data row(s) added")
                            all_rows.extend(data_rows)
                            total_files += 1
                        except Exception as e:
                            self.log.warning(f"    {fname}: could not read — {e}")

                current += datetime.timedelta(hours=1)

            self.log.info(f"Collection done: {total_files} file(s), {len(all_rows)} row(s) before sort")

            if not all_rows:
                self.log.warning("No data rows found in time window.")
                return None, 0

            def sort_key(row):
                if not row:
                    return ''
                try:
                    return float(row[0])
                except (ValueError, TypeError):
                    return str(row[0])

            all_rows.sort(key=sort_key)
            self.log.info(f"Sorted {len(all_rows)} rows by first column")

            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(header)
            writer.writerows(all_rows)
            return buf.getvalue(), len(all_rows)

        def send_email(trigger_timestamp):
            self.log.info("Email send triggered — starting collection...")
            csv_content, row_count = collect_csvs()

            if not csv_content:
                self.log.warning("No CSV data found in time range — skipping send.")
                logclm.insert("No data found — email skipped", trigger_timestamp)
                return

            filename = f'{section}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            self.log.info(f"Preparing email: filename={filename}, rows={row_count}, to={smtp_to}")

            try:
                msg = MIMEMultipart()
                msg['From']    = smtp_from
                msg['To']      = smtp_to
                msg['Date']    = formatdate(localtime=True)
                msg['Subject'] = smtp_subject
                msg.attach(MIMEText(smtp_body if smtp_body else "Please find the attached CSV data."))

                part = MIMEApplication(csv_content.encode('utf-8'), Name=filename)
                part['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(part)

                self.log.info(f"Connecting to SMTP {smtp_host}:{smtp_port} (timeout={smtp_timeout}s)...")
                server = smtplib.SMTP(smtp_host, smtp_port, timeout=int(smtp_timeout))
                try:
                    server.starttls()
                    self.log.info("STARTTLS OK, logging in...")
                    server.login(smtp_user, smtp_password)
                    self.log.info("Login OK, sending...")
                    server.sendmail(smtp_from, to_list, msg.as_string())
                finally:
                    server.quit()

                self.log.info(f"Email sent: {filename} ({row_count} rows)")
                logclm.insert(f"Email sent: {filename} ({row_count} rows)", trigger_timestamp)

            except Exception as e:
                self.log.error(f"Failed to send email: {e}")
                logclm.insert(f"Email FAILED: {e}", trigger_timestamp)

        def periodic_sender():
            interval_seconds = send_hours * 3600
            if interval_seconds <= 0:
                self.log.warning("Periodic sending enabled but send_hours=0 — periodic thread exiting.")
                return
            self.log.info(f"Periodic sender active: interval={interval_seconds}s ({send_hours}h)")
            while self.is_runnable() and not stop_event.is_set():
                stop_event.wait(timeout=interval_seconds)
                if self.is_runnable() and not stop_event.is_set():
                    self.log.info("Periodic interval elapsed — triggering send.")
                    send_email(int(datetime.datetime.now().timestamp() * 1000))

        if enable_periodic:
            periodic_thread = threading.Thread(target=periodic_sender, daemon=True)
            periodic_thread.start()
            self.log.info("Periodic sender thread started.")
        else:
            periodic_thread = None
            self.log.info("Periodic sending disabled.")

        try:
            with self.in_port1.ContinuousReader(start=self.get_timestamp()) as reader:
                self.log.info("Listening for triggers on in_port1...")
                while self.is_runnable():
                    window_data = reader.read()
                    if not window_data:
                        continue
                    for record in window_data.records:
                        for cv in record.data:
                            if cv.value:
                                self.log.info(f"Trigger received: value={cv.value}, timestamp={record.timestamp}")
                                send_email(record.timestamp)
        finally:
            self.log.info("Shutting down — signalling periodic thread...")
            stop_event.set()
            if periodic_thread and periodic_thread.is_alive():
                periodic_thread.join(timeout=5)
            self.log.info("Shutdown complete.")

    def notify_stop(self):
        pass

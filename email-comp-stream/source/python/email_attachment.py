import smtplib
import csv
import io
import threading
import datetime
from collections import deque
from speedbeesynapse.component.base import HiveComponentBase, HiveComponentInfo, DataType, HiveApiError
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

@HiveComponentInfo(
    uuid='6e56afe5-c2c9-49f1-9b71-1220e7d4add9',
    name='Email Export Attachment (Dataflow)',
    inports=2,
    outports=1
)
class HiveComponent(HiveComponentBase):
    SMTP_HOST = "smtp.default.host"
    SMTP_PORT = 587
    SMTP_USER = "default.user"
    SMTP_PASSWORD = "default.pass"
    SMTP_TIMEOUT = 5
    SMTP_FROM = "default.from"
    SMTP_TO = "default.to"
    SMTP_SUBJECT = "default.subject"
    SMTP_BODY = "default.body"
    ROW_THRESHOLD = 0
    MAX_BUFFER_ROWS = 0

    def main(self, param):
        self.SMTP_HOST       = param['smtphost']
        self.SMTP_PORT       = param['smtpport']
        self.SMTP_USER       = param['smtpusername']
        self.SMTP_PASSWORD   = param['smtppassword']
        self.SMTP_TIMEOUT    = param['timeout']
        self.SMTP_FROM       = param['fromaddress']
        self.SMTP_TO         = param['toaddress']
        self.SMTP_SUBJECT    = param['subjectemail']
        self.SMTP_BODY       = param.get('bodyemail', '')
        self.ROW_THRESHOLD   = int(param.get('rowthreshold', 0))
        self.MAX_BUFFER_ROWS = int(param.get('maxbufferrows', 0))

        self.log.info(f"SMTP_HOST: {self.SMTP_HOST}")
        self.log.info(f"SMTP_PORT: {self.SMTP_PORT}")
        self.log.info(f"SMTP_USER: {self.SMTP_USER}")
        self.log.info(f"SMTP_PASSWORD: {'*' * len(self.SMTP_PASSWORD)}")
        self.log.info(f"SMTP_TIMEOUT: {self.SMTP_TIMEOUT}")
        self.log.info(f"SMTP_FROM: {self.SMTP_FROM}")
        self.log.info(f"SMTP_TO: {self.SMTP_TO}")
        self.log.info(f"SMTP_SUBJECT: {self.SMTP_SUBJECT}")
        self.log.info(f"SMTP_BODY: {self.SMTP_BODY}")
        self.log.info(f"ROW_THRESHOLD: {self.ROW_THRESHOLD} ({'disabled' if self.ROW_THRESHOLD == 0 else 'active'})")
        self.log.info(f"MAX_BUFFER_ROWS: {self.MAX_BUFFER_ROWS} ({'unlimited' if self.MAX_BUFFER_ROWS == 0 else 'capped'})")

        self.logclm = self.out_port1.Column('log', DataType.STRING)

        csv_lock    = threading.Lock()
        csv_headers = []
        csv_rows    = deque(maxlen=self.MAX_BUFFER_ROWS if self.MAX_BUFFER_ROWS > 0 else None)
        stop_event  = threading.Event()

        last_send_lock = threading.Lock()
        last_send_time = [0.0]
        MIN_SEND_INTERVAL = 1.0

        to_list = [addr.strip() for addr in self.SMTP_TO.split(",")]

        def safe_log_insert(message, timestamp):
            try:
                self.logclm.insert(message, timestamp)
            except Exception as log_err:
                self.log.warning(f"logclm.insert failed: {log_err}")

        def send_email(snapshot_headers, snapshot_rows, trigger_timestamp, reason):
            row_count = len(snapshot_rows)
            self.log.info(f"Sending email ({reason}) with {row_count} rows.")
            try:
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=snapshot_headers)
                writer.writeheader()
                writer.writerows(snapshot_rows)
                csv_content = output.getvalue()

                now_str  = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f'data_{now_str}.csv'

                msg = MIMEMultipart()
                msg['From']    = self.SMTP_FROM
                msg['To']      = self.SMTP_TO
                msg['Date']    = formatdate(localtime=True)
                msg['Subject'] = self.SMTP_SUBJECT
                msg.attach(MIMEText(self.SMTP_BODY))

                part = MIMEApplication(csv_content.encode('utf-8'), Name=filename)
                part['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(part)

                server = smtplib.SMTP(self.SMTP_HOST, self.SMTP_PORT, timeout=self.SMTP_TIMEOUT)
                try:
                    server.starttls()
                    server.login(self.SMTP_USER, self.SMTP_PASSWORD)
                    server.sendmail(self.SMTP_FROM, to_list, msg.as_string())
                finally:
                    server.quit()

                self.log.info(f"Email sent successfully ({reason}, {row_count} rows).")
                safe_log_insert(f"Email sent ({reason}): {row_count} rows", trigger_timestamp)

            except Exception as e:
                self.log.error(f"Failed to send email ({reason}), data discarded: {e}")
                safe_log_insert(f"Email FAILED ({reason}): {e}", trigger_timestamp)

        def try_flush(trigger_timestamp, reason):
            now = datetime.datetime.now().timestamp()

            with last_send_lock:
                if now - last_send_time[0] < MIN_SEND_INTERVAL:
                    self.log.warning(f"Burst trigger ignored ({reason}) — too soon after last send.")
                    return False
                last_send_time[0] = now

            with csv_lock:
                if not csv_rows:
                    self.log.warning(f"Flush requested ({reason}) but buffer is empty — skipping.")
                    with last_send_lock:
                        last_send_time[0] = 0.0
                    return False

                snapshot_headers = list(csv_headers)
                snapshot_rows    = list(csv_rows)
                csv_rows.clear()

            send_email(snapshot_headers, snapshot_rows, trigger_timestamp, reason)
            return True

        def data_reader():
            with self.in_port2.ContinuousReader(start=self.get_timestamp()) as reader:
                while self.is_runnable() and not stop_event.is_set():
                    window_data = reader.read()
                    if not window_data:
                        continue

                    for record in window_data.records:
                        ts  = datetime.datetime.fromtimestamp(record.timestamp / 1_000_000_000)
                        row = {"timestamp": str(ts)}

                        for cv in record.data:
                            row[cv.column] = cv.value if cv.value is not None else ""

                        should_send = False
                        with csv_lock:
                            if not csv_headers:
                                csv_headers.extend(row.keys())
                                self.log.info(f"CSV headers captured: {csv_headers}")

                            if self.MAX_BUFFER_ROWS > 0 and len(csv_rows) == csv_rows.maxlen:
                                self.log.warning(f"Buffer full ({self.MAX_BUFFER_ROWS} rows) — oldest row dropped.")

                            csv_rows.append(row)
                            self.log.debug(f"CSV row collected ({len(csv_rows)} total): {row}")

                            if self.ROW_THRESHOLD > 0 and len(csv_rows) >= self.ROW_THRESHOLD:
                                should_send = True

                        if should_send:
                            try_flush(record.timestamp, f"threshold={self.ROW_THRESHOLD}")

        data_thread = threading.Thread(target=data_reader, daemon=True)
        data_thread.start()

        try:
            with self.in_port1.ContinuousReader(start=self.get_timestamp()) as reader:
                while self.is_runnable():
                    window_data = reader.read()
                    if not window_data:
                        continue

                    for record in window_data.records:
                        for cv in record.data:
                            if cv.value:
                                self.log.info(f"Trigger received: {cv.value}")
                                try_flush(record.timestamp, "trigger")

        finally:
            stop_event.set()
            data_thread.join(timeout=5)
            if data_thread.is_alive():
                self.log.warning("Data reader thread did not stop cleanly within timeout.")

    def notify_stop(self):
        pass
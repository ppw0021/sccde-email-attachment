import smtplib
import csv
import io
import threading
import datetime
from speedbeesynapse.component.base import HiveComponentBase, HiveComponentInfo, DataType, HiveApiError
from os.path import basename
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
    server = smtplib.SMTP()
    SMTP_HOST = "smtp.default.host"
    SMTP_PORT = 587
    # SMTP_SSL_TLS = 0
    SMTP_USER = "default.user"
    SMTP_PASSWORD = "default.pass"
    SMTP_TIMEOUT = 5
    SMTP_FROM = "default.from"
    SMTP_TO = "default.to"
    SMTP_SUBJECT = "default.subject"
    SMTP_BODY = "default.body"

    def __init__(self):
        pass
    def __del__(self):
        pass
    def premain(self, param):
        pass
    def postmain(self, param):
        pass

    def main(self, param):
        # Set parameters
        self.SMTP_HOST = param['smtphost']
        self.SMTP_PORT = param['smtpport']
        # self.SMTP_SSL_TLS = param['ssltlsselect']
        self.SMTP_USER = param['smtpusername']
        self.SMTP_PASSWORD = param['smtppassword']
        self.SMTP_TIMEOUT = param['timeout']
        self.SMTP_FROM = param['fromaddress']
        self.SMTP_TO = param['toaddress']
        self.SMTP_SUBJECT = param['subjectemail']
        self.SMTP_BODY = param['bodyemail']

        # Log parameters
        self.log.info(f"SMTP_HOST: {self.SMTP_HOST}")
        self.log.info(f"SMTP_PORT: {self.SMTP_PORT}")
        # self.log.info(f"SMTP_SSL_TLS: {self.SMTP_SSL_TLS}")
        self.log.info(f"SMTP_USER: {self.SMTP_USER}")
        self.log.info(f"SMTP_PASSWORD: {'*' * len(self.SMTP_PASSWORD)}")
        self.log.info(f"SMTP_TIMEOUT: {self.SMTP_TIMEOUT}")
        self.log.info(f"SMTP_FROM: {self.SMTP_FROM}")
        self.log.info(f"SMTP_TO: {self.SMTP_TO}")
        self.log.info(f"SMTP_SUBJECT: {self.SMTP_SUBJECT}")
        self.log.info(f"SMTP_BODY: {self.SMTP_BODY}")

        self.logclm = self.out_port1.Column('log', DataType.STRING)

        # --- Shared CSV buffer ---
        # csv_headers: list of column names, set once from the first window
        # csv_rows: list of dicts, one per record
        csv_lock = threading.Lock()
        csv_headers = []
        csv_rows = []

        # --- Data reader: in_port2 collects all rows into the CSV buffer ---
        def data_reader():
            nonlocal csv_headers
            with self.in_port2.ContinuousReader(start=self.get_timestamp()) as reader:
                while self.is_runnable():
                    window_data = reader.read()
                    if not window_data:
                        continue

                    for record in window_data.records:
                        ts = datetime.datetime.fromtimestamp(record.timestamp / 1000000000)
                        row = {"timestamp": str(ts)}

                        for columnvalue in record.data:
                            row[columnvalue.column] = columnvalue.value if columnvalue.value is not None else ""

                        with csv_lock:
                            # Build headers once from the first row received
                            if not csv_headers:
                                csv_headers = list(row.keys())
                                self.log.info(f"CSV headers captured: {csv_headers}")

                            csv_rows.append(row)
                            self.log.info(f"CSV row collected ({len(csv_rows)} total): {row}")

        data_thread = threading.Thread(target=data_reader, daemon=True)
        data_thread.start()

        # --- Trigger reader: in_port1 fires the email with all collected rows ---
        with self.in_port1.ContinuousReader(start=self.get_timestamp()) as reader:
            while self.is_runnable():
                window_data = reader.read()
                if not window_data:
                    continue

                for record in window_data.records:
                    for columnvalue in record.data:
                        if columnvalue.value:
                            self.log.info(f"Trigger received: {columnvalue.value}")

                            with csv_lock:
                                if not csv_rows:
                                    self.log.warning("Trigger fired but no CSV data collected yet — skipping.")
                                    continue

                                # Build the CSV string in memory
                                output = io.StringIO()
                                writer = csv.DictWriter(output, fieldnames=csv_headers)
                                writer.writeheader()
                                writer.writerows(csv_rows)
                                csv_content = output.getvalue()
                                row_count = len(csv_rows)

                                # Clear the buffer after snapshotting so next trigger gets fresh data
                                csv_rows.clear()
                                csv_headers.clear()

                            self.log.info(f"Sending email with {row_count} rows of CSV data.")

                            try:
                                msg = MIMEMultipart()
                                msg['From'] = self.SMTP_FROM
                                msg['To'] = self.SMTP_TO
                                msg['Date'] = formatdate(localtime=True)
                                msg['Subject'] = self.SMTP_SUBJECT

                                msg.attach(MIMEText(self.SMTP_BODY))

                                filename = f'data_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                                part = MIMEApplication(csv_content.encode('utf-8'), Name=filename)
                                part['Content-Disposition'] = f'attachment; filename="{filename}"'
                                msg.attach(part)

                                server = smtplib.SMTP(self.SMTP_HOST, self.SMTP_PORT, timeout=self.SMTP_TIMEOUT)
                                server.starttls()
                                server.login(self.SMTP_USER, self.SMTP_PASSWORD)
                                server.sendmail(self.SMTP_FROM, self.SMTP_TO, msg.as_string())
                                server.quit()

                                self.log.info(f"Email sent successfully with {row_count} rows.")
                                self.logclm.insert(f"Email sent: {row_count} rows", record.timestamp)

                            except Exception as e:
                                self.log.error(f"Failed to send email: {e}")

    def notify_stop(self):
        pass
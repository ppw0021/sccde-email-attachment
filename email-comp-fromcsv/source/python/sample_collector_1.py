import smtplib
import io
import threading
import datetime
from speedbeesynapse.component.base import HiveComponentBase, HiveComponentInfo, DataType, HiveApiError
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

@HiveComponentInfo(
    uuid='a009b7cd-a2bb-438d-a450-b1640b7fa5b3',
    name='Email File From Stream (Dataflow)',
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

    def main(self, param):
        self.SMTP_HOST     = param['smtphost']
        self.SMTP_PORT     = param['smtpport']
        self.SMTP_USER     = param['smtpusername']
        self.SMTP_PASSWORD = param['smtppassword']
        self.SMTP_TIMEOUT  = param['timeout']
        self.SMTP_FROM     = param['fromaddress']
        self.SMTP_TO       = param['toaddress']
        self.SMTP_SUBJECT  = param['subjectemail']
        self.SMTP_BODY     = param.get('bodyemail', '')

        self.log.info(f"SMTP_HOST: {self.SMTP_HOST}")
        self.log.info(f"SMTP_PORT: {self.SMTP_PORT}")
        self.log.info(f"SMTP_USER: {self.SMTP_USER}")
        self.log.info(f"SMTP_PASSWORD: {'*' * len(self.SMTP_PASSWORD)}")
        self.log.info(f"SMTP_TIMEOUT: {self.SMTP_TIMEOUT}")
        self.log.info(f"SMTP_FROM: {self.SMTP_FROM}")
        self.log.info(f"SMTP_TO: {self.SMTP_TO}")
        self.log.info(f"SMTP_SUBJECT: {self.SMTP_SUBJECT}")
        self.log.info(f"SMTP_BODY: {self.SMTP_BODY}")

        self.logclm = self.out_port1.Column('log', DataType.STRING)

        # Holds the latest file path received from in_port2
        path_lock    = threading.Lock()
        latest_path  = [None]
        stop_event   = threading.Event()

        last_send_lock = threading.Lock()
        last_send_time = [0.0]
        MIN_SEND_INTERVAL = 1.0

        to_list = [addr.strip() for addr in self.SMTP_TO.split(",")]

        def safe_log_insert(message, timestamp):
            try:
                self.logclm.insert(message, timestamp)
            except Exception as log_err:
                self.log.warning(f"logclm.insert failed: {log_err}")

        def send_email(file_path, trigger_timestamp):
            self.log.info(f"Reading file: {file_path}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_content = f.read()
            except Exception as e:
                self.log.error(f"Failed to read file '{file_path}': {e}")
                safe_log_insert(f"File read FAILED: {e}", trigger_timestamp)
                return

            self.log.info(f"Sending email with file: {file_path}")
            try:
                filename = f'data_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

                msg = MIMEMultipart()
                msg['From']    = self.SMTP_FROM
                msg['To']      = self.SMTP_TO
                msg['Date']    = formatdate(localtime=True)
                msg['Subject'] = self.SMTP_SUBJECT
                msg.attach(MIMEText(self.SMTP_BODY))

                part = MIMEApplication(file_content.encode('utf-8'), Name=filename)
                part['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(part)

                server = smtplib.SMTP(self.SMTP_HOST, self.SMTP_PORT, timeout=self.SMTP_TIMEOUT)
                try:
                    server.starttls()
                    server.login(self.SMTP_USER, self.SMTP_PASSWORD)
                    server.sendmail(self.SMTP_FROM, to_list, msg.as_string())
                finally:
                    server.quit()

                self.log.info(f"Email sent successfully with file: {file_path}")
                safe_log_insert(f"Email sent: {file_path}", trigger_timestamp)

            except Exception as e:
                self.log.error(f"Failed to send email, data discarded: {e}")
                safe_log_insert(f"Email FAILED: {e}", trigger_timestamp)

        def try_flush(trigger_timestamp):
            now = datetime.datetime.now().timestamp()

            with last_send_lock:
                if now - last_send_time[0] < MIN_SEND_INTERVAL:
                    self.log.warning("Burst trigger ignored — too soon after last send.")
                    return False
                last_send_time[0] = now

            with path_lock:
                path = latest_path[0]

            if not path:
                self.log.warning("Trigger fired but no file path received yet — skipping.")
                with last_send_lock:
                    last_send_time[0] = 0.0
                return False

            send_email(path, trigger_timestamp)
            return True

        def path_reader():
            with self.in_port2.ContinuousReader(start=self.get_timestamp()) as reader:
                while self.is_runnable() and not stop_event.is_set():
                    window_data = reader.read()
                    if not window_data:
                        continue

                    for record in window_data.records:
                        for cv in record.data:
                            if cv.value:
                                with path_lock:
                                    latest_path[0] = cv.value
                                self.log.info(f"Latest file path updated: {cv.value}")

        path_thread = threading.Thread(target=path_reader, daemon=True)
        path_thread.start()

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
                                try_flush(record.timestamp)

        finally:
            stop_event.set()
            path_thread.join(timeout=5)
            if path_thread.is_alive():
                self.log.warning("Path reader thread did not stop cleanly within timeout.")

    def notify_stop(self):
        pass
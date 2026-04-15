import smtplib
import datetime
import os
import threading
from speedbeesynapse.component.base import HiveComponentBase, HiveComponentInfo, DataType
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate

@HiveComponentInfo(
    uuid='a009b7cd-a2bb-438d-a450-b1640b7fa5b3',
    name='Email File From Stream (Dataflow)',
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
        send_minutes       = int(param.get('sendminutes', 0))
        enable_periodic    = int(param.get('enableperiodic', 0)) == 1

        logclm = self.out_port1.Column('log', DataType.STRING)
        to_list = [addr.strip() for addr in smtp_to.split(",")]
        stop_event = threading.Event()

        def collect_csvs():
            """Walk minute folders from (now - timeframe) to now, merge all CSVs."""
            end_time   = datetime.datetime.now()
            start_time = end_time - datetime.timedelta(hours=send_hours, minutes=send_minutes)

            combined_lines = []
            headers_written = False

            current = start_time.replace(second=0, microsecond=0)
            while current <= end_time:
                folder = os.path.join(
                    save_location,
                    save_location_cont,
                    section,
                    current.strftime("%Y%m"),
                    current.strftime("%d"),
                    current.strftime("%H"),
                    current.strftime("%M")
                )
                self.log.info(f"Checking: {folder}")
                if os.path.isdir(folder):
                    for fname in sorted(os.listdir(folder)):
                        if not fname.lower().endswith('.csv'):
                            continue
                        fpath = os.path.join(folder, fname)
                        try:
                            with open(fpath, 'r', encoding='utf-8') as f:
                                lines = f.readlines()
                            if not lines:
                                continue
                            if not headers_written:
                                combined_lines.extend(lines)
                                headers_written = True
                            else:
                                combined_lines.extend(lines[1:])  # skip header row
                        except Exception as e:
                            self.log.warning(f"Could not read {fpath}: {e}")
                current += datetime.timedelta(minutes=1)

            return ''.join(combined_lines)

        def send_email(trigger_timestamp):
            self.log.info("Collecting CSVs...")
            csv_content = collect_csvs()

            if not csv_content.strip():
                self.log.warning("No CSV data found in time range — skipping send.")
                logclm.insert("No data found — email skipped", trigger_timestamp)
                return

            filename = f'{section}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

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

                server = smtplib.SMTP(smtp_host, smtp_port, timeout=smtp_timeout)
                try:
                    server.starttls()
                    server.login(smtp_user, smtp_password)
                    server.sendmail(smtp_from, to_list, msg.as_string())
                finally:
                    server.quit()

                self.log.info(f"Email sent: {filename}")
                logclm.insert(f"Email sent: {filename}", trigger_timestamp)

            except Exception as e:
                self.log.error(f"Failed to send email: {e}")
                logclm.insert(f"Email FAILED: {e}", trigger_timestamp)

        def periodic_sender():
            interval_seconds = (send_hours * 3600) + (send_minutes * 60)
            if interval_seconds <= 0:
                self.log.warning("Periodic sending enabled but interval is 0 — skipping.")
                return
            while self.is_runnable() and not stop_event.is_set():
                stop_event.wait(timeout=interval_seconds)
                if self.is_runnable() and not stop_event.is_set():
                    self.log.info("Periodic send triggered.")
                    send_email(int(datetime.datetime.now().timestamp() * 1000))

        if enable_periodic:
            periodic_thread = threading.Thread(target=periodic_sender, daemon=True)
            periodic_thread.start()
        else:
            periodic_thread = None

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
                                send_email(record.timestamp)
        finally:
            stop_event.set()
            if periodic_thread and periodic_thread.is_alive():
                periodic_thread.join(timeout=5)

    def notify_stop(self):
        pass

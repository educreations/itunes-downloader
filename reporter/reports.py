from email.mime.multipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
import collections
import contextlib
import csv
import datetime
import glob
import gzip
import os
import smtplib
import ssl
import StringIO
import sys
import tempfile
import time
import urllib

from boto.s3.key import Key
from pygooglechart import Chart, SimpleLineChart, Axis
import envoy
import requests

from utils import TemporaryDirectory


COLUMN_DATE = 9
COLUMN_DOWNLOAD_TYPE = 6
COLUMN_VERSION = 5
COLUMN_DOWNLOADS = 7

DOWNLOAD_TYPE_INSTALL = '1T'
DOWNLOAD_TYPE_UPGRADE = '7T'

S3_PREFIX = 'itunes'


def exclude_headers(iterator):
    for l in iterator:
        if l.startswith('Provider'):
            continue
        elif not l.startswith('APPLE'):
            print('Found a line that we do not recognize:\n{}'.format(l))
            continue
        yield l


def datestr_to_datetime(datestr):
    return datetime.datetime.strptime(datestr, '%m/%d/%Y')


def datetime_to_str(dt):
    return dt.strftime('%Y/%m/%d')


def generate_daily_report(f, upgrades=False):
    """
    Generates a summary of the sales data by day.

    Groups the sales data by date and number of downloads on that day.
    """
    data = collections.OrderedDict()

    cumulative = 0

    for row in sorted(f, key=lambda r: datestr_to_datetime(r[COLUMN_DATE])):
        date = datetime_to_str(datestr_to_datetime(row[COLUMN_DATE]))
        install = row[COLUMN_DOWNLOAD_TYPE]
        downloads = int(row[COLUMN_DOWNLOADS])

        if install != DOWNLOAD_TYPE_INSTALL:
            continue

        if date not in data:
            data[date] = (0, cumulative)

        day, cum = data[date]
        cumulative += downloads

        data[date] = (day + downloads, cumulative)

    return data


def generate_weekly_report(f):
    """
    Generates a summary of the sales data by week.

    Groups the sales data by date and number of downloads in that week.
    """
    data = collections.OrderedDict()

    cumulative = 0

    for row in sorted(f, key=lambda r: datestr_to_datetime(r[COLUMN_DATE])):
        dt = datestr_to_datetime(row[COLUMN_DATE])
        weekdt = datetime.datetime.strptime('{} {} 1'.format(dt.year, dt.isocalendar()[1]), '%Y %W %w')
        date = datetime_to_str(weekdt)
        install = row[COLUMN_DOWNLOAD_TYPE]
        downloads = int(row[COLUMN_DOWNLOADS])

        if install != DOWNLOAD_TYPE_INSTALL:
            continue

        if date not in data:
            data[date] = (0, cumulative)

        week, cum = data[date]
        cumulative += downloads

        data[date] = (week + downloads, cumulative)

    # Sort the data
    data = collections.OrderedDict(sorted(data.items(), key=lambda i: i[0]))

    return data


def get_and_store_latest_report(bucket, login, password, vendorid, dry_run=False, verbose=False):
    # Fetch the latest download report and upload it to S3
    with TemporaryDirectory() as dir:
        pwd = os.path.dirname(__file__)

        if verbose:
            print('Copying the ingestor class...')
        command = "cp {pwd}/Autoingestion.class {dir}/Autoingestion.class".format(
            dir=dir, pwd=pwd)
        r = envoy.run(command)
        if r.status_code != 0:
            raise Exception('There was an error running: {}'.format(command))

        # Setup the CWD
        oldcwd = os.getcwd()
        os.chdir(dir)

        if verbose:
            print('Retreiving the latest daily report...')
        command = "java Autoingestion {login} {password} {vendorid} Sales Daily Summary".format(
            login=login,
            password=password,
            vendorid=vendorid,
        )
        r = envoy.run(command)
        if r.status_code != 0:
            raise Exception('There was an error running: {}'.format(command))

        # Get the name of the file that was downloaded
        files = glob.glob('{}/S_D_{}_*.txt.gz'.format(dir, vendorid))
        if not files:
            raise Exception('Unable to find a downloaded data file!')

        filepath = files[0]
        filename = os.path.basename(filepath)

        if verbose:
            print('The latest report is {}. Saving to S3...'.format(filename))

        # Upload the report to S3
        key = Key(bucket)
        key.key = '{}/{}'.format(S3_PREFIX, filename)
        if not dry_run:
            key.set_contents_from_filename(filepath, replace=True)

        os.chdir(oldcwd)


def generate_report_from_files(bucket, verbose=False):
    # Generate a summary report
    if verbose:
        print('Generating a summary report.')

    # For every file in the bucket directory, unzip it and add it to a temporary file
    with tempfile.TemporaryFile() as summary:
        for key in bucket.list(prefix=S3_PREFIX):
            if verbose:
                sys.stdout.write('.')
                sys.stdout.flush()
            key.open('r')
            with contextlib.closing(StringIO.StringIO(key.read())) as s, gzip.GzipFile(fileobj=s) as gz:
                summary.write(gz.read())

        if verbose:
            print(' done.')

        summary.seek(0)

        reader = csv.reader(exclude_headers(summary), delimiter='\t')
        report = generate_daily_report(reader)
        #report = generate_weekly_report(reader)

        return report


def link_for_latest_report(bucket, verbose=False):
    keys = sorted(bucket.list(prefix=S3_PREFIX), key=lambda k: k.name)

    key = keys[-1]

    return key.generate_url(expires_in=60 * 60 * 24 * 365)


def email_report(email, download_link, report, host, port, login=None, password=None, dry_run=False, verbose=False):
    daily = [v[0] for k, v in report.items()]
    cumulative = [v[1] for k, v in report.items()]

    width, height = 700, 300

    # Create the charts
    daily_chart = SimpleLineChart(width, height)
    cumulative_chart = SimpleLineChart(width, height)

    # Titles
    daily_chart.set_title('Daily Downloads')
    cumulative_chart.set_title('Cumulative Downloads')

    # Add data
    daily_chart.add_data(daily)
    daily_chart.set_axis_range(Axis.LEFT, 0, max(daily))
    daily_chart.set_axis_labels(Axis.RIGHT, [min(daily), max(daily)])

    cumulative_chart.add_data(cumulative)
    cumulative_chart.set_axis_range(Axis.LEFT, 0, max(cumulative))
    cumulative_chart.set_axis_labels(Axis.RIGHT, [min(cumulative), max(cumulative)])

    # Set the styling
    marker = ('B', 'C5D4B5BB', '0', '0', '0')
    colors = ['3D7930', 'FF9900']

    daily_chart.markers.append(marker)
    cumulative_chart.markers.append(marker)

    daily_chart.set_colours(colors)
    cumulative_chart.set_colours(colors)

    grid_args = 0, 10
    grid_kwargs = dict(line_segment=2, blank_segment=6)
    daily_chart.set_grid(*grid_args, **grid_kwargs)
    cumulative_chart.set_grid(*grid_args, **grid_kwargs)

    #daily_chart.fill_linear_stripes(Chart.CHART, 0, 'CCCCCC', 0.2, 'FFFFFF', 0.2)

    daily_chart_url = daily_chart.get_url()
    cumulative_chart_url = cumulative_chart.get_url()

    # Create recent versions of the charts
    daily_chart.data = [daily[-90:]]
    cumulative_chart.data = [cumulative[-90:]]
    daily_chart.set_title('Recent Daily Downloads')
    cumulative_chart.set_title('Recent Cumulative Downloads')

    daily_recent_chart_url = daily_chart.get_url()
    cumulative_recent_chart_url = cumulative_chart.get_url()

    if verbose:
        print('Daily: ' + daily_chart_url)
        print('Cumulative: ' + cumulative_chart_url)
        print('Daily Recent: ' + daily_recent_chart_url)
        print('Cumulative Recent: ' + cumulative_recent_chart_url)

    # Create the body of the message (a plain-text and an HTML version).
    text = "Get an HTML mail client."
    html = """\
<html>
    <body>
        <h2>Latest download count: {latest}</h2>
        <h2>Latest cumulative total: {cumulative}</h2>
        <p><a href="{download}">Download today's report</a>.</p>
        <p><img src="cid:daily.png" width="{width}" height="{height}" alt="Daily Downloads" /></p>
        <p><img src="cid:cumulative.png" width="{width}" height="{height}" alt="Cumulative Downloads" /></p>
        <p><img src="cid:daily-recent.png" width="{width}" height="{height}" alt="Recent Daily Downloads" /></p>
        <p><img src="cid:cumulative-recent.png" width="{width}" height="{height}" alt="Recent Cumulative Downloads" /></p>
    </body>
</html>""".format(
        latest=daily[-1],
        cumulative=cumulative[-1],
        download=download_link,
        width=width,
        height=height,
    )

    # Create message container - the correct MIME type is multipart/alternative.
    message_root = MIMEMultipart('related')
    message_root['Subject'] = "Daily iTunes Download Report"
    message_root['From'] = email
    message_root['To'] = email
    message_root.preamble = 'This is a multi-part message.'

    # Record the MIME types of both parts - text/plain and text/html.
    alternative = MIMEMultipart('alternative')
    message_root.attach(alternative)

    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    alternative.attach(part1)
    alternative.attach(part2)

    # Get the images
    r = requests.get(daily_chart_url)
    img = MIMEImage(r.content)
    img.add_header('Content-ID', '<daily.png>')
    message_root.attach(img)

    r = requests.get(cumulative_chart_url)
    img = MIMEImage(r.content)
    img.add_header('Content-ID', '<cumulative.png>')
    message_root.attach(img)

    r = requests.get(daily_recent_chart_url)
    img = MIMEImage(r.content)
    img.add_header('Content-ID', '<daily-recent.png>')
    message_root.attach(img)

    r = requests.get(cumulative_recent_chart_url)
    img = MIMEImage(r.content)
    img.add_header('Content-ID', '<cumulative-recent.png>')
    message_root.attach(img)

    try:
        # Send the message via local SMTP server.
        s = smtplib.SMTP(host, port)
        s.starttls()
        s.login(login, password)

        # sendmail function takes 3 arguments: sender's address, recipient's address
        # and message to send - here it is sent as one string.
        s.sendmail(email, [email], message_root.as_string())
        s.quit()
    except (ssl.SSLError, smtplib.SMTPServerDisconnected):
        print('Error')
        s.close()

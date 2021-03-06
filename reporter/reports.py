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

from boto.s3.key import Key
from pygooglechart import SimpleLineChart, Axis
import envoy
import requests

from utils import TemporaryDirectory


COLUMN_DATE = 9
COLUMN_DOWNLOAD_TYPE = 6
COLUMN_VERSION = 5
COLUMN_DOWNLOADS = 7
COLUMN_PROMO = 16

DOWNLOAD_TYPE_INSTALL = '1T'
DOWNLOAD_TYPE_UPGRADE = '7T'

PROMO_TYPE_EDU = 'EDU'

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


def _iter_sorted_report(reader):
    return sorted(reader, key=lambda r: datestr_to_datetime(r[COLUMN_DATE]))


def _entry_row_for_date(data, row, date, cumulative):
    units_type = row[COLUMN_DOWNLOAD_TYPE]
    downloads = int(row[COLUMN_DOWNLOADS])
    edu_promo = row[COLUMN_PROMO] == PROMO_TYPE_EDU

    install_count = 0
    upgrade_count = 0
    educational_count = 0
    if units_type == DOWNLOAD_TYPE_INSTALL:
        install_count = downloads
        if edu_promo:
            educational_count = downloads
    elif units_type == DOWNLOAD_TYPE_UPGRADE:
        upgrade_count = downloads
    else:
        return 0

    if date not in data:
        data[date] = (0, 0, 0, cumulative)

    day, updates, edu, cum = data[date]

    data[date] = (day + install_count, updates + upgrade_count, edu + educational_count, cumulative + install_count)

    return install_count


def generate_daily_report(f, upgrades=False):
    """
    Generates a summary of the sales data by day.

    Groups the sales data by date and number of downloads on that day.
    """
    data = collections.OrderedDict()

    f.seek(0)
    reader = csv.reader(exclude_headers(f), delimiter='\t')

    cumulative = 0

    for row in _iter_sorted_report(reader):
        date = datetime_to_str(datestr_to_datetime(row[COLUMN_DATE]))

        cumulative += _entry_row_for_date(data, row, date, cumulative)

    return data


def generate_weekly_report(f, upgrades=False):
    """
    Generates a summary of the sales data by week.

    Groups the sales data by date and number of downloads in that week.
    """
    data = collections.OrderedDict()

    f.seek(0)
    reader = csv.reader(exclude_headers(f), delimiter='\t')

    cumulative = 0

    for row in _iter_sorted_report(reader):
        dt = datestr_to_datetime(row[COLUMN_DATE])
        weekdt = datetime.datetime.strptime('{} {} 0'.format(dt.year, dt.isocalendar()[1]), '%Y %W %w')
        date = datetime_to_str(weekdt)

        cumulative += _entry_row_for_date(data, row, date, cumulative)

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


def _concatenate_reports_in_bucket(bucket, dest, verbose=False):
    """Concatenate the report files in `bucket` into dest."""
    if verbose:
        print('Parsing download reports from the files in {}...'.format(
            bucket.name))

    dest.seek(0)

    for key in bucket.list(prefix=S3_PREFIX):
        if 'S_D_' not in key.name:
            continue
        if verbose:
            sys.stdout.write('.')
            sys.stdout.flush()
        key.open('r')
        with contextlib.closing(StringIO.StringIO(key.read())) as s, gzip.GzipFile(fileobj=s) as gz:
            dest.write(gz.read())

    if verbose:
        print(' done fetching download reports.')


def _reports_from_source(source, daily=False, weekly=False, verbose=False):
    """Generate daily and weekly reports from a source file."""
    if verbose:
        print('Generating reports from source file...')
    source.seek(0)

    daily_report = generate_daily_report(source) if daily else None
    weekly_report = generate_weekly_report(source) if weekly else None

    print(' done.')

    return daily_report, weekly_report


def generate_reports_from_files(
        bucket, verbose=False, daily=False, weekly=False):
    """Generate a summary report from `bucket`.

    Generate daily and / or weekly summary reports.

    Returns tuple of daily_report, weekly_report
    """

    # For every file in the bucket directory, unzip it and add it to a
    # temporary file
    with tempfile.TemporaryFile() as summary:
        _concatenate_reports_in_bucket(
            bucket=bucket, dest=summary, verbose=verbose)

        daily_report, weekly_report = _reports_from_source(
            summary,
            daily=daily,
            weekly=weekly,
            verbose=verbose,
        )

        return daily_report, weekly_report


def link_for_latest_report(bucket, verbose=False):
    keys = sorted(bucket.list(prefix=S3_PREFIX), key=lambda k: k.name)

    key = keys[-1]

    return key.generate_url(expires_in=60 * 60 * 24 * 365)


def email_report(email, download_link, daily_report, weekly_report,
                 host, port, login=None, password=None, dry_run=False,
                 verbose=False):
    daily = [v[0] for k, v in daily_report.items()] if daily_report else []
    daily_updates = daily_report.items()[-1][1][1]
    daily_edu = daily_report.items()[-1][1][2]
    weekly = [v[0] for k, v in weekly_report.items()] if weekly_report else []

    cumulative_data = daily_report if daily_report else weekly_report
    if cumulative_data is None:
        raise Exception("No data given to generate a cumulative report!")
    cumulative = [v[3] for k, v in cumulative_data.items()]

    width, height = 700, 300

    # Create the charts
    daily_chart = SimpleLineChart(width, height)
    weekly_chart = SimpleLineChart(width, height)
    cumulative_chart = SimpleLineChart(width, height)

    # Titles
    daily_chart.set_title('Daily Downloads')
    weekly_chart.set_title('Weekly Downloads')
    cumulative_chart.set_title('Cumulative Downloads')

    # Add data
    if daily:
        daily_data = daily[-180:]
        daily_chart.add_data(daily_data)
        daily_chart.set_axis_range(Axis.LEFT, 0, max(daily_data))
        daily_chart.set_axis_labels(Axis.RIGHT, [min(daily_data), max(daily_data)])

    if weekly:
        weekly_chart.add_data(weekly)
        weekly_chart.set_axis_range(Axis.LEFT, 0, max(weekly))
        weekly_chart.set_axis_labels(Axis.RIGHT, [min(weekly), max(weekly)])

    cumulative_chart.add_data(cumulative)
    cumulative_chart.set_axis_range(Axis.LEFT, 0, max(cumulative))
    cumulative_chart.set_axis_labels(
        Axis.RIGHT, [min(cumulative), max(cumulative)])

    # Set the styling
    marker = ('B', 'C5D4B5BB', '0', '0', '0')
    colors = ['3D7930', 'FF9900']

    daily_chart.markers.append(marker)
    weekly_chart.markers.append(marker)
    cumulative_chart.markers.append(marker)

    daily_chart.set_colours(colors)
    weekly_chart.set_colours(colors)
    cumulative_chart.set_colours(colors)

    grid_args = 0, 10
    grid_kwargs = dict(line_segment=2, blank_segment=6)
    daily_chart.set_grid(*grid_args, **grid_kwargs)
    weekly_chart.set_grid(*grid_args, **grid_kwargs)
    cumulative_chart.set_grid(*grid_args, **grid_kwargs)

    #daily_chart.fill_linear_stripes(
    #   Chart.CHART, 0, 'CCCCCC', 0.2, 'FFFFFF', 0.2)

    daily_chart_url = daily_chart.get_url() if daily else None
    weekly_chart_url = weekly_chart.get_url() if weekly else None
    cumulative_chart_url = cumulative_chart.get_url()

    # Create recent versions of the charts
    if daily:
        recent_daily = daily[-90:]

        # Get last year's daily data. First, get the first date for the daily
        # data.
        start = daily_report.items()[-90][0]
        dt = datetime.datetime.strptime(start, '%Y/%m/%d')
        dt = dt - datetime.timedelta(weeks=52)
        last_year_datestr = datetime_to_str(dt)

        # Get the index in the data for the datestr
        try:
            i = daily_report.keys().index(last_year_datestr)
            recent_daily_comparison = daily[i:i + 90]
        except ValueError:
            recent_daily_comparison = []

        if recent_daily_comparison:
            daily_chart.data = [recent_daily, recent_daily_comparison]
        else:
            daily_chart.data = [recent_daily]
        # Reset the axes
        daily_chart.axis = []
        min_daily = min(recent_daily + recent_daily_comparison)
        max_daily = max(recent_daily + recent_daily_comparison)
        daily_chart.set_axis_range(Axis.LEFT, 0, max_daily)
        daily_chart.set_axis_labels(Axis.RIGHT, [min_daily, max_daily])
        daily_chart.set_title('Recent Daily Downloads (filled is now)')

        daily_recent_chart_url = daily_chart.get_url()
    else:
        daily_recent_chart_url = None

    if verbose:
        print('Daily: ' + daily_chart_url) if daily_chart_url else None
        print('Weekly: ' + weekly_chart_url) if weekly_chart_url else None
        print('Cumulative: ' + cumulative_chart_url)
        print('Daily Recent: ' + daily_recent_chart_url) if daily_recent_chart_url else None

    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    # Create the body of the message (a plain-text and an HTML version).
    text = "Get an HTML mail client."
    html = """\
<html>

<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <meta name="viewport" content="width=600" />
    <style>
        @media only screen and (min-device-width: 541px) {{
            .content {{
                width: 540px !important;
            }}
        }}
    </style>
</head>

<body>

<table class="content" align="center" cellpadding="0" cellspacing="0" border="0" style="width: 100%; max-width: 540px; border: 1px solid #cbcbcb;">
    <tr>
        <td height="32" style="text-align: center; background-color:#fe8359;height:32px;color:#fff" bgcolor="fe8359">
            <span style="font-size:11px">Daily Report for</span>
            <span style="font-size:14px;font-weight:bold">{yesterday:%A, %B %d, %Y}</span>
        </td>
    </tr>
    <tr>
        <td height="30" valign="middle">
            <table cellpadding="0" cellspacing="0" border="0" style="width: 100%; text-align: center">
                <tr>
                    <td style="margin-top: 5px;">
                        <span style="font-size: 10px; color: #777">Downloads:</span>
                        <div style="font-size: 15px; margin-top: 3px; "><b>{latest_daily:,}</b></div>
                    </td>
                    <td style="margin-top: 5px;">
                        <span style="font-size: 10px; color: #777">Updates:</span>
                        <div style="font-size: 15px; margin-top: 3px; "><b>{latest_updates:,}</b></div>
                    </td>
                    <td style="margin-top: 5px;">
                        <span style="font-size: 10px; color: #777">Educational:</span>
                        <div style="font-size: 15px; margin-top: 3px; "><b>{latest_edu:,}</b></div>
                    </td>
                    <td style="margin-top: 5px;">
                        <span style="font-size: 10px; color: #777">Since Monday:</span>
                        <div style="font-size: 15px; margin-top: 3px; "><b>{latest_weekly:,}</b></div>
                    </td>
                    <td style="margin-top: 5px;">
                        <span style="font-size: 10px; color: #777">Cumulative:</span>
                        <div style="font-size: 15px; margin-top: 3px; "><b>{cumulative:,}</b></div>
                    </td>
                </tr>
            </table>
        </td>
    </tr>
    <tr>
        <td></td>
    </tr>
    <tr>
        <td><hr color="#cbcbcb"></td>
    </tr>

    <tr><td style="padding: 5px;"><img src="cid:daily.png" width="{width}" height="{height}" alt="Daily Downloads" /></td></tr>
    <tr><td style="padding: 5px;"><img src="cid:weekly.png" width="{width}" height="{height}" alt="Weekly Downloads" /></td></tr>
    <tr><td style="padding: 5px;"><img src="cid:cumulative.png" width="{width}" height="{height}" alt="Cumulative Downloads" /></td></tr>
    <tr><td style="padding: 5px;"><img src="cid:daily-recent.png" width="{width}" height="{height}" alt="Recent Daily Downloads" /></td></tr>

    <tr>
        <td><hr color="#cbcbcb"></td>
    </tr>
    <tr>
        <td style="padding: 10px; text-align: center;">
            <a style="color: #777" href="{download}">Download today's report</a>
        </td>
    </tr>
</table>


</body>

</html>""".format(
        yesterday=yesterday,
        latest_daily=daily[-1] if daily else 0,
        latest_weekly=weekly[-1] if weekly else 0,
        latest_updates=daily_updates,
        latest_edu=daily_edu,
        cumulative=cumulative[-1],
        download=download_link,
        width=width,
        height=height,
    )

    # Create message container - the correct MIME type is multipart/alternative.
    message_root = MIMEMultipart('related')
    message_root['Subject'] = "iTunes Report for {:%A, %B %d, %Y}".format(yesterday)
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
    if daily_chart_url:
        r = requests.get(daily_chart_url)
        img = MIMEImage(r.content, _subtype='png')
        img.add_header('Content-ID', '<daily.png>')
        message_root.attach(img)

    if weekly_chart_url:
        r = requests.get(weekly_chart_url)
        img = MIMEImage(r.content, _subtype='png')
        img.add_header('Content-ID', '<weekly.png>')
        message_root.attach(img)

    r = requests.get(cumulative_chart_url)
    img = MIMEImage(r.content, _subtype='png')
    img.add_header('Content-ID', '<cumulative.png>')
    message_root.attach(img)

    if daily_recent_chart_url:
        r = requests.get(daily_recent_chart_url)
        img = MIMEImage(r.content, _subtype='png')
        img.add_header('Content-ID', '<daily-recent.png>')
        message_root.attach(img)

    try:
        # Send the message via local SMTP server.
        s = smtplib.SMTP(host, port)
        s.starttls()
        s.login(login, password)

        # sendmail function takes 3 arguments: sender's address,
        # recipient's address and message to send - here it is sent as one
        # string.
        s.sendmail(email, [email], message_root.as_string())
        s.quit()
    except (ssl.SSLError, smtplib.SMTPServerDisconnected):
        print('Error')
        s.close()

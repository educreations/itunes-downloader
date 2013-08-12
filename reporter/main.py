#!/usr/bin/env python

from optparse import OptionParser
import os
import sys

from boto.s3.connection import S3Connection, OrdinaryCallingFormat

from reports import get_and_store_latest_report, generate_reports_from_files, link_for_latest_report, email_report


if __name__ == '__main__':
    optparser = OptionParser()
    optparser.add_option("-q", "--quiet", dest="verbose", action="store_false", default=True, help="Be verbose.")
    optparser.add_option("-d", "--dry-run", dest="dry_run", action="store_true", help="Dry run.")

    # Actions
    optparser.add_option("--download", dest="download", action="store_true", default=False, help="Download the latest report data.")
    optparser.add_option("--daily-summary", dest="daily", action="store_true", default=False, help="Generate a daily summary.")
    optparser.add_option("--weekly-summary", dest="weekly", action="store_true", default=False, help="Generate a weekly summary.")

    # Report destination
    optparser.add_option("-e", "--email", dest="email", default=os.getenv('MAILTO'), help="The email to send to.")

    # iTunes Connect Options
    optparser.add_option("-l", "--login", dest="login", default=os.getenv('ITUNES_CONNECT_LOGIN'), help="The apple login.")
    optparser.add_option("-p", "--password", dest="password", default=os.getenv('ITUNES_CONNECT_PASSWORD'), help="The apple password.")
    optparser.add_option("-v", "--vendorid", dest="vendorid", default=os.getenv('ITUNES_CONNECT_VENDORID'), help="The apple vendor ID.")

    # AWS options
    optparser.add_option("-k", "--key", dest="key", default=os.getenv('AWS_ACCESS_KEY_ID'), help="The AWS access key")
    optparser.add_option("-s", "--secret", dest="secret", default=os.getenv('AWS_SECRET_ACCESS_KEY'), help="The AWS access secret")
    optparser.add_option("-b", "--bucket", dest="bucket", default=os.getenv('AWS_BUCKET'), help="The AWS bucket.")

    # SMTP options
    optparser.add_option("--smtp-host", dest="smtp_host", default=os.getenv('SMTP_HOST'), help="The SMTP host.")
    optparser.add_option("--smtp-port", dest="smtp_port", default=os.getenv('SMTP_PORT', 25), help="The SMTP port.")
    optparser.add_option("--smtp-login", dest="smtp_login", default=os.getenv('SMTP_LOGIN', None), help="The SMTP host login.")
    optparser.add_option("--smtp-password", dest="smtp_password", default=os.getenv('SMTP_PASSWORD', None), help="The SMTP host password.")

    (options, args) = optparser.parse_args()

    verbose = options.verbose

    for k in ('login', 'password', 'vendorid', 'key', 'secret', 'bucket', 'smtp_host', ):
        if not hasattr(options, k) or not getattr(options, k):
            print('--{} is a required option.'.format(k.replace('_', '-')))
            optparser.print_help()
            sys.exit()

    s3 = S3Connection(options.key, options.secret, calling_format=OrdinaryCallingFormat())
    bucket = s3.get_bucket(options.bucket)

    if options.download:
        get_and_store_latest_report(
            bucket=bucket,
            login=options.login,
            password=options.password,
            vendorid=options.vendorid,
            dry_run=options.dry_run,
            verbose=verbose,
        )

    if options.daily or options.weekly:
        daily_report, weekly_report = generate_reports_from_files(
            bucket=bucket,
            verbose=verbose,
            daily=options.daily,
            weekly=options.weekly,
        )
    else:
        daily_report, weekly_report = None, None

    if daily_report is not None or weekly_report is not None:

        # Print the report data to the console
        if options.verbose:
            for name, report in {'daily': daily_report, 'weekly': weekly_report}.iteritems():
                if report is None:
                    continue
                output = """
===================================
      {} Download Numbers
===================================
""".format(name.title()) + '\n'.join(
                    ['Date\tCount\tCumulative'] +
                    [
                        '{}\t{}\t{}'.format(k, *v)
                        for k, v in report.iteritems()
                    ]
                )
                print(output)

        download_link = link_for_latest_report(bucket, verbose=verbose)

        if options.email:
            email_report(
                email=options.email,
                download_link=download_link,
                daily_report=daily_report,
                host=options.smtp_host,
                port=options.smtp_port,
                login=options.smtp_login,
                password=options.smtp_password,
                dry_run=options.dry_run,
                verbose=verbose,
            )

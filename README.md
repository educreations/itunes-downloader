iTunes Connect Downloader
=========================

This small app will download reports from iTunes Connect on a daily basis
and upload those reports to S3. After uploading to S3, it can also run a
summary report over all those files. The summary report will generate
daily download and cumulative download graphs which are then emailed to
a specific email address.


Installation / Setup / Usage
----------------------------

This is designed to run on Heroku using
the [Heroku Scheduler](https://devcenter.heroku.com/articles/scheduler) on
a daily basis. Add the scheduler add-on to your app with:

```bash
heroku addons:add scheduler:standard
```

You'll want to make sure several environment variables are defined, or you can
pass everything in as flags to the `main.py` script. To set up this script to
run on Heroku, you can set the config for the app as follows:

```bash
heroku config:set ITUNES_CONNECT_LOGIN="..." \
    ITUNES_CONNECT_PASSWORD="..." \
    ITUNES_CONNECT_VENDORID="..." \
    AWS_ACCESS_KEY_ID="..." \
    AWS_SECRET_ACCESS_KEY="..." \
    AWS_BUCKET="bucket-name" \
    MAILTO="email@example.com" \
    SMTP_HOST="mail.example.com" \
    SMTP_PORT=587 \
    SMTP_LOGIN="user@example.com" \
    SMTP_PASSWORD="..."
```

If you've set up your environment correctly, you can invoke the script with the
following in your scheduler config. This will generate both daily and weekly
reports.

```bash
python reporter/main.py --download --daily-summary --weekly-summary -q
```

You can also run the report manually by executing the following command:

```bash
heroku run python reporter/main.py --download --daily-summary --weekly-summary -q
```

import socket

__author__ = 'msivak'

import smtplib
import ConfigParser
import re
import os
import logging

from . import constants

# regular expression used to split the email addresses
EMAIL_SPLIT_RE = re.compile(' *, *')


def send_email(cfg, message):
    """Send email."""

    try:
        server = smtplib.SMTP(cfg["smtp-server"], port=cfg["smtp-port"])
        server.set_debuglevel(1)
        to_addresses = EMAIL_SPLIT_RE.split(cfg["destination-emails"].strip())
        server.sendmail(cfg["source-email"],
                        to_addresses,
                        message)
        server.quit()
        return True
    except (smtplib.SMTPException, socket.error) as e:
        logging.getLogger("%s.Notifications" % __name__).exception(e)
        return False


def notify(**kwargs):
    """Try sending a notification to the configured addresses. If the
    configuration does not contain a matching rule, do nothing.

    The configuration is refreshed with every call of this method.
    """
    logging.getLogger("%s.Notifications" % __name__)\
        .debug("nofity: %s" % (repr(kwargs),))

    assert "type" in kwargs
    type = kwargs["type"]

    cfg = ConfigParser.SafeConfigParser()
    cfg.read(constants.NOTIFY_CONF_FILE)

    detail = kwargs.get("detail", "")

    try:
        rules = cfg.get("notify", type)
        # only send emails for messages we want
        if not re.search(rules.lower(), detail.lower()):
            return False
    except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
        return False

    try:
        template_path = os.path.join(constants.NOTIFY_TEMPLATES, type+".txt")
        template = open(template_path).read()
    except (OSError, IOError) as e:
        logging.getLogger("%s.Notifications" % __name__).exception(e)
        return False

    # default SMTP configuration
    smtp_config = {
        "destination-emails": "root@localhost",
        "source-email": "root@localhost",
        "smtp-server": "localhost",
        "smtp-port": 25
    }

    # read SMTP configuration from the notification config file
    try:
        smtp_config.update(cfg.items("email"))
    except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
        pass

    # pass SMTP configuration to the formatting dictionary
    # so we can use email addresses in the templates
    for k, v in smtp_config.iteritems():
        kwargs.setdefault(k, v)

    # fill in the values to the template
    try:
        email_body = template.format(**kwargs)
    except KeyError as e:
        logging.getLogger("%s.Notifications" % __name__).exception(e)
        return False

    return send_email(smtp_config, email_body)

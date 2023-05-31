# Post to Slack using native AWS Lambda modules
def _post_to_slack(channel, text, blocks, attachments):
    url = "https://slack.com/api/chat.postMessage"
    data = urllib.parse.urlencode(
        (
            ("token", SLACK_TOKEN),
            ("channel", channel),
            ("text", text),
            ("blocks", blocks),
            ("attachments", attachments),
        )
    )
    data = data.encode("ascii")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    request = urllib.request.Request(url, data, headers)
    resp = urllib.request.urlopen(request)
    result = resp.read()
    return json.loads(result.decode("UTF-8"))
 

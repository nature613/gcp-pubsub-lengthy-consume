## Overview

This is an example of consuming pubsub messages in GCP with the potential of having a very long processing time for each message. This is based on the official example provided [here](https://godoc.org/cloud.google.com/go/pubsub/apiv1#example-SubscriberClient-Pull-LengthyClientProcessing).

The code will create the topic `lengthytesttopic` and a subscription `lengthytestsubscription` of that topic with a 20s deadline. Then it will spin up a goroutine that consumes messages from the subscription. For every batch of pulled messages, a "deadline extender" goroutine is also started with a 10s grace period that will keep extending the deadline while the message is being processed. Then the main routine sends two messages with the contents being the number of seconds to which each message is to be processed, waits for a little bit before terminating.

## Requirements

Information on authenticating the pubsub SDK can be found [here](https://cloud.google.com/pubsub/docs/reference/libraries).

The code also requires the `GCP_PROJECT_ID` environment variable for your GCP project ID. You can run the sample like:

```bash
$ go build -v
$ export GCP_PROJECT_ID=myprojectid
$ ./gcp-pubsub-lengthy-consume

# or

$ go build -v
$ GCP_PROJECT_ID=myprojectid ./gcp-pubsub-lengthy-consume
```

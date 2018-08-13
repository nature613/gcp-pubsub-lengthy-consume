## Overview

This is an example of consuming pubsub messages in GCP with the potential of having a very long processing time for each message. This is based on the official example provided [here](https://godoc.org/cloud.google.com/go/pubsub/apiv1#example-SubscriberClient-Pull-LengthyClientProcessing). 

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

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	gpubsub "cloud.google.com/go/pubsub"
	pubsubv1 "cloud.google.com/go/pubsub/apiv1"
	"github.com/pkg/errors"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

func startLengthy(projectId, sub string) error {
	ctx := context.TODO()
	client, err := pubsubv1.NewSubscriberClient(ctx)
	if err != nil {
		return err
	}

	defer client.Close()

	subname := fmt.Sprintf("projects/%v/subscriptions/%v", projectId, sub)

	req := pubsubpb.PullRequest{
		Subscription: subname,
		MaxMessages:  1,
	}

	log.Printf("start subscription listen on %v", subname)

	for {
		res, err := client.Pull(ctx, &req)
		if err != nil {
			log.Printf("client pull failed, err=%v", err)
			continue
		}

		// Pull() returns an empty list if there are no messages available in the
		// backlog. We should skip processing steps when that happens.
		if len(res.ReceivedMessages) == 0 {
			continue
		}

		var ids []string

		for _, m := range res.ReceivedMessages {
			ids = append(ids, m.AckId)
		}

		var finishc = make(chan error)
		var delay = 0 * time.Second // tick immediately upon reception
		var ackDeadline = time.Second * 20

		// Continuously notify the server that processing is still happening on this batch.
		go func() {
			defer log.Printf("ack extender done for %v", ids)

			for {
				select {
				case <-ctx.Done():
					return
				case <-finishc:
					return
				case <-time.After(delay):
					log.Printf("modify ack deadline for %vs, ids=%v", ackDeadline.Seconds(), ids)

					err := client.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
						Subscription:       subname,
						AckIds:             ids,
						AckDeadlineSeconds: int32(ackDeadline.Seconds()),
					})

					if err != nil {
						log.Printf("failed in ack deadline extend, err=%v", err)
					}

					delay = ackDeadline - 10*time.Second // 10 seconds grace period
				}
			}
		}()

		var wg sync.WaitGroup

		wg.Add(len(res.ReceivedMessages))

		// Process each message concurrently.
		for _, msg := range res.ReceivedMessages {
			go func(rm *pubsubpb.ReceivedMessage) {
				defer wg.Done()
				starttime := time.Now()

				log.Printf("payload=%v, ids=%v", string(rm.Message.Data), ids)

				// In this example, the message we are receiving is the number of
				// seconds we will "do the work".
				delta, err := strconv.Atoi(string(rm.Message.Data))
				if err == nil {
					time.Sleep(time.Second * time.Duration(delta))
				}

				log.Printf("pubsub processing took %v", time.Since(starttime))

				err = client.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
					Subscription: subname,
					AckIds:       []string{rm.AckId},
				})

				if err != nil {
					log.Printf("ack failed, err=%v", err)
				}
			}(msg)
		}

		wg.Wait()      // wait for all message processing to be done
		close(finishc) // terminate our ack extender goroutine
	}
}

// getTopic retrieves a PubSub topic. It creates the topic if it doesn't exist.
func getTopic(project, id string) (*gpubsub.Topic, error) {
	ctx := context.Background()
	client, err := gpubsub.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Wrap(err, "pubsub client failed")
	}

	topic := client.Topic(id)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "pubsub topic exists check failed")
	}

	if !exists {
		return client.CreateTopic(ctx, id)
	}

	return topic, nil
}

// getSubscription retrieves a PubSub subscription. It creates the subscription if it doesn't exist, using the
// provided topic object. The default Ack deadline, if not provided, is 20s.
func getSubscription(project, id string, topic *gpubsub.Topic, ackdeadline ...time.Duration) (*gpubsub.Subscription, error) {
	ctx := context.Background()
	client, err := gpubsub.NewClient(ctx, project)
	if err != nil {
		return nil, errors.Wrap(err, "pubsub client failed")
	}

	sub := client.Subscription(id)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "pubsub subscription exists check failed")
	}

	if !exists {
		deadline := time.Second * 20
		if len(ackdeadline) > 0 {
			deadline = ackdeadline[0]
		}

		return client.CreateSubscription(ctx, id, gpubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: deadline,
		})
	}

	return sub, nil
}

func main() {
	// Authentication check.
	creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if creds == "" {
		panic("setup your GOOGLE_APPLICATION_CREDENTIALS env var for auth")
	}

	// Project ID for GCP.
	projectId := os.Getenv("GCP_PROJECT_ID")
	if projectId == "" {
		panic("set GCP_PROJECT_ID env var to your GCP project id")
	}

	topicName := "lengthytesttopic"
	subscription := "lengthytestsubscription"

	// This will create the topic if not exists.
	t, err := getTopic(projectId, topicName)
	if err != nil {
		log.Fatal(err)
	}

	// This is only to create our subscription. We're not gonna use the return value.
	_, err = getSubscription(projectId, subscription, t)
	if err != nil {
		log.Fatal(err)
	}

	// Start the subscriber routine.
	go startLengthy(projectId, subscription)

	done := make(chan error)

	// Send a couple of messages to the topic to simulate work for our consumer.
	go func() {
		ctx := context.Background()
		log.Printf("sending 30s amount of work...")
		t.Publish(ctx, &gpubsub.Message{
			Data: []byte("30"),
		})

		log.Printf("sending 1min amount of work...")
		t.Publish(ctx, &gpubsub.Message{
			Data: []byte("60"),
		})

		// Wait for a little bit...
		time.Sleep(time.Second * 100)
		done <- nil
	}()

	<-done
}

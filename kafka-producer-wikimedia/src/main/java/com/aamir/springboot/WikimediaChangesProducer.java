package com.aamir.springboot;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    KafkaTemplate<String,String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void sendMessage() throws InterruptedException {
        String topic = "wikimedia_topic";

        // to read real time stream data from Wikimedia, use Event Source
        BackgroundEventHandler eventHandler =  new WikimediaChangesHandler(kafkaTemplate,topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder esBuilder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder eventSource = new BackgroundEventSource.Builder(eventHandler,esBuilder);
        BackgroundEventSource source = eventSource.build();
        source.start();

        /*
        This snippet of code sets up a system to listen to a stream of events from Wikimedia's "recentchange" feed and process these events in a background handler. Here's a breakdown of what's happening:

Creating an Event Handler:
BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topic);
This line initializes a new event handler (WikimediaChangesHandler) that will process incoming events. This handler is passed a Kafka template (kafkaTemplate) and a Kafka topic (topic), indicating that it will forward the processed events to a Kafka topic.


Specifying the Event Stream URL:
String url = "https://stream.wikimedia.org/v2/stream/recentchange";
This line defines the URL for the Wikimedia event stream that emits events when recent changes are made to Wikimedia projects, like Wikipedia.


Building an Event Source:
EventSource.Builder esBuilder = new EventSource.Builder(URI.create(url));
An EventSource.Builder is created with the Wikimedia event stream URL. This builder will be used to establish a connection to the event stream.


Creating a Background Event Source:
BackgroundEventSource.Builder eventSource = new BackgroundEventSource.Builder(eventHandler, esBuilder);
Here, a BackgroundEventSource.Builder is created, combining the event handler (eventHandler) with the event source builder (esBuilder). This builder is used to create an event source that processes events in the background.


Building and Starting the Event Source:
BackgroundEventSource source = eventSource.build();
This line uses the builder to create a BackgroundEventSource. The resulting BackgroundEventSource will manage the connection to the Wikimedia event stream and use the eventHandler to process events.
source.start();
This command starts the background event source, initiating the connection to the Wikimedia event stream and beginning the process of handling and processing events as they arrive.
         */

        TimeUnit.MINUTES.sleep(10);

    }
}

package com.proximetry.mqtt.examples

import com.hivemq.client.mqtt.MqttClient
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck
import org.slf4j.LoggerFactory
import java.util.*
import com.hivemq.client.mqtt.MqttGlobalPublishFilter
import com.proximetry.mqtt.Tools.buildTopicPath
import java.math.RoundingMode
import java.text.DecimalFormat
import java.util.concurrent.TimeUnit


object Simple {}

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger(Simple.javaClass.name)

    val brokerUrl = "mqtt.eclipse.org"

    val mainTopic = "starFleet"
    val devicesTopic = "$mainTopic/devices"
    val statistics = "$devicesTopic/statistics"

    val voyager = "StarFleet_Voyager"

    val warpStatId = "warp_speed"

    val df = DecimalFormat("#.##")
    df.apply { roundingMode = RoundingMode.CEILING }
    val warpValue = df.format(Random().nextFloat() * 18.56)

    var client: Mqtt3BlockingClient = MqttClient.builder()
        .identifier(UUID.randomUUID().toString())
        .serverPort(1883) //usually 1883 for unencrypted, 8883 for tls
        .serverHost(brokerUrl)
        .useMqttVersion3() //.useMqttVersion5() for 5.0 protocol version
        .buildBlocking();

    //client.connect()
    val conack = client.connectWith().cleanSession(false).keepAlive(60).send();
    log.info("Is session persistent: {}", conack.isSessionPresent);


    client.publishWith()
        .topic(buildTopicPath(statistics, voyager, warpStatId))
        .retain(true)
        .qos(MqttQos.AT_LEAST_ONCE)
        .payload(warpValue.toByteArray(Charsets.UTF_8))
        .send();

    val subscriptionTopicFilter = buildTopicPath(mainTopic, "#")
    val subscribe: Mqtt3SubAck = client.subscribeWith()
        .topicFilter(subscriptionTopicFilter)
        .qos(MqttQos.AT_LEAST_ONCE)
        .send()
    log.info("Subscription for {} : {}", subscriptionTopicFilter, subscribe.returnCodes)


    client.publishes(MqttGlobalPublishFilter.ALL).use { publishes ->
        log.info("Let's wait for messages...")
        val msg = publishes.receive(30, TimeUnit.SECONDS)
        msg.ifPresent {
            log.info("Got message on topic: '{}' with payload({}): '{}'",
                it.topic,
                it.payload.isPresent,
                String(it.payloadAsBytes, Charsets.UTF_8)
            )
        }
    }

    client.disconnect()
    log.info("The end");
}


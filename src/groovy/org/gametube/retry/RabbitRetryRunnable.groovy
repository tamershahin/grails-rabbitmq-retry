package org.gametube.retry

import com.budjb.rabbitmq.publisher.RabbitMessageProperties
import com.budjb.rabbitmq.publisher.RabbitMessagePublisher
import org.apache.log4j.Logger

/**
 * Created by tamer on 24/05/15.
 */
class RabbitRetryRunnable implements Runnable {

	Logger log = Logger.getLogger(RabbitRetryRunnable)

	RabbitMessagePublisher rabbitMessagePublisher
	RabbitMessageProperties properties
	Long sleepInterval

	public RabbitRetryRunnable(RabbitMessagePublisher rabbitMessagePublisher, RabbitMessageProperties properties, Long sleepInterval) {
		this.rabbitMessagePublisher = rabbitMessagePublisher
		this.properties = properties
		this.sleepInterval = sleepInterval
	}

	@Override
	void run() {

		try{
			// perform the sleep
			log.debug("sleeping for ${sleepInterval} millis")
			Thread.sleep(sleepInterval)
			log.debug("done sleeping for ${sleepInterval} millis")

			// now I can update the attempts in the message metadata
			properties.headers.retryMetadata.attempts++

			rabbitMessagePublisher.send(properties)

			log.debug("message successfully sent again after the sleep interval")
		}
		catch (themAll){
			log.error("error sending message after the sleep interval", themAll)
			properties.channel.basicNack(properties.headers.retryMetadata.deliveryTag, false, false)
		}


	}
}

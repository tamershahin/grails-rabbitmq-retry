package org.gametube.retry

import com.budjb.rabbitmq.consumer.MessageContext
import com.budjb.rabbitmq.publisher.RabbitMessageProperties
import com.budjb.rabbitmq.publisher.RabbitMessagePublisher

/**
 * Created by tamer on 25/02/15.
 */
trait RetryHandler {

	RabbitMessagePublisher rabbitMessagePublisher

	long initialWaitMillis = 3000
	long maxWaitMillis = 60000
	int maxAttempts = 3

	def onSuccess(MessageContext context) {
		log.debug('RetryHandler : message handled with no exception. now acking:' + context.envelope.deliveryTag)
		context.channel.basicAck(context.envelope.deliveryTag, false)
	}

	def onFailure(MessageContext context) {

		Map header = context.properties.headers ?: [:]

		if (header.retryMetadata == null) {
			header.retryMetadata = [:]
			header.retryMetadata.attempts = 0
			header.retryMetadata.routingKey = context.envelope.routingKey
			header.retryMetadata.exchange = context.envelope.exchange
		}
		header.retryMetadata.deliveryTag = context.envelope.deliveryTag

		if (header.retryMetadata.attempts < maxAttempts) {

			// ack the current message to discard it
			context.channel.basicAck(context.envelope.deliveryTag, false)

			log.debug("RetryHandler: exception occurred while processing. Now performing the re-queuing of header: ${header}")
			// get the sleepInterval
			Long currentWaitMillis = (initialWaitMillis * (Math.pow(2, header.retryMetadata.attempts)))
			Long sleepInterval = (currentWaitMillis > maxWaitMillis) ? maxWaitMillis : currentWaitMillis

			// perform the sleep
			log.debug("RetryHandler: sleeping for ${sleepInterval} millis")
			Thread.sleep(sleepInterval)
			log.debug("RetryHandler: done sleeping for ${sleepInterval} millis")

			// update the attempts in the message metadata
			header.retryMetadata.attempts++

			// resend again to itself with previous state
			RabbitMessageProperties properties = new RabbitMessageProperties()
			properties.body = context.body
			properties.exchange = header.retryMetadata.exchange
			properties.routingKey = header.retryMetadata.routingKey
			properties.headers = header

			rabbitMessagePublisher.send(properties)
		} else {
			// no more attemps, nack(ing) to make the queue send the message
			// to dead letter exchange
			log.debug("RetryHandler: -------- exception occurred while processing, but NO more attempts left, sending a NACK for header")
			context.channel.basicNack(context.envelope.deliveryTag, false, false)
		}
	}

}
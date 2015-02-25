package org.gametube.test

import com.budjb.rabbitmq.consumer.AutoAck
import com.budjb.rabbitmq.consumer.MessageContext
import org.apache.log4j.Logger
import org.gametube.integration.test.ConsumerIntegrationTestRecording

class DeadLettersOnlyConsumer extends ConsumerIntegrationTestRecording {
	/**
	 * Consumer configuration.
	 */
	static rabbitConfig = [
			'queue'      : 'test_sendOnlyDLQ',
			autoAck      : AutoAck.ALWAYS,
			consumers    : 10, //Set the number of concurrent consumers the message consumer should start.
			prefetchCount: 1, //Sets number of messages the consumer will prefetch. optimize the load between consumers
			retry        : false, // if true can be dangerous for looping exceptions. to use with DMG code maybe
			transacted   : false    // Sets whether automatic transactions should be enabled on the consumer.
			//  Useful only if the handler use the Channel instance passed in the MessageContext.
			//	Since the Channel is passed in the MessageContext, the author has full control over committing and rolling back transactions.
	]

	Logger log = Logger.getLogger(SendOnlyConsumer)

	/**
	 * Handle an incoming RabbitMQ message.
	 *
	 * @param body The converted body of the incoming message.
	 * @param context Properties of the incoming message.
	 * @return
	 */
	def handleMessage(def body, MessageContext context) {
		recordLastRequest(body.getClass().toString(), body, context)

		log.debug('message received on DLQ - HEADER: ' + context.properties.headers)
		log.debug('message received on DLQ - BODY: ' + body)
	}
}

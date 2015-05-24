package org.gametube.test

import com.budjb.rabbitmq.consumer.AutoAck
import com.budjb.rabbitmq.consumer.MessageContext
import org.apache.log4j.Logger
import org.gametube.integration.test.ConsumerIntegrationTestRecording
import org.gametube.retry.RetryHandler

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class SendOnlyConsumer extends ConsumerIntegrationTestRecording implements RetryHandler{

	/**
	 * Consumer configuration.
	 */
	static rabbitConfig = [
			'queue'      : 'test_sendOnly',
			autoAck      : AutoAck.MANUAL,
			consumers    : 10, //Set the number of concurrent consumers the message consumer should start.
			prefetchCount: 1, //Sets number of messages the consumer will prefetch. optimize the load between consumers
			retry        : false, // if true can be dangerous for looping exceptions. to use with DMG code maybe
			transacted   : false    // Sets whether automatic transactions should be enabled on the consumer.
			//  Useful only if the handler use the Channel instance passed in the MessageContext.
			//	Since the Channel is passed in the MessageContext, the author has full control over committing and rolling back transactions.
	]

	//to be moved in ConsumerAdapter.. cannot be inside the trait
	ExecutorService executor = Executors.newFixedThreadPool(10)

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

		String objectName = context.envelope.routingKey - '.exception'

		switch (objectName) {
			case 'throw':
				log.error('throwing dummy test exception to throw for' + context.envelope.routingKey)
				throw new Exception('test exception for ' + context.envelope.routingKey)
				break
			default:
				log.debug('message received on Regular Queue - HEADER: ' + context.properties.headers)
				log.debug('message received on Regular Queue - BODY: ' + body)
		}

	}


}

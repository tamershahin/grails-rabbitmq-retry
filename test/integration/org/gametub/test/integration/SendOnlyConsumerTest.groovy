package org.gametub.test.integration

import org.gametube.integration.test.MessageConsumerIntegrationTest
import org.gametube.test.DeadLettersOnlyConsumer
import org.gametube.test.SendOnlyConsumer

/**
 * Created by tamer on 25/02/15.
 */
class SendOnlyConsumerTest extends MessageConsumerIntegrationTest {

	SendOnlyConsumer sendOnlyConsumer
	DeadLettersOnlyConsumer deadLettersOnlyConsumer

	def setup() {
		sendOnlyConsumer.lastMessage = null
		deadLettersOnlyConsumer.lastMessage = null
	}

	def "That sending a message to sendOnly queue with doNotThrow"() {
		setup:
			rabbitMessagePublisher.send('test_topic4sends', 'doNotThrow.exception', [message: 'test it!'])

		when:
			//could be less, but just to have some margin..
			waitUntilMessageReceived(3000) { sendOnlyConsumer.lastMessage }

		then:
			sendOnlyConsumer.lastMessage != null
			noExceptionThrown()
	}

	def "That sending a message to sendOnly queue with throw"() {
		setup:
			rabbitMessagePublisher.send('test_topic4sends', 'throw.exception', [message: 'test it!'])

		when:
			waitUntilMessageReceived(3000) { sendOnlyConsumer.lastMessage }

			waitUntilMessageReceived(25000) { deadLettersOnlyConsumer.lastMessage }

		then:
			assert sendOnlyConsumer.lastMessage
			assert sendOnlyConsumer.lastMessage.body == [message: 'test it!']
			assert sendOnlyConsumer.lastMessage.messageContext.envelope.routingKey == 'throw.exception'
			assert sendOnlyConsumer.lastMessage.messageContext.envelope.exchange == 'test_topic4sends'

			assert deadLettersOnlyConsumer.lastMessage
			assert deadLettersOnlyConsumer.lastMessage.body == [message: 'test it!']
			assert deadLettersOnlyConsumer.lastMessage.messageContext.envelope.routingKey == 'test_sendOnlyDLK'
			assert deadLettersOnlyConsumer.lastMessage.messageContext.envelope.exchange == 'test_dead_letters'
			assert deadLettersOnlyConsumer.lastMessage.messageContext.properties.headers
			assert deadLettersOnlyConsumer.lastMessage.messageContext.properties.headers.retryMetadata
			assert deadLettersOnlyConsumer.lastMessage.messageContext.properties.headers.retryMetadata.attempts == 3
			assert deadLettersOnlyConsumer.lastMessage.messageContext.properties.headers.retryMetadata.exchange.toString() == 'test_topic4sends'
			assert deadLettersOnlyConsumer.lastMessage.messageContext.properties.headers.retryMetadata.routingKey.toString() == 'throw.exception'

			noExceptionThrown()
	}
}

package org.gametube.integration.test

import com.budjb.rabbitmq.consumer.MessageContext

/**
 * Created by tamer on 25/02/15.
 */
class ConsumerIntegrationTestRecording {
	public Map lastMessage


	protected void recordLastRequest(String type, Object body, MessageContext messageContext) {
		lastMessage = [
				type: type,
				body: body,
				messageContext: messageContext
		]
	}
}

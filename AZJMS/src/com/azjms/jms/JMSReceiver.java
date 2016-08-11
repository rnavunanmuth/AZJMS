package com.azjms.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

public class JMSReceiver {
	
	private static TopicConnectionFactory topicConnectionFactory = null;
	private static Topic topic = null;
	private static QueueConnectionFactory queueConnectionFactory = null;
	private static Queue queue = null;

	public void retrieveMessageFromQueue() {
		queue = JMSConfig.lookUpQueue();
		queueConnectionFactory = JMSConfig.getQueueConnectionFactory();
		QueueConnection queueConnection = null;
		QueueSession queueSession = null;
		QueueReceiver queueReceiver = null;
		String clientId = JMSConfig.getClientID();
		String subcriberId = JMSConfig.getSubscriberID();
		
		try {			
			queueConnection = queueConnectionFactory.createQueueConnection();
			if (clientId != null && !clientId.isEmpty()) {
				queueConnection.setClientID(clientId);
			}
			queueConnection.start();			
			queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			if (subcriberId != null && !subcriberId.isEmpty()) {
				queueReceiver = queueSession.createReceiver(queue, subcriberId);
			} else {
				queueReceiver = queueSession.createReceiver(queue);
			}
		} catch (JMSException jmse) {
			jmse.printStackTrace(System.err);
		}
		Message msg = null;
		do {
			try {
				msg = queueReceiver.receive(1000 * 10); // wait for 10 seconds
				if (msg != null) {
					ObjectMessage message = (ObjectMessage) msg;
					System.out.println(message);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} while (msg != null);
	}
	
	public void retrieveMessageFromTopic() {
		topic = JMSConfig.lookUpTopic();
		topicConnectionFactory = JMSConfig.getTopicConnectionFactory();
		TopicConnection topicConnection = null;
		TopicSession topicSession = null;
		TopicSubscriber topicSubscriber = null;
		String clientId = JMSConfig.getClientID();
		String subcriberId = JMSConfig.getSubscriberID();
		try {			
			topicConnection = topicConnectionFactory.createTopicConnection();
			if (clientId != null && !clientId.isEmpty()) {
				topicConnection.setClientID(clientId);
			}
			topicConnection.start();			
			topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
			if (subcriberId != null && !subcriberId.isEmpty()) {
				topicSubscriber = topicSession.createDurableSubscriber(topic, subcriberId);
			} else {
				topicSubscriber = topicSession.createSubscriber(topic);
			}
		} catch (JMSException jmse) {
			jmse.printStackTrace(System.err);
		}
		Message msg = null;
		do {
			try {
				msg = topicSubscriber.receive(1000 * 10); // wait for 10 seconds
				if (msg != null) {
					ObjectMessage message = (ObjectMessage) msg;
					System.out.println(message);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} while (msg != null);
	}

	public static void main(String args[]) {
	}
}

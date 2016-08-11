package com.azjms.jms;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

public class JMSSender {
	
	private static TopicConnectionFactory topicConnectionFactory = null;
	private static Topic topic = null;
	private static QueueConnectionFactory queueConnectionFactory = null;
	private static Queue queue = null;

	public void sendMessageToQueue(String messageText) {
		queue = (Queue) JMSConfig.lookUpQueue();
		queueConnectionFactory = JMSConfig.getQueueConnectionFactory();
		QueueConnection queueConnection = null;
		QueueSession queueSession = null;
		QueueSender queueSender = null;
		TextMessage message = null;
		String clientId = JMSConfig.getClientID();
		try {
			queueConnection = queueConnectionFactory.createQueueConnection();
			if (clientId != null && !clientId.isEmpty()) {
				queueConnection.setClientID(clientId);
			}
			queueConnection.start();
			queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			queueSender = queueSession.createSender(queue);
			message = queueSession.createTextMessage(messageText);
			queueSender.send(message);
		} catch (JMSException jmse) {
			jmse.printStackTrace(System.err);
		} finally {
			message = null;
			queueSender = null;
			if (queueSession != null) {
				try {
					queueSession.close();
				} catch (JMSException e) {
				}
			}
			if (queueConnection != null) {
				try {
					queueConnection.close();
				} catch (JMSException e) {
				}
			}
		}
	}
	
	public void sendMessageToTopic(String text) {
		topic = JMSConfig.lookUpTopic();
		topicConnectionFactory = JMSConfig.getTopicConnectionFactory();
		TopicConnection topicConnection = null;
		TopicSession topicSession = null;
		TopicPublisher topicPublisher = null;
		TextMessage message = null;
		String clientId = JMSConfig.getClientID();
		try {			
			topicConnection = topicConnectionFactory.createTopicConnection();
			if (clientId != null && !clientId.isEmpty()) {
				topicConnection.setClientID(clientId);
			}
			topicConnection.start();
			topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
			topicPublisher = topicSession.createPublisher(topic);			
			message = topicSession.createTextMessage(text);	
			topicPublisher.publish(message);
		} catch (JMSException jmse) {
			jmse.printStackTrace(System.err);
		} finally {
			message = null;
			if (topicSession != null) {
				try {
					topicSession.close();
				} catch (JMSException e) {
				}
			}
			if (topicConnection != null) {
				try {
					topicConnection.close();
				} catch (JMSException e) {
				}
			}
		}
	}

	public static void main(String args[]) {
	}
}

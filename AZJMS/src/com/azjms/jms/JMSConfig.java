package com.azjms.jms;

import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JMSConfig {

	private static Properties properties = null;

	private static String TEST = "test";
	private static String PROVIDER_URL = "provider_url";
	private static String SECURITY_PRINCIPAL = "security_principal";
	private static String SECURITY_CREDENTIALS = "security_credentials";
	private static String INITIAL_CONTEXT_FACTORY = "initial_context_factory";
	private static String CONNECTION_FACTORY = "connection_factory";
	private static String QUEUE_CONNECTION_FACTORY = "queue_connection_factory";
	private static String TOPIC_CONNECTION_FACTORY = "topic_connection_factory";
	private static String QUEUE_NAME = "queue_name";
	private static String TOPIC_NAME = "topic_name";
	private static String CLIENT_ID = "client_id";
	private static String SUBSCRIBER_ID = "subscriber_id";

	private static InitialContext initialContext = null;
	private static ConnectionFactory connectionFactory = null;
	private static QueueConnectionFactory queueConnectionFactory = null;
	private static TopicConnectionFactory topicConnectionFactory = null;
	private static Queue queue = null;
	private static Topic topic = null;

	static {
		properties = new Properties();
		try (InputStream inStream = JMSConfig.class.getResourceAsStream("jms.properties")) {
			properties.load(inStream);
			initialize();
		} catch (IOException e) {
			System.err.println("Unable to load properties: exiting...");
			e.printStackTrace();
			System.exit(1);
		}
	}

	static void initialize() {
		TEST = properties.getProperty(TEST);
		INITIAL_CONTEXT_FACTORY = properties.getProperty(INITIAL_CONTEXT_FACTORY);
		PROVIDER_URL = properties.getProperty(PROVIDER_URL);
		SECURITY_PRINCIPAL = properties.getProperty(SECURITY_PRINCIPAL);
		SECURITY_CREDENTIALS = properties.getProperty(SECURITY_CREDENTIALS);		
		CONNECTION_FACTORY = properties.getProperty(CONNECTION_FACTORY);
		QUEUE_CONNECTION_FACTORY = properties.getProperty(QUEUE_CONNECTION_FACTORY);
		TOPIC_CONNECTION_FACTORY = properties.getProperty(TOPIC_CONNECTION_FACTORY);
		QUEUE_NAME = properties.getProperty(QUEUE_NAME);
		TOPIC_NAME = properties.getProperty(TOPIC_NAME);
		CLIENT_ID = properties.getProperty(CLIENT_ID);
		SUBSCRIBER_ID = properties.getProperty(SUBSCRIBER_ID);
	}

	public static InitialContext getInitialContext() {
		if (initialContext == null) {
			Hashtable<String, String> jms = new Hashtable<>();
			jms.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
			jms.put(Context.PROVIDER_URL, PROVIDER_URL);
			jms.put(Context.SECURITY_PRINCIPAL, SECURITY_PRINCIPAL);
			jms.put(Context.SECURITY_CREDENTIALS, SECURITY_CREDENTIALS);
			try {
				initialContext = new InitialContext(jms);
			} catch (Exception e) {
				System.err.println("Unable to initialize jms context. exiting...");
				e.printStackTrace();
				System.exit(1);
			}
		}
		return initialContext;
	}

	public static ConnectionFactory getConnectionFactory() {
		if (connectionFactory == null) {
			try {
				connectionFactory = (ConnectionFactory) getInitialContext().lookup(JMSConfig.CONNECTION_FACTORY);
			} catch (NamingException e) {
				System.err.println("Unable to get connection factory. exiting...");
				e.printStackTrace();
				System.exit(1);
			}
		}
		return connectionFactory;
	}

	public static QueueConnectionFactory getQueueConnectionFactory() {
		if (queueConnectionFactory == null) {
			try {
				queueConnectionFactory = (QueueConnectionFactory) getInitialContext()
						.lookup(JMSConfig.QUEUE_CONNECTION_FACTORY);
			} catch (NamingException e) {
				System.err.println("Unable to get queue connection factory. exiting...");
				e.printStackTrace();
				System.exit(1);
			}
		}
		return queueConnectionFactory;
	}

	public static TopicConnectionFactory getTopicConnectionFactory() {
		if (topicConnectionFactory == null) {
			try {
				topicConnectionFactory = (TopicConnectionFactory) getInitialContext()
						.lookup(JMSConfig.TOPIC_CONNECTION_FACTORY);
			} catch (NamingException e) {
				System.err.println("Unable to get topic connection factory. exiting...");
				e.printStackTrace();
				System.exit(1);
			}
		}
		return topicConnectionFactory;
	}

	public static Queue lookUpQueue() {
		if (queue == null) {
			try {
				queue = (Queue) getInitialContext().lookup(JMSConfig.QUEUE_NAME);
			} catch (NamingException e) {
				System.err.println("Unable to get queue. exiting...");
				e.printStackTrace();
				System.exit(1);
			}
		}
		return queue;
	}

	public static Topic lookUpTopic() {
		if (topic == null) {
			try {
				topic = (Topic) getInitialContext().lookup(JMSConfig.TOPIC_NAME);
			} catch (NamingException e) {
				System.err.println("Unable to get topic. exiting...");
				e.printStackTrace();
				System.exit(1);
			}
		}
		return topic;
	}
	
	public static String getClientID() {
		return CLIENT_ID;
	}
	
	public static String getSubscriberID() {
		return SUBSCRIBER_ID;
	}

}
package com.youxiang.mq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author: Rivers
 * @date: 2018/4/4
 */
public class Producer {

    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("120.77.177.184");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection connection = null;
        Channel channel = null;
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();

            Map<String, Object> arguments = new HashMap<>();
            arguments.put("passive", true);
            channel.exchangeDeclare("hello-exchange", "direct", false, false, arguments);
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            channel.basicPublish("hello-exchange", "hello", null, "hello-world".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    // do nothing
                }
            }
        }
    }
}

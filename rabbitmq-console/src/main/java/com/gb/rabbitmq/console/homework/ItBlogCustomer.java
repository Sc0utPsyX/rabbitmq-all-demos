package com.gb.rabbitmq.console.homework;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ItBlogCustomer {
    private final static String EXCHANGER_NAME = "it_blog_exchanger";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        System.out.println("Write message (pattern: set_topic + theme, delete_topic + theme) or write exit for exit");
        Scanner scanner = new Scanner(System.in);
        String sb;
        Map<String,String> topicSubscribes = new HashMap<>();
        do {
            sb = scanner.nextLine();
            if (sb.toLowerCase(Locale.ROOT).regionMatches(0, "set_topic ", 0, 10)) {
                String topicName = sb.split(" ")[1].toLowerCase();
                switch(topicName){
                    case "php":{
                        topicName = getMessageFromTopic("php", channel);
                        topicSubscribes.put("php", topicName);
                        break;
                    }
                    case "java":{
                        getMessageFromTopic("java", channel);
                        break;
                    }
                }
            }
            if (sb.toLowerCase(Locale.ROOT).regionMatches(0, "delete_topic ", 0, 13)) {
                String topicName = sb.split(" ")[1].toLowerCase();
                switch(topicName){
                    case "php":{
                        channel.queueDelete(topicSubscribes.get("php"));
                        break;
                    }
                    case "java":{
                        channel.queueDelete(topicSubscribes.get("java"));
                        break;
                    }
                }
            }
        } while (!sb.equals("exit"));
        connection.close();
    }

    public static String getMessageFromTopic(String routingKey, Channel channel) throws IOException {
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGER_NAME, routingKey);
        System.out.println(" [*] Waiting for messages from topic " + routingKey);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("["+ routingKey + "] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
        return queueName;
    }
}

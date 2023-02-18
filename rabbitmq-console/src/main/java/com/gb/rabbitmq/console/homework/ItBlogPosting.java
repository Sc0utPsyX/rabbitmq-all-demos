package com.gb.rabbitmq.console.homework;

import com.rabbitmq.client.*;


import java.io.IOException;
import java.util.Scanner;

public class ItBlogPosting {
    private final static String EXCHANGER_NAME = "it_blog_exchanger";


    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.TOPIC);
            System.out.println("Write message (pattern: topic + message) or write exit for exit");
            Scanner scanner = new Scanner(System.in);
            String sb;
            do {
                sb = scanner.nextLine();
                String topic = sb.split(" ")[0].toLowerCase();
                switch(topic){
                    case "php": {
                        publishInTopic(sb, "php", channel);
                        break;
                    }
                    case "java": {
                        publishInTopic(sb, "java", channel);
                        break;
                    }
                }
            } while (!sb.equals("exit"));
        }
    }

    public static void publishInTopic(String message, String routingKey, Channel channel) throws IOException {
        message = message.substring(4);
        channel.basicPublish(EXCHANGER_NAME, routingKey, new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .deliveryMode(2)
                .build(), message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
    }
}


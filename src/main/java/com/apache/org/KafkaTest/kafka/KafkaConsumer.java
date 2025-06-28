package com.apache.org.KafkaTest.kafka;

import com.apache.org.KafkaTest.model.Employee;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "emp-topic", groupId = "emp-group", containerFactory = "employeeKafkaListenerFactory")
    public void consume(Employee employee) {
        System.out.println("Consumed Employee:" + employee);

    }
}

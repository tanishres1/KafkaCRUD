package com.apache.org.KafkaTest.kafka;

import com.apache.org.KafkaTest.model.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    private static final String TOPIC="emp-topic";
    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;

    public  void send(Employee employee){
        kafkaTemplate.send(TOPIC,employee);

    }

}

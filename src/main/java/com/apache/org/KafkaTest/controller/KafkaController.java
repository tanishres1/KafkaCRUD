package com.apache.org.KafkaTest.controller;

import com.apache.org.KafkaTest.kafka.KafkaProducer;
import com.apache.org.KafkaTest.model.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    @Autowired
    private KafkaProducer kafkaProducer;

   @PostMapping("/send")
    public  String sendEmployee(@RequestBody Employee employee){
       kafkaProducer.send(employee);
       return "Employee message sent to kafka";

   }

}

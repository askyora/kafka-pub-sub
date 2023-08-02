package com.yora.kafka.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    @Autowired
    private KafkaTemplate<String, String> template;

    @GetMapping(path = "/send/{mesage}")
    public void sendtoPrice(@PathVariable String mesage) {
	this.template.send("price", mesage);
    }

    @GetMapping(path = "/send/{topic}/{mesage}")
    public void sendtoPrice(@PathVariable(name = "topic") String topic, @PathVariable(name = "mesage") String mesage) {
	this.template.send(topic, mesage);
    }

}

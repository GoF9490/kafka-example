package gof.example.kafka.controller;

import gof.example.kafka.dtos.MessageDto;
import gof.example.kafka.service.ProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class ProducerController {
    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/message/create")
    public void messageCreate(@RequestBody MessageDto dto){
        producerService.messageCreate(dto);
    }

    @PostMapping("/message/create/key/{key}")
    public void messageCreateKey(@RequestBody MessageDto dto, @PathVariable String key){
        producerService.messageCreateKey(dto, key);
    }

}

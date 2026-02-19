package com.ing.kallitech.kafka.batch.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;

@RestController
public class BeanController {

    @Autowired
    private ApplicationContext applicationContext;

    @GetMapping("/beans")
    public ResponseEntity<String> listBeans() {
        String[] jobBeans = applicationContext.getBeanNamesForType(org.springframework.batch.core.Job.class);
        StringBuilder sb = new StringBuilder();
        sb.append("Job beans found: ").append(jobBeans.length).append("\n");
        for (String beanName : jobBeans) {
            sb.append("- ").append(beanName).append("\n");
        }
        
        String[] allBeans = applicationContext.getBeanDefinitionNames();
        sb.append("Total beans: ").append(allBeans.length).append("\n");
        
        return ResponseEntity.ok(sb.toString());
    }
}

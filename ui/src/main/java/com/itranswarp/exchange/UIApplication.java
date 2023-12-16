package com.itranswarp.exchange;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.itranswarp.exchange.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class UIApplication {
    public static void main(String[] args) {
        SpringApplication.run(UIApplication.class, args);
    }

    @Bean
    public RestClient createRestClient(
            @Value("#{exchangeConfiguration.apiEndpoints.tradingApi}") String tradingApipoint,
            @Autowired ObjectMapper objectMapper){
        return new RestClient.Builder(tradingApipoint).build(objectMapper);
    }

    @Bean
    public WebMvcConfigurer webMvcConfigurer(){
        return new WebMvcConfigurer() {
            @Override
            public void addResourceHandlers(ResourceHandlerRegistry registry) {
                //WebMvcConfigurer.super.addResourceHandlers(registry);
                registry.addResourceHandler("/static/**").addResourceLocations("classpath:/static/");
            }
        };
    }
}






























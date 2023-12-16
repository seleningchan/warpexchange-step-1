package com.itranswarp.exchange.push;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication
@EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class) // 禁用数据库自动配置 (无DataSource, JdbcTemplate...)
public class PushApplication {

    public static void main(String[] args) {
        System.setProperty("vertx.disableFileCPResolving","true");
        System.setProperty("vertx.logger-delegate-factory-class-name","io.vertx.core.logging.SLF4JLogDelegatefactory");
        //SpringApplication.run(PushApplication.class, args);
        SpringApplication app=new SpringApplication(PushApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.run(args);
    }
}

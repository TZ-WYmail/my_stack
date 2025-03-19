package cn.edu.sustech;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("cn.edu.sustech.mapper")
public class StackofWebApplication {

  public static void main(String[] args) {
    SpringApplication.run(StackofWebApplication.class, args);
  }
}

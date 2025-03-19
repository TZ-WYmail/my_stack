package cn.edu.sustech.controller;

import cn.edu.sustech.service.LastUpdateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping("api/last_update")
public class LastUpdateController {

  @Autowired private LastUpdateService service;

  @GetMapping("time")
  public Date lastUpdateTime() {
    return service.lastUpdateTime();
  }
}

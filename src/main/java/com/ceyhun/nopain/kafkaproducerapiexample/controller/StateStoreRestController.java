package com.ceyhun.nopain.kafkaproducerapiexample.controller;

import com.ceyhun.nopain.kafkaproducerapiexample.service.StateStoreQueryService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ceyhunuzunoglu
 */
@Api(value = "State Store Rest API")
@RestController
@RequestMapping("api")
public class StateStoreRestController {

  private final StateStoreQueryService stateStoreQueryService;

  public StateStoreRestController(StateStoreQueryService stateStoreQueryService) {
    this.stateStoreQueryService = stateStoreQueryService;
  }

  @ApiOperation("Get all listeners ids")
  @GetMapping("/get/all")
  public List<Long> getAllListeners() {
    return stateStoreQueryService.getAllListeners();
  }

  @ApiOperation("Get songs of a listener by id")
  @GetMapping("/get/songs")
  public List<String> getAllListeners(@RequestParam Long id) {
    return stateStoreQueryService.getListenerSongs(id);
  }

  @ApiOperation("Get songs of range of listeners by ids")
  @GetMapping("/get/songs/range")
  public List<String> getRangeOfListenersSongs(@RequestParam Long id1, @RequestParam Long id2) {
    return stateStoreQueryService.getRangeOfListenersSongs(id1, id2);
  }
}



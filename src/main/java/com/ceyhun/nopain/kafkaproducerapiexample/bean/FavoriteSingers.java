package com.ceyhun.nopain.kafkaproducerapiexample.bean;

import java.util.List;

/**
 * @author ceyhunuzunoglu
 */
public class FavoriteSingers {

  private String date;

  private List<String> singers;

  public FavoriteSingers() {
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public List<String> getSingers() {
    return singers;
  }

  public void setSingers(List<String> singers) {
    this.singers = singers;
  }

  @Override
  public String toString() {
    return "FavoriteSingers{" +
           "date='" + date + '\'' +
           ", singers=" + singers +
           '}';
  }
}

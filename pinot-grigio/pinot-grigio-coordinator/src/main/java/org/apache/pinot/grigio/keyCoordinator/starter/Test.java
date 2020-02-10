package org.apache.pinot.grigio.keyCoordinator.starter;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class Test {

  public static void main(String[] args) {
    Map<String, String> map = new HashMap<>();
    map.put("a", "b");
    try {
      System.out.println(map.entrySet().iterator().next().getValue());;
    } catch (NoSuchElementException e) {
      System.out.println("no such element");
    }
  }
}

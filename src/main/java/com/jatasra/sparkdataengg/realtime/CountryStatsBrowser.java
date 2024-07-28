package com.jatasra.sparkdataengg.realtime;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.Iterator;

public class CountryStatsBrowser {
  public static void main(String[] args) {

    try {
      Jedis jedis = new Jedis("localhost");
      String lbKey = "country-stats";

      while (true) {

        //Query the leaderboard and print the results
        var scores = jedis.zrevrangeWithScores(lbKey, 0, -1);

        Iterator<Tuple> iScores = scores.iterator();
        int position = 1;

        while (iScores.hasNext()) {
          Tuple score = iScores.next();
          System.out.println(
              "Country Stats - " + position + " : "
                  + score.getElement() + " = " + score.getScore());
          position++;
        }
        System.out.println("-------------------------------------------------------");

        Thread.currentThread().sleep(5000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}

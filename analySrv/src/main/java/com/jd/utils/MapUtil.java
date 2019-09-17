package com.jd.utils;

import java.util.*;

public class MapUtil {
    /**
     *
     */
    public static LinkedHashMap<String,Double> sortMapByValue(Map<String,Double> inputMap){
        if (inputMap == null || inputMap.isEmpty()){
            return null;
        }

        ArrayList<Map.Entry<String,Double>> entryList = new ArrayList<>(inputMap.entrySet());
        Collections.sort(entryList, new MapValueComparator());

        LinkedHashMap<String,Double> sortedMap = new LinkedHashMap<>();

        Iterator<Map.Entry<String, Double>> it = entryList.iterator();
        while (it.hasNext()) {
            Map.Entry<String, Double> one = it.next();
            sortedMap.put(one.getKey(), one.getValue());
        }
        return sortedMap;

    }

    public static class MapValueComparator implements Comparator<Map.Entry<String,Double>>{
        @Override
        public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
            return o2.getValue().compareTo(o1.getValue());
        }
    }
}

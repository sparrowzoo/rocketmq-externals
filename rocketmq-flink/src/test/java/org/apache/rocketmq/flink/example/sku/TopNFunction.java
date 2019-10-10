package org.apache.rocketmq.flink.example.sku;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
     * 
     *针对keyby window的TopN函数，继承自ProcessWindowFunction
     *
     */
    public  class TopNFunction
            extends
        ProcessAllWindowFunction<UserSkuCount,UserSkuCount, TimeWindow> {

        private int topSize = 10;

        public TopNFunction(int topSize) {
            // TODO Auto-generated constructor stub
            this.topSize = topSize;
        }


    @Override
    public void process(Context context, Iterable<UserSkuCount> elements, Collector<UserSkuCount> out) throws Exception {

        System.err.println("++++++++++++++++++++++++++++++++++++++++++process+++++++++++");
        TreeMap<Integer, UserSkuCount> treemap = new TreeMap<Integer,UserSkuCount>(
                new Comparator<Integer>() {

                    @Override
                    public int compare(Integer y, Integer x) {
                        // TODO Auto-generated method stub
                        return (x < y) ? -1 : 1;
                    }

                });

        for (UserSkuCount element : elements) {
            treemap.put(element.getCount(), element);
            if (treemap.size() > topSize) {
                treemap.pollLastEntry();
            }
        }

        for (Map.Entry<Integer,UserSkuCount> entry : treemap
                .entrySet()) {
            out.collect(entry.getValue());
        }
        out.collect(new UserSkuCount("----------------",-1));
    }
}
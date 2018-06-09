package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTips {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiFare> fares = env.addSource(
                new CheckpointedTaxiFareSource("/home/bigdata/Documents/flink/nycTaxiFares.gz", 600));

        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .timeWindow(Time.hours(1))
                .apply(new AddTips());

        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .timeWindowAll(Time.hours(1))
                .maxBy(2);

        hourlyMax.print();

        // execute program
        env.execute("Ejercicio propinas por hora");
    }

    public static class AddTips implements WindowFunction<
                TaxiFare, // input type
                Tuple3<Long, Long, Float>, // output type
                Long, // key type
                TimeWindow> // window type
    {

        @Override
        public void apply(
                Long key,
                TimeWindow window,
                Iterable<TaxiFare> fares,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {

            float sumOfTips = 0;
            for(TaxiFare fare : fares) {
                sumOfTips += fare.tip;
            }

            out.collect(new Tuple3<>(window.getEnd(), key, sumOfTips));
        }
    }
}

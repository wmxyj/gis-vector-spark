package com.katus.model;


import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.BufferArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

@Slf4j
public class Buffer {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();
        log.info("Setup arguments");
        BufferArgs mArgs = BufferArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Buffer Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }
        Layer layer = InputUtil.makeLayer(ss, mArgs.getInput());
        Layer layer1 = buffer(layer, Double.parseDouble(mArgs.getDistance()));
        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer1);
        ss.close();
    }

    public static Layer buffer(Layer layer, Double distance) {
        JavaPairRDD<String, Feature> result = layer.mapToPair(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> stringFeatureTuple2) throws Exception {
                stringFeatureTuple2._2.setGeometry(stringFeatureTuple2._2.getGeometry().buffer(distance));
                return stringFeatureTuple2;
            }
        });
        return Layer.create(result, layer.getMetadata().getFieldNames(), layer.getMetadata().getCrs(), layer.getMetadata().getGeometryType(), result.count());
    }

}

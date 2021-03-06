package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.RandomSelectionArgs;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-18
 */
@Slf4j
public class RandomSelection {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        RandomSelectionArgs mArgs = RandomSelectionArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Random Selection Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer targetLayer = InputUtil.makeLayer(ss, mArgs.getInput(), Boolean.valueOf(mArgs.getHasHeader()),
                Boolean.valueOf(mArgs.getIsWkt()), mArgs.getGeometryFields().split(","), mArgs.getSeparator(),
                mArgs.getCrs(), mArgs.getCharset(), mArgs.getGeometryType());

        log.info("Prepare calculation");
        String rs = mArgs.getRatio().trim();
        double ratio = rs.endsWith("%") ? Double.parseDouble(rs.substring(0, rs.length()-1)) / 100.0 : Double.parseDouble(rs);
        if (ratio > 1 || ratio < 0) {
            String msg = "Ratio must between 0 and 1, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Start Calculation");
        Layer layer = randomSelection(targetLayer, ratio);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer);

        ss.close();
    }

    public static Layer randomSelection(Layer layer, double fraction) {
        LayerMetadata metadata = layer.getMetadata();
        JavaPairRDD<String, Feature> result = layer.sample(false, fraction).cache();
        return Layer.create(result, metadata.getFieldNames(), metadata.getCrs(), metadata.getGeometryType(), result.count());
    }
}

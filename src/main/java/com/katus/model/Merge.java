package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.MergeArgs;
import com.katus.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-11-18
 */
@Slf4j
public class Merge {
    public static void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();

        log.info("Setup arguments");
        MergeArgs mArgs = MergeArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Merge Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Make layers");
        Layer layer1 = InputUtil.makeLayer(ss, mArgs.getInput1(), Boolean.valueOf(mArgs.getHasHeader1()),
                Boolean.valueOf(mArgs.getIsWkt1()), mArgs.getGeometryFields1().split(","), mArgs.getSeparator1(),
                mArgs.getCrs1(), mArgs.getCharset1(), mArgs.getGeometryType1());
        Layer layer2 = InputUtil.makeLayer(ss, mArgs.getInput2(), Boolean.valueOf(mArgs.getHasHeader2()),
                Boolean.valueOf(mArgs.getIsWkt2()), mArgs.getGeometryFields2().split(","), mArgs.getSeparator2(),
                mArgs.getCrs2(), mArgs.getCharset2(), mArgs.getGeometryType2());

        log.info("Dimension check");
        if (GeometryUtil.getDimensionOfGeomType(layer1.getMetadata().getGeometryType()) !=
                GeometryUtil.getDimensionOfGeomType(layer2.getMetadata().getGeometryType())) {
            String msg = "Two layers must have the same dimension, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        log.info("Prepare calculation");
        if (!mArgs.getCrs().equals(mArgs.getCrs1())) {
            layer1 = layer1.project(CrsUtil.getByCode(mArgs.getCrs()));
        }
        if (!mArgs.getCrs().equals(mArgs.getCrs2())) {
            layer2 = layer2.project(CrsUtil.getByCode(mArgs.getCrs()));
        }

        log.info("Start Calculation");
        Layer layer = merge(layer1, layer2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer);

        ss.close();
    }

    public static Layer merge(Layer layer1, Layer layer2) {
        LayerMetadata metadata1 = layer1.getMetadata();
        LayerMetadata metadata2 = layer2.getMetadata();
        String[] fieldNames = FieldUtil.mergeFields(metadata1.getFieldNames(), metadata2.getFieldNames());
        JavaPairRDD<String, Feature> result1 = layer1.mapToPair(pairItem -> {
            Feature feature = pairItem._2();
            LinkedHashMap<String, Object> attributes = FieldUtil.mergeAttributes(fieldNames, feature.getAttributes(), new HashMap<>());
            feature.setAttributes(attributes);
            return new Tuple2<>(pairItem._1(), feature);
        });
        JavaPairRDD<String, Feature> result2 = layer2.mapToPair(pairItem -> {
            Feature feature = pairItem._2();
            LinkedHashMap<String, Object> attributes = FieldUtil.mergeAttributes(fieldNames, new HashMap<>(), feature.getAttributes());
            feature.setAttributes(attributes);
            return new Tuple2<>(pairItem._1(), feature);
        });
        JavaPairRDD<String, Feature> result = result1.union(result2).cache();
        return Layer.create(result, fieldNames, metadata1.getCrs(), metadata1.getGeometryType(), result.count());
    }
}

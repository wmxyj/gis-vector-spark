package com.katus.model;

import com.katus.entity.Feature;
import com.katus.entity.Layer;
import com.katus.entity.LayerMetadata;
import com.katus.io.writer.LayerTextFileWriter;
import com.katus.model.args.UnionArgs;
import com.katus.util.FieldUtil;
import com.katus.util.InputUtil;
import com.katus.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class Union2 {
    public static  void main(String[] args) throws Exception {
        log.info("Setup Spark Session");
        SparkSession ss = SparkUtil.getSparkSession();
        log.info("Setup arguments");
        UnionArgs mArgs = UnionArgs.initArgs(args);
        if (mArgs == null) {
            String msg = "Init Clip Args failed, exit!";
            log.error(msg);
            throw new RuntimeException(msg);
        }
        Layer layer1 = InputUtil.makeLayer(ss, mArgs.getInput1());
        Layer layer2 = InputUtil.makeLayer(ss, mArgs.getInput2());
        Layer layer = union(layer1, layer2);

        log.info("Output result");
        LayerTextFileWriter writer = new LayerTextFileWriter("", mArgs.getOutput());
        writer.writeToFileByPartCollect(layer);
        ss.close();
    }

    public  static Layer union(Layer layer1, Layer layer2) {
        LayerMetadata metadata1 = layer1.getMetadata();
        layer1 = layer1.index(1);
        layer2 = layer2.index(1);
        JavaPairRDD<String, Tuple2<Feature, Feature>> joinRdd = layer1.join(layer2);
        String[]  myFields= FieldUtil.mergeFields(layer1.getMetadata().getFieldNames(),layer2.getMetadata().getFieldNames());
        JavaRDD<Tuple2<Feature, Feature>> joinRddMap = joinRdd.map(new Function<Tuple2<String, Tuple2<Feature, Feature>>, Tuple2<Feature, Feature>>() {
            @Override
            public Tuple2<Feature, Feature> call(Tuple2<String, Tuple2<Feature, Feature>> stringTuple2Tuple2) {
                Geometry geometry1 = stringTuple2Tuple2._2._1.getGeometry();
                Geometry geometry2 = stringTuple2Tuple2._2._2.getGeometry();
                if (!geometry1.intersects(geometry2)) {
                    stringTuple2Tuple2._2._1.setAttributes(FieldUtil.mergeAttributes(myFields,stringTuple2Tuple2._2._1.getAttributes(),stringTuple2Tuple2._2._2.getAttributes()));
                    stringTuple2Tuple2._2._2.setAttributes(FieldUtil.mergeAttributes(myFields,stringTuple2Tuple2._2._2.getAttributes(),stringTuple2Tuple2._2._1.getAttributes()));
                    return new Tuple2<>(stringTuple2Tuple2._2._1, stringTuple2Tuple2._2._2);
                } else {
                    Feature feature = new Feature();
                    feature.setGeometry(geometry1.difference(geometry2));
                    feature.setFid(stringTuple2Tuple2._2._1.getFid()+"#"+stringTuple2Tuple2._2._2.getFid());
                    feature.setAttributes(FieldUtil.mergeAttributes(myFields,stringTuple2Tuple2._2._1.getAttributes(),stringTuple2Tuple2._2._2.getAttributes()));
//                    feature.setGeometry(stringTuple2Tuple2._2._1.getGeometry());
                    return new Tuple2<>(feature, stringTuple2Tuple2._2._2);
                }
//                return null;
            }
        });
        List<Tuple2<Feature, Feature>> collect3 = joinRddMap.collect();
        JavaPairRDD<String, Feature> objectObjectJavaPairRDD = joinRddMap.flatMapToPair(new PairFlatMapFunction<Tuple2<Feature, Feature>, String, Feature>() {
            @Override
            public Iterator<Tuple2<String, Feature>> call(Tuple2<Feature, Feature> featureFeatureTuple2) throws Exception {
                List<Tuple2<String, Feature>> list = new ArrayList<Tuple2<String, Feature>>();
                list.add(new Tuple2<>(featureFeatureTuple2._1.getFid(), featureFeatureTuple2._1));
                list.add(new Tuple2<>(featureFeatureTuple2._2.getFid(), featureFeatureTuple2._2));
                return list.iterator();
            }
        });
        List<Tuple2<String, Feature>> collect5 = objectObjectJavaPairRDD.collect();
        JavaPairRDD<String, Feature> result = objectObjectJavaPairRDD.reduceByKey(new Function2<Feature, Feature, Feature>() {
            @Override
            public Feature call(Feature feature1, Feature feature2) throws Exception {
                if (feature1 == null) return null;
                if (feature2 == null) return null;
                if (feature1.getGeometry().intersects(feature2.getGeometry())) {
                    Feature feature = new Feature();
                    feature.setFid(feature1.getFid());
                    feature.setAttributes(feature1.getAttributes());
                    feature.setGeometry(feature1.getGeometry().intersection(feature2.getGeometry()));
                    return feature;
                } else {
                    return null;
                }
            }
        }).filter(new Function<Tuple2<String, Feature>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Feature> stringFeatureTuple2) throws Exception {
                return stringFeatureTuple2._2 != null;
            }
        });
        List<Tuple2<String, Feature>> collect = result.collect();

        return Layer.create(result, myFields, metadata1.getCrs(), metadata1.getGeometryType(), result.count());

    }

    private static String[] stringUntil(String[] arr1, String[] arr2) {
        //返回两个数组相差的字符
        String[] arrLong = null;
        String[] arrShort = null;
        if (arr1.length <= arr2.length) {
            arrLong = arr2;
            arrShort = arr1;
        } else {
            arrLong = arr1;
            arrShort = arr2;
        }
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < arrLong.length; i++) {
            if (!Arrays.asList(arrShort).contains(arrLong[i])) {
                fields.add(arrLong[i]);
            }
        }
        String[] strings = new String[fields.size()];
        return fields.toArray(strings);
    }
}


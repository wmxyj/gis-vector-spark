package com.katus.model.args;

import lombok.Getter;
import lombok.Setter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

@Setter
@Getter
public class UnionArgs implements Serializable {
    private final static Logger logger = LoggerFactory.getLogger(ClipArgs.class);

    @Option(name = "-input1", usage = "shp或csv文件输入路径", required = true)
    private String input1;
//    @Option(name = "-type1", usage = "输入文件类型，WKT或者SHP", required = true)
//    private String type1;
//    @Option(name = "-idIndex1", usage = "如果为csv文件，则需要指定id的列数，从0开始", required = true)
//    private String idIndex1;
    @Option(name = "-input2", usage = "shp或csv文件输入路径", required = true)
    private String input2;
//    @Option(name = "-type2", usage = "输入文件类型，WKT或者SHP", required = true)
//    private String type2;
//    @Option(name = "-idIndex2", usage = "如果为csv文件，则需要指定id的列数，从0开始", required = true)
//    private String idIndex2;
//    @Option(name = "-type1", usage = "输入文件类型，WKT或者SHP", required = true)
//    private String type1;
//    @Option(name = "-type2", usage = "输入文件类型，WKT或者SHP", required = true)
//    private String type2;

    @Option(name = "-output", usage = "buffer结果输出路径，输出格式为csv", required = true)
    private String output;



    public static UnionArgs initArgs(String[] args) {
        UnionArgs mArgs = new UnionArgs();
        CmdLineParser parser = new CmdLineParser(mArgs);
        try {
            parser.parseArgument(args);
            return mArgs;
        } catch (CmdLineException e) {
            logger.error(e.getLocalizedMessage());
            return null;
        }
    }
}




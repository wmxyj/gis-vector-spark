package com.katus.model.args;
import lombok.Getter;
import lombok.Setter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

@Getter
@Setter
public class BufferArgs implements Serializable {
    private final static Logger logger = LoggerFactory.getLogger(ClipArgs.class);

    @Option(name = "-input", usage = "shp或csv文件输入路径", required = true)
    private String input;
    @Option(name = "-output", usage = "buffer结果输出路径，输出格式为csv", required = true)
    private String output;
    @Option(name = "-distance", usage = "缓冲半径", required = true)
    private String distance;
//    @Option(name = "-type", usage = "输入文件类型，WKT或者SHP", required = true)
//    private String type;
//    @Option(name = "-idIndex", usage = "如果为csv文件，则需要指定id的列数，从0开始", required = true)
//    private String idIndex;

    public static BufferArgs initArgs(String[] args) {
        BufferArgs mArgs = new BufferArgs();
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


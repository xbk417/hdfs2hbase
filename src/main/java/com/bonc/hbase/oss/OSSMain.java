package com.bonc.hbase.oss;

import com.bonc.hbase.hbase2hdfs.HBase2HdfsOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * Created by xiabaike on 2016/11/25.
 */
public class OSSMain {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OSSMain.class);

    final static String RAW_SCAN = "hbase.mapreduce.include.deleted.rows";
    final static String EXPORT_BATCHING = "hbase.export.scanner.batch";
    private static final String CONF_OUTPUT_ROOT = "hbase2hdfs.output.root";

    public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
        conf.addResource(new Path(args[2]));

        String hbaseTableName = "";
        String confTableName = "";
        String spec_id = "null";
        String outputPath = "";
        String errOutputPath = "";
        String confPath = "";
        String jobName = "";
        Path outputDir = null;
        if(args.length > 0){
            for(int i = 0; i<= args.length-1;i++){
                switch(args[i]){
                    case "-hbaseTableName":
                        hbaseTableName = args[++i];
                        break;
                    case "-confTableName":
                        confTableName = args[++i];
                        break;
                    case "-spec_id":
                        spec_id = args[++i];
                        break;
                    case "-outputPath":
                        outputPath = args[++i];
                        break;
                    case "-errOutputPath":
                        errOutputPath = args[++i];
                        break;
                    case "-confPath" :
                        confPath = args[++i];
                        break;
                    case "-jobName":
                        jobName = args[++i];
                        break;
                }
            }
            if(hbaseTableName.equals("") || confTableName.equals("") || outputPath.equals("") || confPath.equals("") || jobName.equals("")){
                LOG.error("请输入参数");
                System.exit(-1);
            }
            Path path = new Path(confPath);
            conf.addResource(path);

            outputDir = new Path(outputPath);
            FileSystem dst = FileSystem.get(conf);
            dst.delete(outputDir, true);
        }else{
            LOG.error("请输入参数");
            System.exit(-1);
        }

        conf.set("conf.confTableName", confTableName);
        conf.set("conf.hbaseTableName", hbaseTableName);
        conf.set("conf.specid", spec_id);
        conf.set("conf.outputPath", outputPath);
        conf.set("conf.errOutputPath", errOutputPath);
        conf.set(CONF_OUTPUT_ROOT, outputPath);

        Job job = Job.getInstance(conf,  jobName);
        job.setJarByClass(OSSMain.class);
        // Set optional scan parameters
        Scan scan = new Scan();
        scan.setMaxVersions(1);
        scan.setCacheBlocks(false);
        BoncTableMapper.initJob(hbaseTableName, scan, BoncTableMapper.class, job);
        // No reducers.  Just write straight to output files.
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(HBase2HdfsOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, (Class<? extends CompressionCodec>) conf.getClass("get.table.out.compress", GzipCodec.class));
        FileOutputFormat.setOutputPath(job, outputDir); // job conf doesn't contain the conf so doesn't have a default fs.

        return job;
    }

    private static void usage(final String errorMsg) {
        if (errorMsg != null && errorMsg.length() > 0) {
            System.err.println("ERROR: " + errorMsg);
        }
        System.err.println("Usage: Export [-D <property=value>]* -hbaseTableName <hbaseTableName> " +
                "-confTableName <confTableName> -spec_id spec_id -outputPath outputPath " +
                "-errOutputPath errOutputPath -confPath confPath -jobName jobName\n");
        System.err.println("  Note: -D properties will be applied to the conf used. ");
        System.err.println("  For example: ");
        System.err.println("   -D mapreduce.output.fileoutputformat.compress=true");
        System.err.println("   -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec");
        System.err.println("   -D mapreduce.output.fileoutputformat.compress.type=BLOCK");
        System.err.println("  Additionally, the following SCAN properties can be specified");
        System.err.println("  to control/limit what is exported..");
        System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
        System.err.println("   -D " + RAW_SCAN + "=true");
        System.err.println("   -D " + TableInputFormat.SCAN_ROW_START + "=<ROWSTART>");
        System.err.println("   -D " + TableInputFormat.SCAN_ROW_STOP + "=<ROWSTOP>");
        System.err.println("For performance consider the following properties:\n"
                + "   -Dhbase.client.scanner.caching=100\n"
                + "   -Dmapreduce.map.speculative=false\n"
                + "   -Dmapreduce.reduce.speculative=false");
        System.err.println("For tables with very wide rows consider setting the batch size as below:\n"
                + "   -D" + EXPORT_BATCHING + "=10");
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 6) {
            usage("Wrong number of arguments: " + otherArgs.length);
            System.exit(-1);
        }
        Job job = createSubmittableJob(conf, otherArgs);
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}

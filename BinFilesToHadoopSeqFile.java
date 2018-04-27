import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.*;

public class BinFilesToHadoopSeqFile {
    public static class BinFilesToHadoopSeqFileMapper extends Mapper < Object, Text, Text, BytesWritable > {
        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {

            String uri = value.toString();

            Configuration conf = new Configuration();

            FileSystem fsys = FileSystem.get(URI.create(uri), conf);

            FSDataInputStream fsin = null;

            try {

                fsin = fsys.open(new Path(uri));

                java.io.ByteArrayOutputStream bout = new ByteArrayOutputStream();

                byte buf[] = new byte[1024 * 1024];

                while (fsin.read(buf, 0, buf.length) >= 0) {

                    bout.write(buf);

                }

                context.write(value, new BytesWritable(bout.toByteArray()));

            } finally {

                IOUtils.closeStream(fsin);

            }

        }


        public static void main(String[] args) throws Exception {

            Configuration conf = new Configuration();

            Job job = new Job(conf, "BinFilesToHadoopSeqFile");

            job.setJarByClass(BinFilesToHadoopSeqFile.class);

            job.setMapperClass(BinFilesToHadoopSeqFileMapper.class);

            job.setOutputKeyClass(Text.class);

            job.setOutputValueClass(BytesWritable.class);

            job.setInputFormatClass(TextInputFormat.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));

            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }
    }
}
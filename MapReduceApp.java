import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.warc.WARCFileInputFormat;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Job;
import org.commoncrawl.warc.WARCFileInputFormat;

//FIX: When you do map.put(domain) you are using the entire list of domains as the key, not just the single domain you're looking at at that moment.

public class MapReduceApp {
    private static final Logger LOG = Logger.getLogger(MapReduceApp.class);
    protected static enum MAPPERCOUNTER {
        RECORDS_IN,
        EXCEPTIONS
    }
    public static class TagCounterMapper extends Mapper<Text, ArchiveReader, Text, Text> {
        Text outKey = new Text();
        Text outVal = new Text();
        private static final String EMAIL_REGEX = "[0-9a-z._%+-]+@[a-z0-9.-]+\\.[a-z]{2,64}"; 
            //"([_a-z0-9-\\+]+(\\.[_a-z0-9-]+)*@[a-z0-9-]+(\\.[a-z0-9]+)*(\\.[a-z]{2,64}))";
        private static final String DOMAIN_REGEX = "([\\w_-]+(?:(?:\\.[\\w_-]+)+))"; 
        Pattern patternTag; //Email pattern tag
        Pattern domainp; //Domain pattern tag
        Matcher emailm; //Email matcher tag
        Matcher matcherTag;
        public void map(Text key, ArchiveReader value, Context context) throws IOException {
            patternTag = Pattern.compile(EMAIL_REGEX, Pattern.CASE_INSENSITIVE);
            domainp = Pattern.compile(DOMAIN_REGEX, Pattern.CASE_INSENSITIVE);
            
            for (ArchiveRecord r : value) {
                try {
                    if (r.getHeader().getMimetype().equals("application/http; msgtype=response")){
                        
                        String url = r.getHeader().getUrl();
                        byte[] rawData = IOUtils.toByteArray(r, r.available());
                        
                        String content = new String(rawData);
                        
                        String headerText = content.substring(0, content.indexOf("\r\n\r\n"));
                        if (headerText.contains("Content-Type: text/html"))
                            {
                                context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
                                String body = content.substring(content.indexOf("\r\n\r\n") + 4);
                                emailm = patternTag.matcher(body);
                                matcherTag = domainp.matcher(url);
                                
                                String domainCatch = "";
                                if (matcherTag.find())
                                    {
                                        domainCatch = matcherTag.group();
                                    }
                                while(emailm.find())
                                    {
                                        String allEmails = emailm.group();
                                        outKey.set(allEmails.toLowerCase());
                                        outVal.set(domainCatch.toLowerCase());
                                        context.write(outKey, outVal);
                                    }
                            }
                    }
                }
                catch (Exception ex) {
                    LOG.error("Caught Exception", ex);
                    context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
                    
                }
            }
        }
    }
    
    public static class emailDomainReducer extends Reducer<Text, Text, Text, IntWritable>
    {
        IntWritable output = new IntWritable();
        
        public void reduce(Text email, Iterable<Text> domain,
                           Context context
                           ) throws IOException, InterruptedException {
            Map<Iterable<Text>, Integer> map = new HashMap<Iterable<Text>, Integer>();
            int sum = 0;
            
            for (Text values : domain)
                {
                    if (map.containsValue(values) == false)
                        {
                            sum+=1;
                            map.put(domain, 0);
                        }
                }
            output.set(sum);
            context.write(email, output);
        }
    }

    public static void main (String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapreduce.task.timeout", "10000000");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduceApp.class);
        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setMapperClass(TagCounterMapper.class);
        job.setReducerClass(emailDomainReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


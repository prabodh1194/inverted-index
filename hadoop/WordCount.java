import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
    {
        private IntWritable idi;
        private Text word = new Text();
        private int flag = 0;
        private String id = "";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            FileInputStream fis = new FileInputStream(new File("stopwords.dat"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String token, line = null;
            ArrayList<String> sw = new ArrayList<String>();
            Stemmer st = new Stemmer();

            while((line = br.readLine()) != null)
                sw.add(line);

            br.close();

            Set<Integer> set = new HashSet<Integer>();
            Map<String, Set<Integer> > dict = new HashMap<String, Set<Integer> >();
            String a = value.toString();
            StringTokenizer strtok = new StringTokenizer(a," \n");
            Pattern reid = Pattern.compile("id=\"([0-9]+)\"");
            Pattern retitle = Pattern.compile("title=\"(.*)\" non");
            Matcher m;

            while(strtok.hasMoreTokens())
            {
                token = strtok.nextToken();
                if(token.equals("</doc>"))
                {
                    id = "";
                    flag = 0;
                    continue;
                }

                if(id.equals(""))
                {
                    m = reid.matcher(token);
                    if(!m.find())                    
                        continue;
                    id = m.group(1);
                    continue;
                }

                if(token.charAt(token.length()-1) == '>' && flag == 0)
                {
                    flag = 1;
                    continue;
                }

                if(flag == 0)
                    continue;

                token = token.replaceAll("[^a-zA-Z0-9 ]"," ");
                token = token.toLowerCase();

                StringTokenizer strtok1 = new StringTokenizer(token," ");
                StringBuilder sb = new StringBuilder();

                while(strtok1.hasMoreTokens())
                {
                    token = strtok1.nextToken();
                    token += " ";
                    if(Collections.binarySearch(sw, token) >= 0)
                        continue;
                    token = st.stem(token);
                    word.set(token);
                    context.write(word, new Text(id));
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
        {
            Set<String> s = new HashSet<String>();
            for (Text val : values)
            {
                s.add(val.toString());
            }

            StringBuilder sb = new StringBuilder();
            for (String val : s)
                sb.append(val+",");

            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

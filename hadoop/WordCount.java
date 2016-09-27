import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
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
        private long pos = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            FileInputStream fis = new FileInputStream(new File("stopwords.dat"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String token, line = null;
            ArrayList<String> sw = new ArrayList<String>();
            Stemmer st = new Stemmer();
            long len;

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
                len = token.length();

                if(token.equals("</doc>"))
                {
                    id = "";
                    flag = 0;
                    pos = 0;
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

                sb.append(id);
                sb.append(",");
                sb.append(Long.toString(pos));

                while(strtok1.hasMoreTokens())
                {
                    token = strtok1.nextToken();
                    token += " ";
                    if(Collections.binarySearch(sw, token) >= 0)
                        continue;
                    token = st.stem(token);
                    word.set(token);
                    context.write(word, new Text(sb.toString()));
                }
                pos += len+1;
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
        {
            Map<String, StringBuilder> m = new HashMap<String, StringBuilder>();
            for (Text val : values)
            {
                StringBuilder s = new StringBuilder();
                String kv[] = new String[2];

                kv = (val.toString()).split(",");

                if(kv.length < 2)
                    continue;

                if(m.containsKey(kv[0]))
                {
                    s = m.get(kv[0]);
                }
                s.append(kv[1]);
                s.append(":");
                m.put(kv[0], s);
            }

            StringBuilder sb = new StringBuilder();
            for (String ke: m.keySet())
            {
                sb.append(ke);
                sb.append(":");
                sb.append((m.get(ke)).toString());
                sb.deleteCharAt(sb.length()-1);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length()-1);
            String text = sb.toString();
            context.write(key, new Text(text));
        }
    }

    //Partitioner class
    public static class InvertPartitioner extends Partitioner <Text,Text>
    {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks)
        {
            if(numReduceTasks == 0)
            {
                return 0;
            }

            int ch = key.charAt(0);

            if(Character.isDigit(ch))
                return (int)(ch-'0');
            else
                return (int)(ch-'a');
        }
    }

    //Query Mapper
    public static class QueryMapper extends Mapper<Object, Text, Text, Text>
    {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            String pairs, token, query = conf.get("query");
            String word = value.toString();

            int pos = word.indexOf(' ');
            word = word.substring(0, pos);

            Stemmer st = new Stemmer();
            query = st.stem(query+" ");

            pos = query.indexOf(word);

            if(pos == -1 || query.charAt(pos+word.length()) != ' ')
            {
                context.write(new Text(""), new Text(""));
                return;
            }

            pairs = (value.toString()).substring(word.length());

            StringTokenizer tok = new StringTokenizer(pairs, ",");

            while(tok.hasMoreTokens())
            {
                token = tok.nextToken();
                pos = token.indexOf(':');
                context.write(new Text(token.substring(0,pos).trim()), new Text(word+":"+token.substring(pos+1)));
            }
        }
    }

    //Query Reducer
    public static class QueryReducer extends Reducer<Text,Text,Text,Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            String squery = conf.get("query");
            String token, word, posList;
            int pos;
            Map<String, String> m = new HashMap<String, String>();

            Stemmer st = new Stemmer();
            String query = st.stem(squery+" ");

            if(key.toString().length() == 0)
                return;

            for(Text value: values)
            {
                word = value.toString();
                pos = word.indexOf(':');
                posList = word.substring(pos+1);
                word = word.substring(0,pos);

                if(query.indexOf(' ') == -1 || query.indexOf(' ') == query.length()-1)
                {
                    System.err.println(key+" "+posList);
                    return;
                }

                m.put(word, posList);
            }

            StringTokenizer tok = new StringTokenizer(query);

            while(tok.hasMoreTokens())
            {
                if(m.get(tok.nextToken()) == null)
                    return;
            }

            tok = new StringTokenizer(query);
            StringTokenizer tok1 = new StringTokenizer(squery);
            String w1[], w2[];
            int wi=0, wj=0, wk=0, len, len1;
            int w11[], w22[], w33[];
            boolean querySatisfied = true, queryFlag = false;

            token = tok.nextToken();
            len1 = (tok1.nextToken()).length();
            w1 = (m.get(token)).split(":");

            w11 = new int[w1.length];

            for(int i = 0; i < w1.length; i++)
                w11[i] = Integer.parseInt(w1[i]);
            Arrays.sort(w11);

            while(tok.hasMoreTokens())
            {
                token = tok.nextToken();
                w2 = (m.get(token)).split(":");
                w22 = new int[w2.length];

                w33 = new int[(w22.length < w11.length ? w22.length:w11.length)];

                for(int i = 0; i < w2.length; i++)
                    w22[i] = Integer.parseInt(w2[i]);
                Arrays.sort(w22);

                wi = wj = wk = 0;
                queryFlag = false;

                while(wi < w11.length && wj < w22.length)
                {
                    len = w22[wj] - w11[wi];
                    if(len-1 == len1)
                    {
                        System.err.print(key+":"+w11[wi]+",");
                        w33[wk] = w22[wj];
                        wi++; wj++; wk++;
                        queryFlag = true;
                    }
                    else if(len < len1+1)
                        wj++;
                    else
                        wi++;
                }

                if(queryFlag)
                    System.err.println("\b ");
                w11 = new int[w33.length];

                for(int i = 0; i < w33.length; i++)
                    w11[i] = w33[i];

                len1 = (tok1.nextToken()).length();
                querySatisfied &= queryFlag;
            }

            if(querySatisfied)
                System.err.println(key+" contains the query term");
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setPartitionerClass(InvertPartitioner.class);
        job.setNumReduceTasks(26);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.waitForCompletion(true);

        //query

        int queryNum = 0;

        while(true)
        {
            Scanner s = new Scanner(System.in);
            System.err.print("query> ");
            String query = s.nextLine();

            if(query.length() == 0)
                continue;

            conf = new Configuration();
            conf.set("query", query);
            job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCount.class);

            job.setMapperClass(QueryMapper.class);
            job.setReducerClass(QueryReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            StringBuilder files = new StringBuilder();
            Set<Integer> qs = new HashSet<Integer>();
            StringTokenizer qtok = new StringTokenizer(query);

            while(qtok.hasMoreTokens())
                qs.add((qtok.nextToken()).charAt(0)-'a');

            String path = args[1]+"/"+"part-r-000";
            for(int n : qs)
            {
                files.append(path);
                files.append(n>9?"":"0");
                files.append(Integer.toString(n));
                files.append(",");
            }
            files.deleteCharAt(files.length()-1);
            FileInputFormat.addInputPaths(job, files.toString());
            FileOutputFormat.setOutputPath(job, new Path(args[2]+"/out"+Integer.toString(queryNum++)));
            job.waitForCompletion(true);
        }
    }
}

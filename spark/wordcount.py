#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

from operator import add

from pyspark import SparkContext
import re
import stemmer

def fun(word):
    return re.split("[ \n]+", word)

def fun1(word):
    return re.split("[^a-zA-Z0-9]", word)

def partitioner(tup):
    try:
        if(tup == -1):
            return 0
        if len(tup) == 0:
            return 0
        c = tup[0]
        if c >= 'a':
            return (ord(c)-ord('a'))
        else:
            return (ord(c)-ord('0'))
    except:
        return 0

def mapper(word):
    global pos, idi, flag

    if word == "</doc>":
        idi = 0
        flag = 0
        pos = 0
        return ((), [])
    
    if idi == 0:
        m = re.search("id=\"([0-9]+)\"", word)
        if m != None:
            idi = int(m.group(1))
        return ((), [])

    if (flag == 0) and (word[-1] == '>'):
        flag = 1
        return ((), [])

    if flag == 0:
        return ((), [])
    
    len1 = len(word)
    word = re.sub("[^a-zA-Z0-9 ]+", " ", word)

    if len(word) == 0:
        return ((), [])

    word = stemmer.stem(word+" ")

    pos1 = pos
    pos += len1+1

    return (((wor, idi), [pos1]) for wor in re.split("[ ]+",word))

def queryMapper(x, query):
    x = eval(x)
    squery = stemmer.stem(query+" ")
    if (x[0]+' ') not in squery:
        return ()
    posting = x[1]
    posting = zip(posting, posting[1:])[::2]
    return ((tup[0], [(','.join(map(str, tup[1])),x[0])]) for tup in posting)


if __name__ == "__main__":
    pos = idi = flag = 0
    sc = SparkContext(appName="PythonWordCount")
    #lines = sc.textFile("hdfs://localhost:54310/user/span/input/a[0-1]")
    #counts = lines.flatMap(fun) \
    #              .map(lambda x: x) \
    #              .flatMap(lambda x: mapper(x)) \
    #              .map(lambda x: ((),[]) if len(x)==0 else x) \
    #              .reduceByKey(add) \
    #              .map(lambda x: (-1, -1) if len(x[0])==0 else (x[0][0],(x[0][1],x[1]))) \
    #              .reduceByKey(add) \
    #              .partitionBy(26, lambda x: partitioner(x)) \
    #              .saveAsTextFile("hdfs://localhost:54310/user/span/out/")
    
    #query
    while(True):
        query = "british broadcasting corporation"
        squery = stemmer.stem(query+" ")

        files = set()
        for tok in query.split():
            files.add(int(ord(tok[0])-ord('a')))

        filel=''
        for f in files:
            filel += "hdfs://localhost:54310/user/span/out/part-000"+("" if f > 9 else "0")+str(f)+","
        filel = filel[:-1]

        lines = sc.textFile(filel)

        result = lines.flatMap(lambda x: queryMapper(x, query)) \
                      .reduceByKey(add)

        output = result.collect()
        for a,b in output:
            querySatisfied=True
            queryFlag=False
            m = {}
            flag = 0
            for tup in b:
                m[tup[1]] = map(int, tup[0].split(','))
                m[tup[1]].sort()

            for word in squery.split():
                if word not in m:
                    flag = 1
                    break

            if(flag == 1):
                continue

            wordlist = squery.split()
            wordlist1 = query.split()
            token = wordlist[0]
            len1 = len(wordlist1[0])
            wordlist1.pop(0)
            w11 = m[token]

            for token in wordlist[1:]:
                w22 = m[token]
                wi = wj = 0
                queryFlag = False
                w33 = []

                while(wi < len(w11) and wj < len(w22)):
                    len2 = w22[wj]-w11[wi]
                    if(len2-1 == len1):
                        print(a,":",w11[wi],",")
                        w33 += [w22[wj]]
                        wi += 1
                        wj += 1
                        queryFlag = True
                    elif(len2 < len1+1):
                        wj += 1
                    else:
                        wi += 1
                w11 = w33
                len1 = len(wordlist1[0])
                wordlist1.pop(0)
                querySatisfied &= queryFlag
                
            if querySatisfied:
                print(a," contains the query")
        break;
    
    sc.stop()

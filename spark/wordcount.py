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

def fun(word):
    return re.split('[ \n]+', word)


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
    
    pos1 = pos
    pos += len(word)+1

    return ((word, idi), [pos1])

if __name__ == "__main__":
    pos = idi = flag = 0
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile("[0-1]")
    print(lines)
    counts = lines.flatMap(fun) \
                  .map(lambda x: mapper(x)) \
                  .reduceByKey(add) \
                  .map(lambda x: (-1, -1) if len(x[0])==0 else (x[0][0],(x[0][1],x[1]))) \
                  .reduceByKey(add) \
                  #.map(lambda x: print(x, len(x)))
    
    output = counts.collect()
    for word,count in output:
        print (word, count)
    
    sc.stop()

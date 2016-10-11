#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <regex>
#include <algorithm>
#include <string>
#include <set>
#include <mpi.h>
#include <sys/stat.h>
#include <unistd.h>
#include "porter.h"
#include <chrono>
#include <cmath>

using namespace std;

int size, world_rank;

void
loadDict(map<string, map<int, vector<int> > >& dict, string& querys)
{
    int i = querys[0]-(querys[0]>='a'?'a':'0');
    string term, postingList, out = "inout_"+to_string(i);
    ifstream ifs(out.c_str());
    int id;
    char *pos;

    while(!ifs.eof())
    {
        ifs >> term >> postingList;

        if(term != querys)
            continue;

        stringstream ss(postingList);
        while(getline(ss, postingList, ','))
        {
            char ll[postingList.size()];
            strcpy(ll, postingList.c_str());
            id = atoi(strtok(ll,":|"));
            while((pos = strtok(NULL,":|")) != NULL)
                dict[term][id].push_back(atoi(pos));
        }
        break;
    }
    ifs.close();
}

void
query()
{
    map<string, map<int, vector<int> > > dict;
    MPI_Status status;
    if(world_rank == 0)
    {
        vector<int> s1, s2, w1, w2, w3;
        string squery, squerys, query, querys;
        int c = 0, query_num = 0;
        ofstream graph("graph_mpi", ofstream::out);

        graph <<"query, time\n";
        ifstream ifquery("query");

        while(true)
        {
            query_num++;

            if(query_num >= 1000)
                graph.close();

            map<string, map<int, vector<int> > > resultmap;
            s1.clear();
            s2.clear();
            cerr <<query_num<< " query> ";
            getline(ifquery, squery);
            cerr << squery << "\n";
            chrono::high_resolution_clock::time_point t1 = chrono::high_resolution_clock::now();
            if(squery.size() == 0)
            {
                c++;
                if(c == 2)
                {
                    graph.close();
                    return;
                }
                continue;
            }
            else
                c = 0;
            query = stemfile(squery);
            regex alpha ("[^[:alpha:]0-9 ]");
            query = regex_replace(query, alpha, " ");
            stringstream ss(query);

            while(ss >> querys)
            {
                int i = querys[0]-(querys[0]>='a'?'a':'0');
                i = i%size;
                if(i == 0)
                {
                    loadDict(dict, querys);
                    resultmap[querys] = dict[querys];
                    map<int, vector<int> > myset = dict[querys];
                    s2.resize(myset.size());
                    int k = 0;

                    for (map<int, vector<int> >::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
                        s2[k++] = (it1->first);
                }
                else
                {
                    MPI_Send(querys.c_str(), querys.size()+1, MPI_CHAR, i, 1, MPI_COMM_WORLD);
                    int charSize;

                    MPI_Probe(i, 1, MPI_COMM_WORLD, &status);
                    MPI_Get_count(&status, MPI_CHAR, &charSize);

                    if(charSize<0)
                        continue;

                    char res[charSize];
                    map<int, vector<int> > myset = dict[querys];
                    char *pos;

                    MPI_Recv(&res, charSize, MPI_CHAR, i, 1, MPI_COMM_WORLD, &status);

                    int id = atoi(strtok(res,":|"));
                    while((pos = strtok(NULL,":|")) != NULL)
                        myset[id].push_back(atoi(pos));

                    resultmap[querys] = myset;

                    s2.resize(myset.size());
                    int k = 0;

                    for (map<int, vector<int> >::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
                        s2[k++] = (it1->first);
                }

                if(s1.empty())
                {
                    s1 = s2;
                    s2.clear();
                }
                else
                {
                    vector<int> temp(min(s1.size(), s2.size()));
                    set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(), temp.begin());
                    s1 = temp;
                }
            }

            for(int docid : s1)
            {
                int len1, len, wi = 0, wj = 0;
                bool querySatisfied = true, queryFlag = false;
                stringstream ss(query);
                stringstream ss1(squery);
                ss >> querys;
                ss1 >> squerys;
                len1 = squerys.size();
                w1 = resultmap[querys][docid];
                while(ss >> querys)
                {
                    ss1 >> squerys;
                    w2 = resultmap[querys][docid];
                    //merge

                    wi = wj = 0;
                    queryFlag = false;

                    while(wi < w1.size() && wj < w2.size())
                    {
                        len = w2[wj] - w1[wi];
                        if(len-1 == len1)
                        {
                            cout<<docid<<":"<<w1[wi]<<",";
                            w3.push_back(w2[wj]);
                            wi++; wj++;
                            queryFlag = true;
                        }
                        else if(len < len1+1)
                            wj++;
                        else
                            wi++;
                    }

                    if(queryFlag)
                        cout<<"\b \n";
                    w1 = w3;
                    w3.clear();
                    len1 = squerys.size();
                    querySatisfied &= queryFlag;
                }

                if(querySatisfied)
                    cout<<docid<<" contains the query\n";
            }
            chrono::high_resolution_clock::time_point t2 = chrono::high_resolution_clock::now();

            chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double> >(t2 - t1);
            graph << query_num << ", " << time_span.count()*1000<<endl;
        }
        ifquery.close();
    }
    else
    {
        string result;
        while(true)
        {
            char res[256];
            MPI_Recv(&res, 256, MPI_CHAR, 0, 1, MPI_COMM_WORLD, &status);
            string querys(res);
            loadDict(dict, querys);
            map<int, vector<int> > myset = dict[querys];
            result = "";
            for (map<int, vector<int> >::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
            {
                result += to_string(it1->first) + '|';
                for(int val : it1->second)
                    result += to_string(val)+':';
                result[result.size()-1] = ',';
            }
            result[result.size()-1] = '\n';
            result += "\0";
            MPI_Send(result.c_str(), result.size()+1, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
        }
    }
}

void
reduce(map<string, map<int, vector<int> > >& dict)
{
    string list, token, out;
    char *pos;
    int id, numFiles = (int)ceil(26/size);

    ofstream ofs[26];

    for(int j = world_rank; j < 26; j+=size)
    {
        string out1 = "inout_"+to_string(j);
        ofs[j].open(out1.c_str(), ofstream::out);
    }

    for(int i = 0; i < size; i++)
    {
        out = "out_"+to_string(i)+"_"+to_string(world_rank);
        ifstream ifs(out.c_str());

        while(!ifs.eof())
        {
            ifs >> token >> list;
            stringstream ss(list);
            while(getline(ss, list, ','))
            {
                char ll[list.size()];
                strcpy(ll, list.c_str());
                id = atoi(strtok(ll,":|"));
                while((pos = strtok(NULL,":|")) != NULL)
                    dict[token][id].push_back(atoi(pos));
            }
        }

        ifs.close();
    }

    //debug
    map<int, vector<int> > myset;

    for (map<string, map<int, vector<int> > >::iterator it=dict.begin(); it!=dict.end(); ++it)
    {
        int dest;
        if(it->first[0]>='a')
            dest = it->first[0]-'a';
        else
            dest = it->first[0]-'0';
        myset = it->second;
        ofs[dest] << it->first << " ";
        for (map<int, vector<int> >::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
        {
            ofs[dest] << it1->first << '|';
            for(int pos : it1->second)
                ofs[dest] << pos << ':';
            long pos = ofs[dest].tellp();
            ofs[dest].seekp(pos-1);
            ofs[dest] << ",";
        }
        long pos = ofs[dest].tellp();
        ofs[dest].seekp(pos-1);
        ofs[dest] << "\n";
    }

    for(int i = 0; i < numFiles; i++)
        ofs[i].close();
}

void
parse(ifstream &fp, map<string, map<int, vector<int> > >& dict, size_t fsize)
{
    size_t len, pos=0, tots=0;
    int idi, flag = 0;
    string doc, line, id, text, token;
    smatch sm;
    regex reid ("id=\"([0-9]+)\"");
    regex retitle ("title=\"(.*)\" non");
    regex retext (">(.*)</");

    while(!fp.eof())
    {
        doc.clear();
        fp >> line;
        tots += line.size();
        len = line.size();

        if(line == "</doc>")
        {
            cerr<<"\r"<<tots<<"/"<<fsize<<"("<<world_rank<<"-"<<(tots*100/fsize)<<"%)";
            id.clear();
            flag = 0;
            pos = 0;
            continue;
        }

        if(id.empty())
        {
            regex_search(line, sm, reid);
            id = sm[1];
            idi = atoi(id.c_str());
            continue;
        }

        if(line[line.size()-1] == '>' && flag == 0)
        {
            flag = 1;
            continue;
        }

        if(flag == 0)
            continue;

        transform(line.begin(), line.end(), line.begin(), ::tolower);
        regex alpha ("[^[:alpha:]0-9 ]");
        line = regex_replace(line, alpha, " ");

        stringstream ss(line);

        while(ss >> token)
        {
            //stem
            doc += token + " ";
        }
        doc = stemfile(doc);
        stringstream ss1(doc);

        while(ss1 >> token)
            dict[token][idi].push_back(pos);
        pos += len+1;
    }
    return;
}

void
createIndex()
{
    int i = world_rank;
    string stopwords, line, title = "";
    map<string, map<int, vector<int> > > dict;
    map<int, vector<int> > myset;

    while(true)
    {
        string file = to_string(i);
        ifstream fp (file.c_str());
        if(!fp.good())
        {
            break;
        }
        cerr<<"Parse "<<file<<"from " <<world_rank<<"\n";

        struct stat st;
        stat(file.c_str(), &st);
        parse(fp, dict, st.st_size);
        fp.close();
        i += size;
    }
    //write to disk
    ofstream ofs[size];

    for(i = 0; i < size; i++)
    {
        string out = "out_"+to_string(world_rank)+"_"+to_string(i);
        ofs[i].open(out.c_str(), ofstream::out);
    }

    for (map<string, map<int, vector<int> > >::iterator it=dict.begin(); it!=dict.end(); ++it)
    {
        myset = it->second;
        int ch = it->first[0]-(it->first[0]>='a'?'a':'0');
        ofs[ch % size] << it->first << " ";
        for (map<int, vector<int> >::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
        {
            ofs[ch % size] << it1->first << '|';
            for(int pos : it1->second)
                ofs[ch % size] << pos << ':';
            long pos = ofs[ch % size].tellp();
            ofs[ch % size].seekp(pos-1);
            ofs[ch % size] << ",";
        }
        long pos = ofs[ch % size].tellp();
        ofs[ch % size].seekp(pos-1);
        ofs[ch % size] << "\n";
    }

    for(i = 0; i < size; i++)
        ofs[i].close();
    return;
}

int
main(void)
{
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    cerr<<world_rank<<" "<<getpid()<<"\n";

    string stopwords;
    map<string, map<int, vector<int> > > dict;

    createIndex();

    cerr<<"Done "<<world_rank<<"\n";
    //start reduce
    MPI_Barrier(MPI_COMM_WORLD);
    reduce(dict);

    MPI_Barrier(MPI_COMM_WORLD);

    query();

    MPI_Finalize();
    return 0;
}

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

using namespace std;

int size, world_rank;

void
query(map<string, map<int, vector<int> > >& dict)
{
    MPI_Status status;
    if(world_rank == 0)
    {
        vector<int> s1, s2, w1, w2;
        string query, querys;
        int c = 0;
        while(true)
        {
            map<string, map<int, vector<int> > > resultmap;
            s1.clear();
            s2.clear();
            cout << "query> ";
            getline(cin, query);
            if(query.size() == 0)
            {
                c++;
                if(c == 2)
                    return;
                continue;
            }
            else
                c = 0;
            query = stemfile(query);
            stringstream ss(query);

            while(ss >> querys)
            {
                int i = querys[0]-'a';
                i = i % size;
                if(i == 0)
                {
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
                    resultmap[querys] = dict[querys];
                    map<int, vector<int> > myset = dict[querys];
                    char *pos;

                    MPI_Recv(&res, charSize, MPI_CHAR, i, 1, MPI_COMM_WORLD, &status);

                    int id = atoi(strtok(res,":|"));
                    while((pos = strtok(NULL,":|")) != NULL)
                        myset[id].push_back(atoi(pos));

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
                stringstream ss(query);
                ss >> querys;
                len1 = querys.size();
                w1 = resultmap[querys][docid];
                while(ss >> querys)
                {
                    w2 = resultmap[querys][docid];

                    //merge

                    wi = wj = 0;
                    while(wi < w1.size() && wj < w2.size())
                    {
                        len = w2[wj] - w1[wi];
                        if(len-1 == len1)
                        {
                            cout<<docid<<",";
                            wi++; wj++;
                        }
                        else if(len < len1+1)
                            wj++;
                        else
                            wi++;
                    }
                    cout<<"\b \n";
                    w1 = w2;
                }
            }
        }
    }
    else
    {
        string result;
        while(true)
        {
            char res[256];
            MPI_Recv(&res, 256, MPI_CHAR, 0, 1, MPI_COMM_WORLD, &status);
            string querys(res);
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
    string list, token, out, out1;
    char *pos;
    out1 = "inout_"+to_string(world_rank)+"_"+to_string(world_rank);
    ofstream ofs(out1.c_str(), ofstream::out);
    int id;

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

        //debug
        map<int, vector<int> > myset;

        for (map<string, map<int, vector<int> > >::iterator it=dict.begin(); it!=dict.end(); ++it)
        {
            myset = it->second;
            ofs << it->first << " ";
            for (map<int, vector<int> >::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
            {
                ofs << it1->first << '|';
                for(int pos : it1->second)
                    ofs << pos << ':';
                long pos = ofs.tellp();
                ofs.seekp(pos-1);
                ofs << ",";
            }
            long pos = ofs.tellp();
            ofs.seekp(pos-1);
            ofs << "\n";
        }

        ifs.close();
    }
    ofs.close();
}

void
parse(ifstream &fp, const vector<string>& sw, map<string, map<int, vector<int> > >& dict, size_t fsize)
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
        regex alpha ("[^[:alpha:] ]");
        line = regex_replace(line, alpha, " ");

        stringstream ss(line);

        while(ss >> token)
        {
            if(binary_search(sw.begin(), sw.end(), token))
                continue;

            //stem
            doc += token + " ";
        }
        doc = stemfile(doc);
        stringstream ss1(doc);

        while(ss1 >> token)
            dict[token][idi].push_back(pos);
        pos += len+1;
    }
}

void
createIndex(const vector<string>& sw)
{
    int i = world_rank;
    string stopwords, line, title = "";
    map<string, map<int, vector<int> > > dict;
    map<int, vector<int> > myset;

    while(true)
    {
        string file = to_string(i);
        ifstream fp (file.c_str());
        if(fp.fail())
            break;
        cerr<<"Parse "<<file<<"from " <<world_rank<<"\n";

        struct stat st;
        stat(file.c_str(), &st);
        parse(fp, sw, dict, st.st_size);
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
        int ch = it->first[0]-'a';
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
    vector<string> sw;

    ifstream swfp ("stopwords.dat");
    while(!swfp.eof())
    {
        swfp >> stopwords;
        sw.push_back(stopwords);
    }
    swfp.close();

    //createIndex(sw);

    //start reduce
    reduce(dict);

    MPI_Barrier(MPI_COMM_WORLD);

    query(dict);

    MPI_Finalize();
    return 0;
}

#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <regex>
#include <algorithm>
#include <string>
#include <set>
#include <mpi.h>
#include "porter.h"

using namespace std;

int size, world_rank;

void
query(map<string, set<int> >& dict)
{
    char res[256];
    MPI_Status status;
    if(world_rank == 0)
    {
        string querys;
        int c = 0;
        while(true)
        {
            cout << "query> ";
            getline(cin, querys);
            if(querys.size() == 0)
            {
                c++;
                if(c == 2)
                    return;
                continue;
            }
            else
                c = 0;
            querys = stemfile(querys);
            int i = querys[0]-'a';
            i = i % size;
            if(i == 0)
            {
                set<int> myset = dict[querys];
                for (set<int>::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
                    cout << *it1 << ',';
                cout << "\b \n";
            }
            else
            {
                MPI_Send(querys.c_str(), querys.size()+1, MPI_CHAR, i, 0, MPI_COMM_WORLD);
                while(true)
                {
                    MPI_Recv(&res, 256, MPI_CHAR, i, 0, MPI_COMM_WORLD, &status);
                    string result(res);
                    cout<<result;
                    if(result.size() < 256)
                        break;
                }
                cout<<"\n";
            }
        }
    }
    else
    {
        string result;
        while(true)
        {
            MPI_Recv(&res, 256, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
            string querys(res);
            set<int> myset = dict[querys];
            result = "";
            for (set<int>::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
            {
                result += to_string(*it1) + ',';
            }
            result[result.size()-1] = '\n';
            result += "\0";
            MPI_Send(result.c_str(), result.size()+1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        }
    }
}

void
sendFiles(int i)
{
    string line;
    string out = "out_"+to_string(world_rank)+"_"+to_string(i);
    ifstream ofs(out.c_str());
    while(!ofs.eof())
    {
        getline(ofs, line);
        MPI_Send(line.c_str(), line.size()+1, MPI_CHAR, i, 0, MPI_COMM_WORLD);
        line.clear();
    }
    ofs.close();
    cerr<<world_rank<<" "<<i<<"Closeing\n";
}

void
recvFiles()
{
    int count = 0;
    char res[256];
    MPI_Status status;

    string out = "out_"+to_string(world_rank)+"_"+to_string(world_rank);
    ofstream ofs(out.c_str(), ofstream::app);
    while(true)
    {
        MPI_Recv(&res, 256, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        string line(res);
        if(line.size() == 0)
        {
            cerr<<world_rank<<"Done\n";
            count++;
            if(count+1 >= size)
                return;
            continue;
        }
        line += "\n";
        ofs<<line;
    }
    ofs.close();
}

void
reduce(map<string, set<int> >& dict)
{
    for(int i = 0; i < size; i++)
    {
        if(i == world_rank)
            recvFiles();
        sendFiles(i);
    }

    string list, token, out = "out_"+to_string(world_rank)+"_"+to_string(world_rank);
    
    ifstream ifs(out.c_str());
    string out1 = "in"+out;
    ofstream ofs(out1.c_str(), ofstream::out);

    while(!ifs.eof())
    {
        ifs >> token >> list;
        stringstream ss(list);
        while(getline(ss, list, ','))
            dict[token].insert(atoi(list.c_str()));
    }

    //debug
    set<int> myset;
    for (map<string,set<int> >::iterator it=dict.begin(); it!=dict.end(); ++it)
    {
        myset = it->second;
        ofs << it->first << " ";
        for (set<int>::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
            ofs << *it1 << ',';
        long pos = ofs.tellp();
        ofs.seekp(pos-1);
        ofs << "\n";
    }

    ifs.close();
    ofs.close();
}

void
parse(ifstream &fp, const vector<string>& sw, map<string, set<int> >& dict)
{
    string line, doc, id, title, text, token;
    smatch sm;

    while(!fp.eof())
    {
        fp >> line;
        doc += line+" ";
        if(line == "</doc>")
            break;
    }

    regex reid ("id=\"([0-9]+)\"");
    regex retitle ("title=\"(.*)\" non");
    regex retext (">(.*)</");

    regex_search(doc, sm, reid);
    id = sm[1];
    int idi = atoi(id.c_str());

    regex_search(doc, sm, retitle);
    title = sm[1];

    regex_search(doc, sm, retext);
    text = sm[1];

    if(id == "" || title == "" || text == "")
        return;

    doc = "";
    text = title + text;
    transform(text.begin(), text.end(), text.begin(), ::tolower);
    regex alpha ("[^[:alpha:] ]");
    text = regex_replace(text, alpha, " ");

    stringstream ss(text);

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
        dict[token].insert(idi);
}

void
createIndex(const vector<string>& sw)
{
    int i = world_rank;
    string stopwords, line, title = "";
    map<string, set<int> > dict;
    set<int> myset;

    while(true)
    {
        string file = to_string(i);
        ifstream fp (file.c_str());
        if(fp.fail())
            break;
        cerr<<"Parse "<<file<<"from " <<world_rank<<"\n";
        parse(fp, sw, dict);
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

    for (map<string,set<int> >::iterator it=dict.begin(); it!=dict.end(); ++it)
    {
        myset = it->second;
        int ch = it->first[0]-'a';
        ofs[ch % size] << it->first << " ";
        for (set<int>::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
            ofs[ch % size] << *it1 << ',';
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

    string stopwords;
    map<string, set<int> > dict;
    vector<string> sw;

    ifstream swfp ("stopwords.dat");
    while(!swfp.eof())
    {
        swfp >> stopwords;
        sw.push_back(stopwords);
    }
    swfp.close();

    createIndex(sw);

    //start reduce
    reduce(dict);

    query(dict);

    MPI_Finalize();
    return 0;
}

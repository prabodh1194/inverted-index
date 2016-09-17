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
    regex alpha ("[^a-z0-9 ]");
    text = regex_replace(text, alpha, "");

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
        cerr<<"Parse "<<file<<"from " <<world_rank<<endl;
        parse(fp, sw, dict);
        fp.close();
        i += size;
    }
    //write to disk

    string out = "out"+to_string(world_rank);
    ofstream ofs (out, ofstream::out);
    for (map<string,set<int>>::iterator it=dict.begin(); it!=dict.end(); ++it)
    {
        myset = it->second;
        ofs << it->first << ":";
        for (set<int>::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
            ofs << *it1 << ',';
        ofs << "\b\n";
    }
    ofs.close();
}

int
main(void)
{
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    string stopwords;
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

    MPI_Finalize();
    return 0;
}

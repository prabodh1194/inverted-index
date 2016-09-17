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
parse(ifstream &fp, const vector<string>& sw)
{
    string line, doc, id, title, text, token;
    map<string, set<int> > dict;
    smatch sm;
    set<int> myset;

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

    //write to disk
    for (map<string,set<int>>::iterator it=dict.begin(); it!=dict.end(); ++it)
    {
        myset = it->second;
        cout << it->first << " => ";
        for (set<int>::iterator it1=myset.begin(); it1!=myset.end(); ++it1)
            std::cout << ' ' << *it1;
        cout << '\n';
    }
}

void
createIndex(const vector<string>& sw)
{
    int i = world_rank;
    string stopwords, line, title = "";

    while(true)
    {
        string file = to_string(i);
        ifstream fp (file.c_str());
        if(fp.fail())
            return;
        parse(fp, sw);
        fp.close();
        i += size;
    }
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

    MPI_Finalize();
    return 0;
}

#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <regex>
#include <algorithm>
#include <string>
#include <set>
#include "porter.h"

using namespace std;

int world_size, world_rank;

void
reduce(map<string, set<int> > dict)
{

}

void
parse(ifstream &fp, const vector<string>& sw)
{
    string line, doc, id, title, text, token;
    map<string, set<int> > dict;
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
    reduce(dict);
}

void
createIndex(string file, const vector<string>& sw)
{
    int length = -1, i = 0, p = -1;
    string stopwords, line, title = "";
    ifstream fp (file.c_str());
    parse(fp, sw);
    fp.close();
}

int
main(void)
{
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
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

    createIndex("englishText_0_10000", sw);

    MPI_Finalize();
    return 0;
}

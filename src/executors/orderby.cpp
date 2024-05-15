#include "global.h"

/**
 * @brief 
 * SYNTAX: R <- ORDER BY <attribute> ASC|DESC ON <relationname>
 */
bool syntacticParseORDERBY()
{
    logger.log("syntacticParseORDERBY");

    if (tokenizedQuery.size() != 8 || tokenizedQuery[3]!="BY" || tokenizedQuery[6]!="ON")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = ORDERBY;
    parsedQuery.orderbyResultRelationName = tokenizedQuery[0];
    parsedQuery.orderbyAttributeName = tokenizedQuery[4];
    parsedQuery.orderbyRelationName = tokenizedQuery[7];
    if (tokenizedQuery[5] != "ASC" && tokenizedQuery[5] != "DESC")
    {
        cout << "SYNTAX ERROR: Invalid Sorting Strategy" << endl;
        return false;
    }
    else if (tokenizedQuery[5] == "ASC")
    {
        parsedQuery.orderbySortingStrategy = ASC;
    }
    else
    {
        parsedQuery.orderbySortingStrategy = DESC;
    }
    return true;
}

bool semanticParseORDERBY()
{
    logger.log("semanticParseORDERBY");
    //Both tables must exist and resultant table shouldn't
    if (tableCatalogue.isTable(parsedQuery.orderbyResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.orderbyRelationName))
    {
        cout << "SEMANTIC ERROR: Order by relation does not exist" << endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.orderbyAttributeName, parsedQuery.orderbyRelationName))
    {
        cout << "SEMANTIC ERROR: The attribute does not exist in specified relation" << endl;
        return false;
    }

    return true;
}

void executeORDERBY()
{
    logger.log("executeORDERBY");

    // cout<<"Resultant Relation Name: "<<parsedQuery.orderbyResultRelationName<<endl;
    // cout<<"Relation Name: "<<parsedQuery.orderbyRelationName<<endl;
    // cout<<"Attribute Name: "<<parsedQuery.orderbyAttributeName<<endl;
    // cout<<"Sorting Strategy: "<<parsedQuery.orderbySortingStrategy<<endl;


    Table table = *tableCatalogue.getTable(parsedQuery.orderbyRelationName);

    Table* resultantTable = new Table(parsedQuery.orderbyResultRelationName, table.columns);

    Cursor cursor = table.getCursor();
    vector<int> row = cursor.getNext();

    while(!row.empty())
    {
        resultantTable->writeRow(row);
        row = cursor.getNext();
    }

    if(resultantTable->blockify())
    {
        tableCatalogue.insertTable(resultantTable);
    }

    parsedQuery.sortColumnIndices.clear();

    for(int j=0; j<table.columnCount;j++)
    {
        if(parsedQuery.orderbyAttributeName == table.columns[j])
        {
            parsedQuery.sortColumnIndices.emplace_back(j);
            break;
        }
    }

    parsedQuery.sortColumnCount = 1;

    parsedQuery.sortTypeList.clear();

    if(parsedQuery.orderbySortingStrategy == ASC)
    {
        parsedQuery.sortTypeList.emplace_back("ASC");
    }
    else
    {
        parsedQuery.sortTypeList.emplace_back("DESC");
    }

    resultantTable->tableSort();

    return;
}
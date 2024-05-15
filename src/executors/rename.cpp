#include "global.h"
/**
 * @brief 
 * SYNTAX: RENAME column_name TO column_name FROM relation_name
 */
bool syntacticParseRENAME()
{
    logger.log("syntacticParseRENAME");
    if (tokenizedQuery.size() != 6 || tokenizedQuery[2] != "TO" || tokenizedQuery[4] != "FROM")
    {
        if(tokenizedQuery.size() == 4 && tokenizedQuery[1] == "MATRIX"){
            parsedQuery.queryType = RENAME;
            parsedQuery.renameRelationName = tokenizedQuery[2];
            parsedQuery.renameFromColumnName = tokenizedQuery[2];
            parsedQuery.renameToColumnName = tokenizedQuery[3];
            return true;
        }

        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = RENAME;
    parsedQuery.renameFromColumnName = tokenizedQuery[1];
    parsedQuery.renameToColumnName = tokenizedQuery[3];
    parsedQuery.renameRelationName = tokenizedQuery[5];
    return true;
}

bool semanticParseRENAME()
{
    logger.log("semanticParseRENAME");

    if(tokenizedQuery.size() == 4 && tokenizedQuery[1] == "MATRIX"){
        if (!tableCatalogue.isTable(parsedQuery.renameRelationName))
        {
            cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
            return false;
        }
        if (tableCatalogue.isTable(parsedQuery.renameToColumnName))
        {
            cout << "SEMANTIC ERROR: New relation name already exists" << endl;
            return false;
        }
        Table* table = tableCatalogue.getTable(parsedQuery.renameRelationName);
        if(!table->isMatrix){
            cout << "SEMANTIC ERROR: Relation is not a matrix" << endl;
            return false;
        }
        return true;
    }

    if (!tableCatalogue.isTable(parsedQuery.renameRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.renameFromColumnName, parsedQuery.renameRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if (tableCatalogue.isColumnFromTable(parsedQuery.renameToColumnName, parsedQuery.renameRelationName))
    {
        cout << "SEMANTIC ERROR: Column with name already exists" << endl;
        return false;
    }
    return true;
}

void executeRENAME()
{
    logger.log("executeRENAME");

    if(tokenizedQuery.size() == 4 && tokenizedQuery[1] == "MATRIX"){
        tableCatalogue.renameTable(parsedQuery.renameFromColumnName, parsedQuery.renameToColumnName);
        return;
    }

    Table* table = tableCatalogue.getTable(parsedQuery.renameRelationName);
    table->renameColumn(parsedQuery.renameFromColumnName, parsedQuery.renameToColumnName);
    return;
}
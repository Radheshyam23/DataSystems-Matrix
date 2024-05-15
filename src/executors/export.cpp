#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT <relation_name> 
 */

bool syntacticParseEXPORT()
{
    logger.log("syntacticParseEXPORT");
    if (tokenizedQuery.size() != 2)
    {
        if (tokenizedQuery.size() == 3 && tokenizedQuery[1] == "MATRIX")
        {
            parsedQuery.queryType = EXPORT;
            parsedQuery.exportRelationName = tokenizedQuery[2];
            return true;
        }
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORT;
    parsedQuery.exportRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseEXPORT()
{
    logger.log("semanticParseEXPORT");
    //Table should exist
    if (!tableCatalogue.isTable(parsedQuery.exportRelationName))
    {
        cout << "SEMANTIC ERROR: No such relation exists" << endl;
        return false;
    }
    
    if (tokenizedQuery[1] == "MATRIX" && !tableCatalogue.getTable(parsedQuery.exportRelationName)->isMatrix)
    {
        cout << "SEMANTIC ERROR: Relation is not a matrix" << endl;
        return false;
    }
    
    // This case is never going to be accessed as long as there is not an equivalent "Rename" command for tables,
    // similar to the one for matrices (because as of right now, the "Rename" for tables renames the table columns
    // and not the table name itself).
    if (tokenizedQuery[1] != "MATRIX" && tableCatalogue.getTable(parsedQuery.exportRelationName)->isMatrix)
    {
        cout << "SEMANTIC ERROR: Relation is a matrix, not a table" << endl;
        return false;
    }
    
    return true;
}

void executeEXPORT()
{
    logger.log("executeEXPORT");
    Table* table = tableCatalogue.getTable(parsedQuery.exportRelationName);

    if (tokenizedQuery.size() == 3 && tokenizedQuery[1] == "MATRIX")
    {
        table->isMatrix = true;
    }

    table->makePermanent();
    return;
}
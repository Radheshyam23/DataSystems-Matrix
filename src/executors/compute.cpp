#include "global.h"
/**
 * @brief 
 * SYNTAX: COMPUTE relation_name
 */
bool syntacticParseCOMPUTE()
{
    logger.log("syntacticParseCOMPUTE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = COMPUTE;
    parsedQuery.loadRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseCOMPUTE()
{
    logger.log("semanticParseCOMPUTE");

    if (!tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: matrix does not exist" << endl;
        return false;
    }
    Table *matrix = tableCatalogue.getTable(parsedQuery.loadRelationName);
    if (!matrix->isMatrix)
    {
        cout<<"SEMANTIC ERROR: not a valid matrix"<<endl;
        return false;
    }
    return true;
}

void executeCOMPUTE()
{
    logger.log("executeCOMPUTE");

    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);

    Table* resultTable = new Table(parsedQuery.loadRelationName + "_RESULT");

    table->compute(resultTable);

}
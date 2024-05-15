#include "global.h"
/**
 * @brief 
 * SYNTAX: SOURCE filename
 */
bool syntacticParseTRANSPOSE()
{
    logger.log("syntacticParseTRANSPOSE");
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = TRANSPOSE;
    parsedQuery.loadRelationName = tokenizedQuery[2];
    return true;
}

bool semanticParseTRANSPOSE()
{
    logger.log("semanticParseTRANSPOSE");
    // check if the matrix is already loaded
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

void executeTRANSPOSE()
{
    logger.log("executeTRANSPOSE");

    Table *matrix = tableCatalogue.getTable(parsedQuery.sourceFileName);

    matrix->matrixTranspose();


    // string sourcefilename = "../data/";

    return;
}

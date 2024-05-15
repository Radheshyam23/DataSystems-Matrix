#include "global.h"
/**
 * @brief 
 * SYNTAX: CHECKSYMMETRY relation_name
 */
bool syntacticParseCHECKSYMMETRY()
{
    logger.log("syntacticParseCHECKSYMMETRY");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CHECKSYMMETRY;
    parsedQuery.loadRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseCHECKSYMMETRY()
{
    logger.log("semanticParseCHECKSYMMETRY");

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

void executeCHECKSYMMETRY()
{
    logger.log("executeCHECKSYMMETRY");

    Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);

    if(table->isSymmetric())
    {
        cout<<"TRUE"<<endl;
    }
    else
    {
        cout<<"FALSE"<<endl;
    }

}
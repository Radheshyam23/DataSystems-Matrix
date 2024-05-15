#include"global.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 * 
 * sorting_order = ASC | DESC 
 */
bool syntacticParseSORT(){
    logger.log("syntacticParseSORT");
    // if(tokenizedQuery.size()< 6 || tokenizedQuery[2] != "BY" || tokenizedQuery[6] != "IN"){
    //     cout<<"SYNTAX ERROR"<<endl;
    //     return false;
    // }
    if(tokenizedQuery.size()< 6 || tokenizedQuery[2] != "BY"){
        cout<<"SYNTAX ERROR: QUERY IN INCORRECT FORMAT"<<endl;
        return false;
    }

    parsedQuery.sortColumnCount = 0;

    for(int i=3;i<tokenizedQuery.size();i++){
        if(tokenizedQuery[i] != "IN"){
            if(i+1 == tokenizedQuery.size()){
                cout<<"SYNTAX ERROR: SORT TYPE NOT SPECIFIED"<<endl;
                return false;
            }
            parsedQuery.sortColumnList.emplace_back(tokenizedQuery[i]);
            parsedQuery.sortColumnCount++;
        }
        else
        {
            break;
        }
    }

    if(tokenizedQuery.size()!= 4 + (parsedQuery.sortColumnCount)*2){
        cout<<"SYNTAX ERROR: COLUMN COUNT DOESNT MATCH SORT METHOD OCCURRENCES"<<endl;
        cout<<tokenizedQuery.size()<<" "<<parsedQuery.sortColumnCount<<endl;
        return false;
    }

    for(int i=4+parsedQuery.sortColumnCount;i<tokenizedQuery.size();i++){
        if(tokenizedQuery[i]!= "ASC" && tokenizedQuery[i]!= "DESC"){
            cout<<"SYNTAX ERROR: INVALID SORT TYPE"<<endl;
            return false;
        }

        parsedQuery.sortTypeList.emplace_back(tokenizedQuery[i]);
    }


    parsedQuery.queryType = SORT;
    // parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortRelationName = tokenizedQuery[1];
    // parsedQuery.sortColumnName = tokenizedQuery[5];
    // string sortingStrategy = tokenizedQuery[7];
    // if(sortingStrategy == "ASC")
    //     parsedQuery.sortingStrategy = ASC;
    // else if(sortingStrategy == "DESC")
    //     parsedQuery.sortingStrategy = DESC;
    // else{
    //     cout<<"SYNTAX ERROR"<<endl;
    //     return false;
    // }
    return true;
}

bool semanticParseSORT(){
    logger.log("semanticParseSORT");

    // if(tableCatalogue.isTable(parsedQuery.sortResultRelationName)){
    //     cout<<"SEMANTIC ERROR: Resultant relation already exists"<<endl;
    //     return false;
    // }

    if(!tableCatalogue.isTable(parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Relation doesn't exist"<<endl;
        return false;
    }

    for(int i=0;i<parsedQuery.sortColumnCount;i++){
        if(!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnList[i], parsedQuery.sortRelationName)){
            cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
            return false;
        }
    }

    // if(!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName)){
    //     cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
    //     return false;
    // }

    return true;
}

void executeSORT(){
    logger.log("executeSORT");

    parsedQuery.sortColumnIndices.clear();

    Table* table = tableCatalogue.getTable(parsedQuery.sortRelationName);
    for (int i=0; i<parsedQuery.sortColumnCount; i++)
    {
        for (int j=0; j<table->columnCount; j++)
        {
            if (parsedQuery.sortColumnList[i] == table->columns[j])
            {
                // cout<<parsedQuery.sortColumnList[i]<<" "<<table->columns[j]<<endl;
                parsedQuery.sortColumnIndices.emplace_back(j);
                break;
            }
        }
    }

    // for(int i=0;i<parsedQuery.sortColumnCount;i++){
    //     cout<<parsedQuery.sortColumnList[i]<<" "<<parsedQuery.sortTypeList[i]<<" "<<parsedQuery.sortColumnIndices[i]<<endl;
    // }

    table->tableSort();
    return;
}
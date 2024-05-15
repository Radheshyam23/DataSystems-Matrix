#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 9 || tokenizedQuery[5] != "ON")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[3];
    parsedQuery.joinSecondRelationName = tokenizedQuery[4];
    parsedQuery.joinFirstColumnName = tokenizedQuery[6];
    parsedQuery.joinSecondColumnName = tokenizedQuery[8];

    string binaryOperator = tokenizedQuery[7];
    if (binaryOperator == "<")
        parsedQuery.joinBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.joinBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.joinBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.joinBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

// Function to load a table from a CSV file
void loadTableFromCSV(ifstream &file, vector<vector<string>> &table)
{
    if (!file.is_open())
    {
        cout << "Error: File not opened successfully" << endl;
        return;
    }

    string line;
    while (getline(file, line))
    {
        istringstream iss(line);
        vector<string> row;
        string value;

        while (getline(iss, value, ','))
        {
            row.push_back(value);
        }

        table.push_back(row);
    }

    // Print the tables for debugging
    // cout << "Load Table function output :" << endl;
    // for (const vector<string> &row : table)
    // {
    //     for (const string &col : row)
    //     {
    //         cout << col << " ";
    //     }
    //     cout << endl;
    // }
}

// Function to write a row to a CSV file
void writeRowToCSV(const vector<string> &row, ofstream &fout)
{
    for (size_t i = 0; i < row.size(); ++i)
    {
        if (i != 0)
        {
            fout << ", ";
        }
        fout << row[i];
    }
    fout << endl;
}

// Function to trim leading and trailing spaces from a string
std::string trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\n\r");
    size_t end = str.find_last_not_of(" \t\n\r");

    if (start != std::string::npos && end != std::string::npos)
        return str.substr(start, end - start + 1);

    return "";
}

void executeJOIN()
{
    logger.log("executeJOIN");

    // Firstly, use "orderby.cpp" to sort the two tables on the join columns (ASC)
    parsedQuery.orderbyResultRelationName = parsedQuery.joinFirstRelationName + "_sorted";
    parsedQuery.orderbyAttributeName = parsedQuery.joinFirstColumnName;
    parsedQuery.orderbyRelationName = parsedQuery.joinFirstRelationName;
    parsedQuery.orderbySortingStrategy = ASC;
    executeORDERBY();

    parsedQuery.orderbyResultRelationName = parsedQuery.joinSecondRelationName + "_sorted";
    parsedQuery.orderbyAttributeName = parsedQuery.joinSecondColumnName;
    parsedQuery.orderbyRelationName = parsedQuery.joinSecondRelationName;
    parsedQuery.orderbySortingStrategy = ASC;
    executeORDERBY();

    // Export the sorted tables to CSV files
    parsedQuery.exportRelationName = parsedQuery.joinFirstRelationName + "_sorted";
    executeEXPORT();

    parsedQuery.exportRelationName = parsedQuery.joinSecondRelationName + "_sorted";
    executeEXPORT();

    logger.log("executeJOIN: Exported sorted tables to CSV files");

    // Open the CSV files
    string file1Name = "../data/" + parsedQuery.joinFirstRelationName + "_sorted.csv";
    string file2Name = "../data/" + parsedQuery.joinSecondRelationName + "_sorted.csv";

    ifstream file1(file1Name);
    ifstream file2(file2Name);

    // Print the names for debugging
    // cout << "File 1: " << file1Name << endl;
    // cout << "File 2: " << file2Name << endl;

    logger.log("executeJOIN: Opened CSV files");

    // Load the tables from the CSV files
    vector<vector<string>> table1, table2;
    loadTableFromCSV(file1, table1);
    loadTableFromCSV(file2, table2);

    // Print the tables for debugging
    // cout << "Table 1:" << endl;
    // for (const vector<string> &row : table1)
    // {
    //     for (const string &col : row)
    //     {
    //         cout << col << " ";
    //     }
    //     cout << endl;
    // }
    // cout << "Table 2:" << endl;
    // for (const vector<string> &row : table2)
    // {
    //     for (const string &col : row)
    //     {
    //         cout << col << " ";
    //     }
    //     cout << endl;
    // }

    logger.log("executeJOIN: Loaded tables from CSV files");

    // Create a new result table with columns from both tables
    vector<string> resultColumns;
    for (const string &col : table1[0])
    {
        resultColumns.push_back(col);
    }
    for (const string &col : table2[0])
    {
        resultColumns.push_back(col);
    }

    logger.log("executeJOIN: Created temp vector conbining columns from both tables");

    // Create a new CSV file for the result
    string resultFilePath = "../data/" + parsedQuery.joinResultRelationName + ".csv";
    ofstream resultFile(resultFilePath);
    writeRowToCSV(resultColumns, resultFile);

    logger.log("executeJOIN: Created result table (initially empty) and CSV file");


    // Get column indices of the join columns
    int joinColumnIndex1 = -1, joinColumnIndex2 = -1;
    for (int i = 0; i < table1[0].size(); i++)
    {
        // cout << "Column name: " << table1[0][i] << endl;
        // cout << "Parsed query column name: " << parsedQuery.joinFirstColumnName << endl;
        // Print difference between the two strings for debugging 
        // cout << "Difference: " << table1[0][i].compare(parsedQuery.joinFirstColumnName) << endl;
        // for (size_t j = 0; j < std::min(table1[0][i].length(), parsedQuery.joinFirstColumnName.length()); ++j) {
        //     if (table1[0][i][j] != parsedQuery.joinFirstColumnName[j]) {
        //         cout << "Difference at position " << j << ": " << table1[0][i][j] << " vs " << parsedQuery.joinFirstColumnName[j] << endl;
        //     }
        // }

        // Trim the strings before comparing
        if (trim(table1[0][i]) == trim(parsedQuery.joinFirstColumnName))
        {
            // cout << "Found column index for " << i << endl;
            joinColumnIndex1 = i;
            break;
        }
    }
    for (int i = 0; i < table2[0].size(); i++)
    {
        if (trim(table2[0][i]) == trim(parsedQuery.joinSecondColumnName))
        {
            joinColumnIndex2 = i;
            break;
        }
    }

    // Print the column names for debugging
    // cout << "Column 1: " << parsedQuery.joinFirstColumnName << endl;
    // cout << "Column 2: " << parsedQuery.joinSecondColumnName << endl;
    // cout << "Column 1 index: " << joinColumnIndex1 << endl;
    // cout << "Column 2 index: " << joinColumnIndex2 << endl;

    // If the join columns are not found, then there is some error
    if (joinColumnIndex1 == -1 || joinColumnIndex2 == -1)
    {
        cout << "INTERNAL ERROR: Join columns not found" << endl;
        return;
    }

    logger.log("executeJOIN: Got column indices of join columns, about to enter loop.");

    // Implement the sort-merge join algorithm
    size_t i = 1, j = 1;

    // Simple equi-join i.e. binary operator is EQUAL (==)

    if(parsedQuery.joinBinaryOperator == EQUAL){
        while (i < table1.size() && j < table2.size())
        {
            // Print the values of the join columns for debugging
            // cout << "Table 1 value: " << table1[i][joinColumnIndex1] << endl;
            // cout << "Table 2 value: " << table2[j][joinColumnIndex2] << endl;

            if (stoi(table1[i][joinColumnIndex1]) < stoi(table2[j][joinColumnIndex2]))
            {
                ++i;
            }
            else if (stoi(table1[i][joinColumnIndex1]) > stoi(table2[j][joinColumnIndex2]))
            {
                ++j;
            }
            else
            {
                // Rows match, perform join
                vector<string> resultRow;

                // Output the combined tuple <R(i), S(j)> to result CSV file
                for (size_t col = 0; col < table1[i].size(); ++col)
                {
                    resultRow.push_back(table1[i][col]);
                }
                for (size_t col = 0; col < table2[j].size(); ++col)
                {
                    resultRow.push_back(table2[j][col]);
                }
                writeRowToCSV(resultRow, resultFile);

                // Output other tuples that match R(i), if any
                size_t l = j + 1;
                while (l < table2.size() && stoi(table1[i][joinColumnIndex1]) == stoi(table2[l][joinColumnIndex2]))
                {
                    resultRow.clear();
                    for (size_t col = 0; col < table1[i].size(); ++col)
                    {
                        resultRow.push_back(table1[i][col]);
                    }
                    for (size_t col = 0; col < table2[l].size(); ++col)
                    {
                        resultRow.push_back(table2[l][col]);
                    }
                    writeRowToCSV(resultRow, resultFile);
                    ++l;
                }

                // // Output other tuples that match S(j), if any
                // size_t k = i + 1;
                // while (k < table1.size() && stoi(table1[k][joinColumnIndex1]) == stoi(table2[j][joinColumnIndex2]))
                // {
                //     resultRow.clear();
                //     for (size_t col = 0; col < table1[k].size(); ++col)
                //     {
                //         resultRow.push_back(table1[k][col]);
                //     }
                //     for (size_t col = 0; col < table2[j].size(); ++col)
                //     {
                //         resultRow.push_back(table2[j][col]);
                //     }
                //     writeRowToCSV(resultRow, resultFile);
                //     ++k;
                // }

                // // Increment both pointers accordingly
                // i = k;
                // j = l;

                i++;
            }
        }
    }
    else if(parsedQuery.joinBinaryOperator == LESS_THAN){
        while (i < table1.size() && j < table2.size())
        {
            // Print the values of the join columns for debugging
            // cout << "Table 1 value: " << table1[i][joinColumnIndex1] << endl;
            // cout << "Table 2 value: " << table2[j][joinColumnIndex2] << endl;

            if (stoi(table1[i][joinColumnIndex1]) < stoi(table2[j][joinColumnIndex2]))
            {
                vector<string> resultRow;

                // Output the combined tuple <R(i), S(j)> to result CSV file
                for (size_t col = 0; col < table1[i].size(); ++col)
                {
                    resultRow.push_back(table1[i][col]);
                }
                for (size_t col = 0; col < table2[j].size(); ++col)
                {
                    resultRow.push_back(table2[j][col]);
                }
                writeRowToCSV(resultRow, resultFile);

                size_t l = j + 1;
                // cout << "l < table2.size() is " << (l < table2.size()) << endl;
                // cout << "trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2]) is " << (trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2])) << endl;
                while (l < table2.size() && stoi(table1[i][joinColumnIndex1]) < stoi(table2[l][joinColumnIndex2]))
                {
                    // cout << "i is " << i << " and l is " << l << endl;
                    // cout << "table1[i][joinColumnIndex1] is " << table1[i][joinColumnIndex1] << " and table2[l][joinColumnIndex2] is " << table2[l][joinColumnIndex2] << endl;
                    resultRow.clear();
                    for (size_t col = 0; col < table1[i].size(); ++col)
                    {
                        resultRow.push_back(table1[i][col]);
                    }
                    for (size_t col = 0; col < table2[l].size(); ++col)
                    {
                        resultRow.push_back(table2[l][col]);
                    }
                    writeRowToCSV(resultRow, resultFile);
                    // cout << "l < table2.size() is " << (l < table2.size()) << endl;
                    // cout << "trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2]) is " << (trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2])) << endl;
                    ++l;
                }
                ++i;
            }
            else if (stoi(table1[i][joinColumnIndex1]) > stoi(table2[j][joinColumnIndex2]))
            {
                ++j;
            }
            else
            {
                ++j;
            }
        }
    }
    else if(parsedQuery.joinBinaryOperator == LEQ){
        while (i < table1.size() && j < table2.size())
        {
            // Print the values of the join columns for debugging
            // cout << "Table 1 value: " << table1[i][joinColumnIndex1] << endl;
            // cout << "Table 2 value: " << table2[j][joinColumnIndex2] << endl;

            if (stoi(table1[i][joinColumnIndex1]) < stoi(table2[j][joinColumnIndex2]))
            {
                vector<string> resultRow;

                // Output the combined tuple <R(i), S(j)> to result CSV file
                for (size_t col = 0; col < table1[i].size(); ++col)
                {
                    resultRow.push_back(table1[i][col]);
                }
                for (size_t col = 0; col < table2[j].size(); ++col)
                {
                    resultRow.push_back(table2[j][col]);
                }
                writeRowToCSV(resultRow, resultFile);

                size_t l = j + 1;
                // cout << "l < table2.size() is " << (l < table2.size()) << endl;
                // cout << "trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2]) is " << (trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2])) << endl;
                while (l < table2.size() && stoi(table1[i][joinColumnIndex1]) < stoi(table2[l][joinColumnIndex2]))
                {
                    // cout << "i is " << i << " and l is " << l << endl;
                    // cout << "table1[i][joinColumnIndex1] is " << table1[i][joinColumnIndex1] << " and table2[l][joinColumnIndex2] is " << table2[l][joinColumnIndex2] << endl;
                    resultRow.clear();
                    for (size_t col = 0; col < table1[i].size(); ++col)
                    {
                        resultRow.push_back(table1[i][col]);
                    }
                    for (size_t col = 0; col < table2[l].size(); ++col)
                    {
                        resultRow.push_back(table2[l][col]);
                    }
                    writeRowToCSV(resultRow, resultFile);
                    // cout << "l < table2.size() is " << (l < table2.size()) << endl;
                    // cout << "trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2]) is " << (trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2])) << endl;
                    ++l;
                }
                ++i;
            }
            else if (stoi(table1[i][joinColumnIndex1]) > stoi(table2[j][joinColumnIndex2]))
            {
                ++j;
            }
            else
            {
                // Rows match, perform join
                vector<string> resultRow;

                // Output the combined tuple <R(i), S(j)> to result CSV file
                for (size_t col = 0; col < table1[i].size(); ++col)
                {
                    resultRow.push_back(table1[i][col]);
                }
                for (size_t col = 0; col < table2[j].size(); ++col)
                {
                    resultRow.push_back(table2[j][col]);
                }
                writeRowToCSV(resultRow, resultFile);

                // Output other tuples that match R(i), if any
                size_t l = j + 1;
                while (l < table2.size())
                {
                    resultRow.clear();
                    for (size_t col = 0; col < table1[i].size(); ++col)
                    {
                        resultRow.push_back(table1[i][col]);
                    }
                    for (size_t col = 0; col < table2[l].size(); ++col)
                    {
                        resultRow.push_back(table2[l][col]);
                    }
                    writeRowToCSV(resultRow, resultFile);
                    ++l;
                }

                ++i;
            }
        }
    }
    else if(parsedQuery.joinBinaryOperator == GREATER_THAN){
        while (i < table1.size() && j < table2.size())
        {
            // Print the values of the join columns for debugging
            // cout << "Table 1 value: " << table1[i][joinColumnIndex1] << endl;
            // cout << "Table 2 value: " << table2[j][joinColumnIndex2] << endl;

            if (stoi(table1[i][joinColumnIndex1]) < stoi(table2[j][joinColumnIndex2]))
            {
                ++i;
            }
            else if (stoi(table1[i][joinColumnIndex1]) > stoi(table2[j][joinColumnIndex2]))
            {
                vector<string> resultRow;

                // Output the combined tuple <R(i), S(j)> to result CSV file
                for (size_t col = 0; col < table1[i].size(); ++col)
                {
                    resultRow.push_back(table1[i][col]);
                }
                for (size_t col = 0; col < table2[j].size(); ++col)
                {
                    resultRow.push_back(table2[j][col]);
                }
                writeRowToCSV(resultRow, resultFile);

                size_t k = i + 1;
                // cout << "l < table2.size() is " << (l < table2.size()) << endl;
                // cout << "trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2]) is " << (trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2])) << endl;
                while (k < table1.size() && stoi(table1[k][joinColumnIndex1]) > stoi(table2[j][joinColumnIndex2]))
                {
                    // cout << "i is " << i << " and l is " << l << endl;
                    // cout << "table1[i][joinColumnIndex1] is " << table1[i][joinColumnIndex1] << " and table2[l][joinColumnIndex2] is " << table2[l][joinColumnIndex2] << endl;
                    resultRow.clear();
                    for (size_t col = 0; col < table1[k].size(); ++col)
                    {
                        resultRow.push_back(table1[k][col]);
                    }
                    for (size_t col = 0; col < table2[j].size(); ++col)
                    {
                        resultRow.push_back(table2[j][col]);
                    }
                    writeRowToCSV(resultRow, resultFile);
                    // cout << "l < table2.size() is " << (l < table2.size()) << endl;
                    // cout << "trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2]) is " << (trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2])) << endl;
                    ++k;
                }
                ++j;
            }
            else
            {
                ++i;
            }
        }
    }
    else if(parsedQuery.joinBinaryOperator == GEQ){
        while (i < table1.size() && j < table2.size())
        {
            // Print the values of the join columns for debugging
            // cout << "Table 1 value: " << table1[i][joinColumnIndex1] << endl;
            // cout << "Table 2 value: " << table2[j][joinColumnIndex2] << endl;

            if (stoi(table1[i][joinColumnIndex1]) < stoi(table2[j][joinColumnIndex2]))
            {
                ++i;
            }
            else if (stoi(table1[i][joinColumnIndex1]) > stoi(table2[j][joinColumnIndex2]))
            {
                vector<string> resultRow;

                // Output the combined tuple <R(i), S(j)> to result CSV file
                for (size_t col = 0; col < table1[i].size(); ++col)
                {
                    resultRow.push_back(table1[i][col]);
                }
                for (size_t col = 0; col < table2[j].size(); ++col)
                {
                    resultRow.push_back(table2[j][col]);
                }
                writeRowToCSV(resultRow, resultFile);

                size_t k = i + 1;
                // cout << "l < table2.size() is " << (l < table2.size()) << endl;
                // cout << "trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2]) is " << (trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2])) << endl;
                while (k < table1.size() && stoi(table1[k][joinColumnIndex1]) > stoi(table2[j][joinColumnIndex2]))
                {
                    // cout << "i is " << i << " and l is " << l << endl;
                    // cout << "table1[i][joinColumnIndex1] is " << table1[i][joinColumnIndex1] << " and table2[l][joinColumnIndex2] is " << table2[l][joinColumnIndex2] << endl;
                    resultRow.clear();
                    for (size_t col = 0; col < table1[k].size(); ++col)
                    {
                        resultRow.push_back(table1[k][col]);
                    }
                    for (size_t col = 0; col < table2[j].size(); ++col)
                    {
                        resultRow.push_back(table2[j][col]);
                    }
                    writeRowToCSV(resultRow, resultFile);
                    // cout << "l < table2.size() is " << (l < table2.size()) << endl;
                    // cout << "trim(table1[i][joinColumnIndex1]) < trim(table2[l][joinColumnIndex2]) is " << (trim(table1[i][joinColumnIndex1]) < trim(table2[l][join
                    ++k;
                }
                ++j;
            }
            else
            {
                // Rows match, perform join
                vector<string> resultRow;

                // Output the combined tuple <R(i), S(j)> to result CSV file
                for (size_t col = 0; col < table1[i].size(); ++col)
                {
                    resultRow.push_back(table1[i][col]);
                }
                for (size_t col = 0; col < table2[j].size(); ++col)
                {
                    resultRow.push_back(table2[j][col]);
                }
                writeRowToCSV(resultRow, resultFile);

                // Output other tuples that match R(i), if any
                size_t k = i + 1;
                while (k < table1.size())
                {
                    resultRow.clear();
                    for (size_t col = 0; col < table1[k].size(); ++col)
                    {
                        resultRow.push_back(table1[k][col]);
                    }
                    for (size_t col = 0; col < table2[j].size(); ++col)
                    {
                        resultRow.push_back(table2[j][col]);
                    }
                    writeRowToCSV(resultRow, resultFile);
                    ++k;
                }
                ++j;
            }
        }
    }


    // Close the files
    file1.close();
    file2.close();
    resultFile.close();

    // Load the result CSV file into the database
    Table *resultTable = new Table(parsedQuery.joinResultRelationName);

    if (resultTable->load())
    {
        tableCatalogue.insertTable(resultTable);
        cout << "Loaded Result Table. Column Count: " << resultTable->columnCount << " Row Count: " << resultTable->rowCount << endl;
    }

    // Delete the result CSV file
    if (remove(resultFilePath.c_str()) != 0)
    {
        cerr << "Error deleting file: " << resultFilePath << endl;
    }  

    // Clear the loaded sorted tables
    parsedQuery.clearRelationName = parsedQuery.joinFirstRelationName + "_sorted";
    executeCLEAR();
    parsedQuery.clearRelationName = parsedQuery.joinSecondRelationName + "_sorted";
    executeCLEAR();

    // Delete the sorted CSV files
    if (remove(file1Name.c_str()) != 0)
    {
        cerr << "Error deleting file: " << file1Name << endl;
    }
    if (remove(file2Name.c_str()) != 0)
    {
        cerr << "Error deleting file: " << file2Name << endl;
    }

    return; 
}
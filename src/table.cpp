#include "global.h"

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table()
{
    logger.log("Table::Table");
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName 
 */
Table::Table(string tableName)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
    this->subMatrixsize = floor(sqrt((BLOCK_SIZE*1000)/(sizeof(int))));
    // this->blocksPerRow = ceil((float)this->columnCount/(float)this->subMatrixsize);
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName 
 * @param columns 
 */
Table::Table(string tableName, vector<string> columns)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/temp/" + tableName + ".csv";
    this->tableName = tableName;
    this->columns = columns;
    this->columnCount = columns.size();
    this->subMatrixsize = floor(sqrt((BLOCK_SIZE*1000)/(sizeof(int))));
    this->blocksPerRow = ceil((float)this->columnCount/(float)this->subMatrixsize);
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    this->writeRow<string>(columns);
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded 
 * @return false if an error occurred 
 */
bool Table::load()
{
    logger.log("Table::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (this->extractColumnNames(line))
            if (this->blockify())
                return true;
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file. 
 *
 * @param line 
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine)
{
    logger.log("Table::extractColumnNames");
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    while (getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        if (!(this->isMatrix) && (columnNames.count(word)))
            return false;
        if (this->isMatrix)
        {
            // if there is any non-digit in the first row, not a matrix.
            if (word.find_first_not_of("0123456789") != string::npos)
            {
                cout << "SEMANTIC ERROR: Not a valid Matrix. Should only contain numbers." << endl;
                return false;
            }
        }
        columnNames.insert(word);
        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    if (this->isMatrix)
    {
        this->rowCount = this->columnCount;
        this->blocksPerRow = ceil((float)this->columnCount/(float)this->subMatrixsize);
        this->maxRowsPerBlock = this->subMatrixsize;
    }
   
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
    logger.log("Table::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    int pageCounter = 0;
    unordered_set<int> dummy;
    dummy.clear();
    this->distinctValuesInColumns.assign(this->columnCount, dummy);
    this->distinctValuesPerColumnCount.assign(this->columnCount, 0);

    int blocksRead = 0, blocksWrite = 0;

    if (this->isMatrix)
    {
        int m = this->subMatrixsize;
        vector<string> mRows;

        for (int startRow = 0; startRow < this->rowCount; startRow+=m)
        {
            // obtain first mRows
            int endRow = min(startRow+m, int(this->rowCount));

            for (int i=startRow; i<endRow; i++)
            {
                getline(fin, line);
                mRows.push_back(line);
            }

            for (int startCol = 0; startCol<this->columnCount; startCol+=m)
            {
                vector<vector<int>> newRowsInPage;
                for (int i=0; i<mRows.size(); i++)
                {
                    stringstream s(mRows[i]);

                    // skip first startCol columns
                    for (int j=0; j<startCol; j++)
                    {
                        if (!getline(s, word, ','))
                            return false;
                        // cout<<"Skipped: "<<word<<"\n";
                    }

                    int endCol = min(startCol+m, int(this->columnCount));
                    
                    vector<int> newRow;
                    for (int j=startCol; j<endCol; j++)
                    {
                        if (!getline(s, word, ','))
                            return false;
                        newRow.push_back(stoi(word));
                        // rowsInPage[i][j] = stoi(word);
                    }
                    newRowsInPage.push_back(newRow);
                }

                bufferManager.writePage(this->tableName, this->blockCount, newRowsInPage, newRowsInPage.size());
                this->blockCount++;
                blocksWrite++;
                this->rowsPerBlockCount.emplace_back(newRowsInPage.size());    
            }

            mRows.clear();
        }
        cout<<"Number of Blocks Read: "<<blocksRead<<"\nNumber of Blocks Written: "<<blocksWrite<<"\nNumber of Blocks Accessed: "<<blocksRead+blocksWrite<<"\n";
    }
    else
    {
        vector<int> row(this->columnCount, 0);
        vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);

        // to skip header
        getline(fin, line);

        while (getline(fin, line))
        {
            stringstream s(line);
            for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
            {
                if (!getline(s, word, ','))
                    return false;
                row[columnCounter] = stoi(word);
                rowsInPage[pageCounter][columnCounter] = row[columnCounter];
            }
            pageCounter++;
            this->updateStatistics(row);
            if (pageCounter == this->maxRowsPerBlock)
            {
                bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
                this->blockCount++;
                blocksWrite++;
                this->rowsPerBlockCount.emplace_back(pageCounter);
                pageCounter = 0;
            }
        }
        if (pageCounter)
        {
            bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
            this->blockCount++;
            blocksWrite++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }

        if (this->rowCount == 0)
            return false;
        this->distinctValuesInColumns.clear();
        cout<<"Number of Blocks Read: "<<blocksRead<<"\nNumber of Blocks Written: "<<blocksWrite<<"\nNumber of Blocks Accessed: "<<blocksRead+blocksWrite<<"\n";
        return true;
    }
        
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row 
 */
void Table::updateStatistics(vector<int> row)
{
    this->rowCount++;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (!this->distinctValuesInColumns[columnCounter].count(row[columnCounter]))
        {
            this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
            this->distinctValuesPerColumnCount[columnCounter]++;
        }
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName 
 * @return true 
 * @return false 
 */
bool Table::isColumn(string columnName)
{
    if(this->isMatrix)
        return false;

    logger.log("Table::isColumn");
    for (auto col : this->columns)
    {
        if (col == columnName)
        {
            return true;
        }
    }
    return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName 
 * @param toColumnName 
 */
void Table::renameColumn(string fromColumnName, string toColumnName)
{
    if(this->isMatrix)
        return;
    
    logger.log("Table::renameColumn");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (columns[columnCounter] == fromColumnName)
        {
            columns[columnCounter] = toColumnName;
            break;
        }
    }
    return;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Table::print()
{
    logger.log("Table::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);
    cout << "Row count for table " << this->tableName << " : " << this->rowCount << "\n";
    // int counter = 0;

    if(this->isMatrix){
        logger.log("Table::printMatrix");
        int k = this->blocksPerRow;
        int m = this->subMatrixsize;
        int blocksRead = 0, blocksWrite = 0;

        for (int i=0; i<k; i++)
        {
            vector<vector<vector<int>>> allPagesInSameRow;
            for (int j=0; j<k; j++)
                {
                    // get page i*k+j

                    // matrix 1

                    int ogColCount = this->columnCount;
                    int ogRowCount = this->rowCount;
                    uint ogMaxRowsPerBlock = this->maxRowsPerBlock;

                    this->rowCount = (i < k-1) ? m : (this->rowCount - i*m);
                    this->columnCount = (j < k-1) ? m : (this->columnCount - j*m);
                    this->maxRowsPerBlock = this->rowCount;

                    Page matrix1Page = bufferManager.getPage(this->tableName, i*k+j);
                    blocksRead++;

                    vector<vector<int>> matrixOne;
                    for (int i=0; i<this->rowCount; i++)
                    {
                        matrixOne.push_back(matrix1Page.getRow(i));
                    }

                    this->rowCount = ogRowCount;
                    this->columnCount = ogColCount;
                    this->maxRowsPerBlock = ogMaxRowsPerBlock;

                    allPagesInSameRow.push_back(matrixOne);
                }
            
            // Determine the number of rows (fixed) and initialize the combinedRows matrix
            int numRows = allPagesInSameRow[0].size();
            vector<vector<int>> combinedRows(numRows);

            // Iterate over rows of the matrices
            for (int row = 0; row < numRows; ++row) {
                vector<int> combinedRow;
                
                // Iterate over matrices in allPagesInSameRow
                for (int mat = 0; mat < allPagesInSameRow.size(); ++mat) {
                    // Check if the current row has enough columns for this matrix
                    if (row < allPagesInSameRow[mat].size()) {
                        // Append values from the current row of the matrix to combinedRow
                        combinedRow.insert(combinedRow.end(), allPagesInSameRow[mat][row].begin(), allPagesInSameRow[mat][row].end());
                    }
                }

                // Add the combined row to combinedRows
                combinedRows[row] = combinedRow;
            }

            // Write the combinedRows matrix to the file
            for (int row = 0; row < numRows; ++row) {
                int colCount = min((long long)PRINT_COUNT, this->rowCount);
                for (int col = 0; col < combinedRows[row].size(); ++col) {
                    if(col == combinedRows[row].size() - 1) {
                        cout << combinedRows[row][col];
                        continue;
                    }
                    else{
                        cout << combinedRows[row][col] << ",";
                    }
                    colCount--;
                    if (colCount == 0){
                        // cout << "\n";
                        break;
                    }
                }
                cout << "\n";
                count--;
                if (count == 0){
                    cout << "Number of Blocks Read: " << blocksRead << "\nNumber of Blocks Written: " << blocksWrite << "\nNumber of Blocks Accessed: " << blocksRead + blocksWrite << "\n";
                    return;
                }
            }
        }

        cout << "Number of Blocks Read: " << blocksRead << "\nNumber of Blocks Written: " << blocksWrite << "\nNumber of Blocks Accessed: " << blocksRead + blocksWrite << "\n";
        return;
    }

    //print headings
    if(!this->isMatrix)
        this->writeRow(this->columns, cout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, cout);
    }
    printRowCount(this->rowCount);
}

/**
 * @brief Function that Transposes a given Matrix.
 *
 * @param tableName
 */

void Table::matrixTranspose()
{

    int blocksRead = 0, blocksWrite = 0;

    logger.log("Table::matrixTranspose");
    int k = this->blocksPerRow;
    int m = this->subMatrixsize;

    for (int i=0; i<k; i++)
    {
        for (int j=i; j<k; j++)
        {
            // get page i*k+j

            // matrix 1

            int ogColCount = this->columnCount;
            int ogRowCount = this->rowCount;
            uint ogMaxRowsPerBlock = this->maxRowsPerBlock;

            this->rowCount = (i < k-1) ? m : (this->rowCount - i*m);
            this->columnCount = (j < k-1) ? m : (this->columnCount - j*m);
            this->maxRowsPerBlock = this->rowCount;

            Page matrix1Page = bufferManager.getPage(this->tableName, i*k+j);
            blocksRead++;

            vector<vector<int>> matrixOne;
            for (int i=0; i<this->rowCount; i++)
            {
                matrixOne.push_back(matrix1Page.getRow(i));
            }

            this->rowCount = ogRowCount;
            this->columnCount = ogColCount;
            this->maxRowsPerBlock = ogMaxRowsPerBlock;

            // Transpose MatrixOne

            vector<vector<int>> MatrixOneTrans(matrixOne[0].size(), vector<int>(matrixOne.size(), 0));

            for (int r=0; r<matrixOne.size(); r++)
            {
                for (int c=0; c<matrixOne[0].size(); c++)
                    MatrixOneTrans[c][r] = matrixOne[r][c];
            }

            if (i == j)
            {
                // then we just write back.
                bufferManager.writePage(this->tableName, i*k+j, MatrixOneTrans, MatrixOneTrans.size());
                blocksWrite++;
             
                // Page MatrixOneWrite(this->tableName, i*k+j, MatrixOneTrans, MatrixOneTrans.size());
                // MatrixOneWrite.writePage();
                continue;
            }

            // matrix 2

            ogColCount = this->columnCount;
            ogRowCount = this->rowCount;
            ogMaxRowsPerBlock = this->maxRowsPerBlock;

            this->rowCount = (j < k-1) ? m : (this->rowCount - j*m);
            this->columnCount = (i < k-1) ? m : (this->columnCount - i*m);
            this->maxRowsPerBlock = this->rowCount;

            Page matrix2Page = bufferManager.getPage(this->tableName, j*k+i);
            blocksRead++;

            vector<vector<int>> matrixTwo;
            for (int i=0; i<this->rowCount; i++)
            {
                matrixTwo.push_back(matrix2Page.getRow(i));
            }

            this->rowCount = ogRowCount;
            this->columnCount = ogColCount;
            this->maxRowsPerBlock = ogMaxRowsPerBlock;

            // Transpose MatrixTwo

            vector<vector<int>> MatrixTwoTrans(matrixTwo[0].size(), vector<int>(matrixTwo.size(), 0));

            for (int r=0; r<matrixTwo.size(); r++)
            {
                for (int c=0; c<matrixTwo[0].size(); c++)
                    MatrixTwoTrans[c][r] = matrixTwo[r][c];
            }

            // Page MatrixOneWrite(this->tableName, i*k+j, MatrixTwoTrans, MatrixTwoTrans.size());
            // MatrixOneWrite.writePage();
            bufferManager.writePage(this->tableName, i*k+j, MatrixTwoTrans, MatrixTwoTrans.size());
            blocksWrite++;

            // Page MatrixTwoWrite(this->tableName, j*k+i, MatrixOneTrans, MatrixOneTrans.size());
            // MatrixTwoWrite.writePage();
            bufferManager.writePage(this->tableName, j*k+i, MatrixOneTrans, MatrixOneTrans.size());
            blocksWrite++;
        }
    }

    cout<<"Number of Blocks Read: "<<blocksRead<<"\nNumber of Blocks Written: "<<blocksWrite<<"\nNumber of Blocks Accessed: "<<blocksRead+blocksWrite<<"\n";
}

bool Table::isSymmetric()
{

    int blocksRead = 0, blocksWrite = 0;

    logger.log("Table::isSymmetric");
    int k = this->blocksPerRow;
    int m = this->subMatrixsize;

    for (int i=0; i<k; i++)
    {
        for (int j=i; j<k; j++)
        {
            if(i==j)
            {
                // get page i*k+j

                // matrix 1

                int ogColCount = this->columnCount;
                int ogRowCount = this->rowCount;
                uint ogMaxRowsPerBlock = this->maxRowsPerBlock;

                this->rowCount = (i < k-1) ? m : (this->rowCount - i*m);
                this->columnCount = (j < k-1) ? m : (this->columnCount - j*m);
                this->maxRowsPerBlock = this->rowCount;

                Page matrix1Page = bufferManager.getPage(this->tableName, i*k+j);
                blocksRead++;

                vector<vector<int>> matrixOne;
                for (int i=0; i<this->rowCount; i++)
                {
                    matrixOne.push_back(matrix1Page.getRow(i));
                }

                this->rowCount = ogRowCount;
                this->columnCount = ogColCount;
                this->maxRowsPerBlock = ogMaxRowsPerBlock;

                // Transpose MatrixOne

                vector<vector<int>> MatrixOneTrans(matrixOne[0].size(), vector<int>(matrixOne.size(), 0));

                for (int r=0; r<matrixOne.size(); r++)
                {
                    for (int c=0; c<matrixOne[0].size(); c++)
                        MatrixOneTrans[c][r] = matrixOne[r][c];
                }

                for(int i=0;i<MatrixOneTrans.size();i++)
                {
                    for(int j=0;j<MatrixOneTrans[0].size();j++)
                    {
                        if(MatrixOneTrans[i][j]!=matrixOne[i][j])
                        {
                            cout<<"Number of Blocks Read: "<<blocksRead<<"\nNumber of Blocks Written: "<<blocksWrite<<"\nNumber of Blocks Accessed: "<<blocksRead+blocksWrite<<"\n";
                            return false;
                        }
                    }
                }

                if(i==k-1)
                {
                    if(j==k-1)
                    {
                        cout<<"Number of Blocks Read: "<<blocksRead<<"\nNumber of Blocks Written: "<<blocksWrite<<"\nNumber of Blocks Accessed: "<<blocksRead+blocksWrite<<"\n";
                        return true;
                    }
                }

                continue;

            }
            // get page i*k+j

            // matrix 1

            int ogColCount = this->columnCount;
            int ogRowCount = this->rowCount;
            uint ogMaxRowsPerBlock = this->maxRowsPerBlock;

            this->rowCount = (i < k-1) ? m : (this->rowCount - i*m);
            this->columnCount = (j < k-1) ? m : (this->columnCount - j*m);
            this->maxRowsPerBlock = this->rowCount;

            Page matrix1Page = bufferManager.getPage(this->tableName, i*k+j);
            blocksRead++;

            vector<vector<int>> matrixOne;
            for (int i=0; i<this->rowCount; i++)
            {
                matrixOne.push_back(matrix1Page.getRow(i));
            }

            this->rowCount = ogRowCount;
            this->columnCount = ogColCount;
            this->maxRowsPerBlock = ogMaxRowsPerBlock;

            // Transpose MatrixOne

            vector<vector<int>> MatrixOneTrans(matrixOne[0].size(), vector<int>(matrixOne.size(), 0));

            for (int r=0; r<matrixOne.size(); r++)
            {
                for (int c=0; c<matrixOne[0].size(); c++)
                    MatrixOneTrans[c][r] = matrixOne[r][c];
            }

            // matrix 2

            ogColCount = this->columnCount;
            ogRowCount = this->rowCount;
            ogMaxRowsPerBlock = this->maxRowsPerBlock;

            this->rowCount = (j < k-1) ? m : (this->rowCount - j*m);
            this->columnCount = (i < k-1) ? m : (this->columnCount - i*m);
            this->maxRowsPerBlock = this->rowCount;

            Page matrix2Page = bufferManager.getPage(this->tableName, j*k+i);
            blocksRead++;

            vector<vector<int>> matrixTwo;
            for (int i=0; i<this->rowCount; i++)
            {
                matrixTwo.push_back(matrix2Page.getRow(i));
            }

            this->rowCount = ogRowCount;
            this->columnCount = ogColCount;
            this->maxRowsPerBlock = ogMaxRowsPerBlock;

            // check if matrixOneTrans == matrixTwo

            for(int i=0;i<MatrixOneTrans.size();i++)
            {
                for(int j=0;j<MatrixOneTrans[0].size();j++)
                {
                    if(MatrixOneTrans[i][j]!=matrixTwo[i][j])
                    {
                        cout<<"Number of Blocks Read: "<<blocksRead<<"\nNumber of Blocks Written: "<<blocksWrite<<"\nNumber of Blocks Accessed: "<<blocksRead+blocksWrite<<"\n";
                        return false;
                    }    
                }
            }
        }
    }
}

void Table::compute(Table *resultTable)
{

    int blocksRead = 0, blocksWrite = 0;

    logger.log("Table::compute");
    resultTable->isMatrix = true;
    resultTable->rowCount = this->rowCount;
    resultTable->columnCount = this->columnCount;
    resultTable->subMatrixsize = this->subMatrixsize;
    resultTable->blocksPerRow = this->blocksPerRow;
    resultTable->maxRowsPerBlock = this->maxRowsPerBlock;
    resultTable->blockCount = this->blockCount;
    resultTable->rowsPerBlockCount = this->rowsPerBlockCount;

    tableCatalogue.insertTable(resultTable);

    logger.log("Table::matrixTranspose");
    int k = this->blocksPerRow;
    int m = this->subMatrixsize;

    for (int i=0; i<k; i++)
    {
        for (int j=i; j<k; j++)
        {

            if(i==j)
            {
                // get page i*k+j

                // matrix 1

                int ogColCount = this->columnCount;
                int ogRowCount = this->rowCount;
                uint ogMaxRowsPerBlock = this->maxRowsPerBlock;

                this->rowCount = (i < k-1) ? m : (this->rowCount - i*m);
                this->columnCount = (j < k-1) ? m : (this->columnCount - j*m);
                this->maxRowsPerBlock = this->rowCount;

                Page matrix1Page = bufferManager.getPage(this->tableName, i*k+j);
                blocksRead++;

                vector<vector<int>> matrixOne;
                for (int i=0; i<this->rowCount; i++)
                {
                    matrixOne.push_back(matrix1Page.getRow(i));
                }

                this->rowCount = ogRowCount;
                this->columnCount = ogColCount;
                this->maxRowsPerBlock = ogMaxRowsPerBlock;

                // Transpose MatrixOne

                vector<vector<int>> MatrixOneTrans(matrixOne[0].size(), vector<int>(matrixOne.size(), 0));

                for (int r=0; r<matrixOne.size(); r++)
                {
                    for (int c=0; c<matrixOne[0].size(); c++)
                        MatrixOneTrans[c][r] = matrixOne[r][c];
                }


                vector<vector<int>> resultMatrixOne(matrixOne.size(), vector<int>(matrixOne[0].size(), 0));

                for(int i=0;i<matrixOne.size();i++)
                {
                    for(int j=0;j<matrixOne[0].size();j++)
                    {
                        resultMatrixOne[i][j] = matrixOne[i][j] - MatrixOneTrans[i][j];
                    }
                }

                bufferManager.writePage(resultTable->tableName, i*k+j, resultMatrixOne, resultMatrixOne.size());
                blocksWrite++;

            }
            else
            {
                // get page i*k+j

                // matrix 1

                int ogColCount = this->columnCount;
                int ogRowCount = this->rowCount;
                uint ogMaxRowsPerBlock = this->maxRowsPerBlock;

                this->rowCount = (i < k-1) ? m : (this->rowCount - i*m);
                this->columnCount = (j < k-1) ? m : (this->columnCount - j*m);
                this->maxRowsPerBlock = this->rowCount;

                Page matrix1Page = bufferManager.getPage(this->tableName, i*k+j);
                blocksRead++;

                vector<vector<int>> matrixOne;
                for (int i=0; i<this->rowCount; i++)
                {
                    matrixOne.push_back(matrix1Page.getRow(i));
                }

                this->rowCount = ogRowCount;
                this->columnCount = ogColCount;
                this->maxRowsPerBlock = ogMaxRowsPerBlock;

                // Transpose MatrixOne

                vector<vector<int>> MatrixOneTrans(matrixOne[0].size(), vector<int>(matrixOne.size(), 0));

                for (int r=0; r<matrixOne.size(); r++)
                {
                    for (int c=0; c<matrixOne[0].size(); c++)
                        MatrixOneTrans[c][r] = matrixOne[r][c];
                }

                // matrix 2

                ogColCount = this->columnCount;
                ogRowCount = this->rowCount;
                ogMaxRowsPerBlock = this->maxRowsPerBlock;

                this->rowCount = (j < k-1) ? m : (this->rowCount - j*m);
                this->columnCount = (i < k-1) ? m : (this->columnCount - i*m);
                this->maxRowsPerBlock = this->rowCount;

                Page matrix2Page = bufferManager.getPage(this->tableName, j*k+i);
                blocksRead++;

                vector<vector<int>> matrixTwo;
                for (int i=0; i<this->rowCount; i++)
                {
                    matrixTwo.push_back(matrix2Page.getRow(i));
                }

                this->rowCount = ogRowCount;
                this->columnCount = ogColCount;
                this->maxRowsPerBlock = ogMaxRowsPerBlock;

                // Transpose MatrixTwo

                vector<vector<int>> MatrixTwoTrans(matrixTwo[0].size(), vector<int>(matrixTwo.size(), 0));

                for (int r=0; r<matrixTwo.size(); r++)
                {
                    for (int c=0; c<matrixTwo[0].size(); c++)
                        MatrixTwoTrans[c][r] = matrixTwo[r][c];
                }

                // subtract matrixTwoTrans from matrixOne

                vector<vector<int>> resultMatrixOne(matrixOne.size(), vector<int>(matrixOne[0].size(), 0));

                for(int i=0;i<matrixOne.size();i++)
                {
                    for(int j=0;j<matrixOne[0].size();j++)
                    {
                        resultMatrixOne[i][j] = matrixOne[i][j] - MatrixTwoTrans[i][j];
                    }
                }

                // subtractmatrixOneTrans from matrixTwo

                vector<vector<int>> resultMatrixTwo(matrixTwo.size(), vector<int>(matrixTwo[0].size(), 0));

                for(int i=0;i<matrixTwo.size();i++)
                {
                    for(int j=0;j<matrixTwo[0].size();j++)
                    {
                        resultMatrixTwo[i][j] = matrixTwo[i][j] - MatrixOneTrans[i][j];
                    }
                }


                // write back to resultTable

                bufferManager.writePage(resultTable->tableName, i*k+j, resultMatrixOne, resultMatrixOne.size());
                blocksWrite++;
                bufferManager.writePage(resultTable->tableName, j*k+i, resultMatrixTwo, resultMatrixTwo.size());
                blocksWrite++;
            }
        }
    }

    cout<<"Number of Blocks Read: "<<blocksRead<<"\nNumber of Blocks Written: "<<blocksWrite<<"\nNumber of Blocks Accessed: "<<blocksRead+blocksWrite<<"\n";

}


/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor 
 * @return vector<int> 
 */
void Table::getNextPage(Cursor *cursor)
{
    logger.log("Table::getNext");

        if (cursor->pageIndex < this->blockCount - 1)
        {
            cursor->nextPage(cursor->pageIndex+1);
        }
}



/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent()
{
    logger.log("Table::makePermanent");
    if(!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    if(this->isMatrix){
        int k = this->blocksPerRow;
        int m = this->subMatrixsize;
        int blocksRead = 0, blocksWrite = 0;

        for (int i=0; i<k; i++)
        {
            vector<vector<vector<int>>> allPagesInSameRow;
            for (int j=0; j<k; j++)
                {
                    // get page i*k+j

                    // matrix 1

                    int ogColCount = this->columnCount;
                    int ogRowCount = this->rowCount;
                    uint ogMaxRowsPerBlock = this->maxRowsPerBlock;

                    this->rowCount = (i < k-1) ? m : (this->rowCount - i*m);
                    this->columnCount = (j < k-1) ? m : (this->columnCount - j*m);
                    this->maxRowsPerBlock = this->rowCount;

                    Page matrix1Page = bufferManager.getPage(this->tableName, i*k+j);
                    blocksRead++;

                    vector<vector<int>> matrixOne;
                    for (int i=0; i<this->rowCount; i++)
                    {
                        matrixOne.push_back(matrix1Page.getRow(i));
                    }

                    this->rowCount = ogRowCount;
                    this->columnCount = ogColCount;
                    this->maxRowsPerBlock = ogMaxRowsPerBlock;

                    allPagesInSameRow.push_back(matrixOne);
                }
            
            // Determine the number of rows (fixed) and initialize the combinedRows matrix
            int numRows = allPagesInSameRow[0].size();
            vector<vector<int>> combinedRows(numRows);

            // Iterate over rows of the matrices
            for (int row = 0; row < numRows; ++row) {
                vector<int> combinedRow;
                
                // Iterate over matrices in allPagesInSameRow
                for (int mat = 0; mat < allPagesInSameRow.size(); ++mat) {
                    // Check if the current row has enough columns for this matrix
                    if (row < allPagesInSameRow[mat].size()) {
                        // Append values from the current row of the matrix to combinedRow
                        combinedRow.insert(combinedRow.end(), allPagesInSameRow[mat][row].begin(), allPagesInSameRow[mat][row].end());
                    }
                }

                // Add the combined row to combinedRows
                combinedRows[row] = combinedRow;
            }

            // Write the combinedRows matrix to the file
            for (int row = 0; row < numRows; ++row) {
                for (int col = 0; col < combinedRows[row].size(); ++col) {
                    if(col == combinedRows[row].size() - 1) {
                        fout << combinedRows[row][col];
                        continue;
                    }
                    else{
                        fout << combinedRows[row][col] << ",";
                    }
                }
                fout << "\n";
            }
        }
        fout.close();

        cout << "Number of Blocks Read: " << blocksRead << "\nNumber of Blocks Written: " << blocksWrite << "\nNumber of Blocks Accessed: " << blocksRead + blocksWrite << "\n";
        return;
    }

    if(!(this->isMatrix)){
        //print headings in case it is not a matrix and just a table
        this->writeRow(this->columns, fout);
    }

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, fout);
    }
    fout.close();
}

/**
 * @brief Function to check if table is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent()
{
    logger.log("Table::isPermanent");
    if (this->sourceFileName == "../data/" + this->tableName + ".csv")
    return true;
    return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Table::unload(){
    logger.log("Table::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->tableName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this table
 * 
 * @return Cursor 
 */
Cursor Table::getCursor()
{
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 * 
 * @param columnName 
 * @return int 
 */
int Table::getColumnIndex(string columnName)
{
    logger.log("Table::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName)
            return columnCounter;
    }
}


bool sortComparator(const vector<int> &a, const vector<int> &b)
{
    for (int i=0; i<parsedQuery.sortColumnIndices.size(); i++)
    {
        if (parsedQuery.sortTypeList[i] == "ASC")
        {
            if (a[parsedQuery.sortColumnIndices[i]] < b[parsedQuery.sortColumnIndices[i]])
                return true;
            else if (a[parsedQuery.sortColumnIndices[i]] > b[parsedQuery.sortColumnIndices[i]])
                return false;
        }
        else if (parsedQuery.sortTypeList[i] == "DESC")
        {
            if (a[parsedQuery.sortColumnIndices[i]] < b[parsedQuery.sortColumnIndices[i]])
                return false;
            else if (a[parsedQuery.sortColumnIndices[i]] > b[parsedQuery.sortColumnIndices[i]])
                return true;
        }
    }
    return false;
}

// Note: The return value of this function is opposite to the one above
bool sortComparatorPQ(const pair<vector<int>, int> &p, const pair<vector<int>, int> &q)
{
    const vector<int> a = p.first;
    const vector<int> b = q.first;

    logger.log("Table::sortComparatorPQ");

    for (int i=0; i<parsedQuery.sortColumnIndices.size(); i++)
    {
        if (parsedQuery.sortTypeList[i] == "ASC")
        {
            if (a[parsedQuery.sortColumnIndices[i]] < b[parsedQuery.sortColumnIndices[i]])
            {
                logger.log("Table::sortComparatorPQ::ASC::"+to_string(a[parsedQuery.sortColumnIndices[i]])+"<"+to_string(b[parsedQuery.sortColumnIndices[i]]));
                return false;
            }
            else if (a[parsedQuery.sortColumnIndices[i]] > b[parsedQuery.sortColumnIndices[i]])
            {
                logger.log("Table::sortComparatorPQ::ASC::"+to_string(a[parsedQuery.sortColumnIndices[i]])+">"+to_string(b[parsedQuery.sortColumnIndices[i]]));
                return true;
            }
        }
        else if (parsedQuery.sortTypeList[i] == "DESC")
        {
            if (a[parsedQuery.sortColumnIndices[i]] < b[parsedQuery.sortColumnIndices[i]])
            {
                logger.log("Table::sortComparatorPQ::DESC::"+to_string(a[parsedQuery.sortColumnIndices[i]])+"<"+to_string(b[parsedQuery.sortColumnIndices[i]]));
                return true;
            }
            else if (a[parsedQuery.sortColumnIndices[i]] > b[parsedQuery.sortColumnIndices[i]])
            {
                logger.log("Table::sortComparatorPQ::DESC::"+to_string(a[parsedQuery.sortColumnIndices[i]])+">"+to_string(b[parsedQuery.sortColumnIndices[i]]));
                return false;
            }
        }
    }
    logger.log("Table::sortComparatorPQ::Equal");
    return false;
}

/**
 * @brief This function sorts a given relation based on the conditions given
 *
 */
void Table::tableSort()
{
    logger.log("Table::sort");

    // check how many pages does this relation occupy
    uint numBlocks = this->blockCount;


    // Steps:
    // 1. Do internal sort for all the pages.
    // Write out the sorted page.
    // 2. Take 9 pages at a time and merge


    // Internal Sorting of all pages
    for (int i=0; i<numBlocks; i++)
    {

        // GetPage
        Page tablePage = bufferManager.getPage(this->tableName, i);
        
        // load all the rows into a vector
        vector<vector<int>> pageRows;
        for (int i=0; i< this->rowCount; i++)
        {
            vector<int> currRow = tablePage.getRow(i);
            if (currRow.empty())
                break;
            pageRows.push_back(currRow);
        }

        // cout<<"OG Matrix\n";
        // for (int i=0; i<pageRows)

        sort(pageRows.begin(), pageRows.end(), sortComparator);

        // write back

        bufferManager.writePage(this->tableName, i, pageRows, pageRows.size());
        
    }

    logger.log("Table::sort::InternalSortDone");
    

    // /* DO NOT DISTURB */
    // // external sorting

    // uint start = 0, end = 0;

    // while(1)
    // {
    //     if (start < numBlocks)
    //     {
    //         end = min(start+8, numBlocks-1); 
    //     }
    //     else
    //     {
    //         break;
    //     }
        
    //     // The row, page Number 
    //     priority_queue<pair<vector<int>, int>, vector<pair<vector<int>, int>>, decltype(&sortComparatorPQ)> pq(sortComparatorPQ); 

    //     vector<int> rowNumber((end-start)+1,0);

    //     //  Read all pages from Start to end into the buffer
    //     for (int i=start; i<=end; i++)
    //     {
    //         Page tablePage = bufferManager.getPage(this->tableName, i);
    //         pq.push({tablePage.getRow(0), i});
    //     }

    //     // Page writePage = 
    //     vector<vector<int>> writePage;
    //     int writePageNum = numBlocks;

    //     while(1)
    //     {
    //         // if the writepage is filled, write it out and get a new write page
    //         if (writePage.size() == this->maxRowsPerBlock)
    //         {
    //             bufferManager.writePage(this->tableName, writePageNum, writePage, writePage.size());
    //             writePageNum++;
    //             writePage.clear();
    //         }

    //         // check if we have already written 9 (Block_Count - 1) pages (or finished reading all 9 pages)
    //         if (pq.empty())
    //         {
    //             // write the last page
    //             bufferManager.writePage(this->tableName, writePageNum, writePage, writePage.size());
    //             writePageNum++;
    //             writePage.clear();
    //             break;
    //         }

    //         // Get the top
    //         // alumnos &al = const_cast<alumnos &>(mypq.top());
    //         // pair<vector<int>, int> &myTop = (pair<vector<int>, int> &)pq.top();
    //         pair<vector<int>, int> myTop = pq.top();
    //         pq.pop();
    //         rowNumber[myTop.second]++;

    //         writePage.push_back(myTop.first);
            
    //         Page tablePage = bufferManager.getPage(this->tableName, myTop.second);

    //         if (rowNumber[myTop.second] >= this->rowsPerBlockCount[myTop.second])
    //         {
    //             rowNumber[myTop.second] = -1;
    //         }
    //         else 
    //         {
    //             pq.push({tablePage.getRow(rowNumber[myTop.second]), myTop.second});
    //         }

    //     }

    //     // TO DO:
    //     // Remove the original pages and replace it with the new pages
    //     // Original pages are from Start to end inclusive
    //     // New pages are from numBlocks to writePageNum-1 inclusive

    //     // Update the start and end and repeat
    //     start = end + 1;

    // /* DO NOT DISTURB */



    uint poolSize = BLOCK_COUNT - 1;
    uint factor = 1;

    uint order_level = ceil(log(numBlocks)/log(poolSize));

    uint iterationSet = 1;

    logger.log("Table::sort::ValuesSet");

    while(iterationSet<=order_level)
    {
        uint start = 0, end = 0;

        logger.log("Table::sort::Orderloop"+to_string(iterationSet));

        while(1)
        {

            logger.log("Table::sort::Orderloop::Start = "+to_string(start));
            if (start < numBlocks)
            {
                end = min(start+(uint)pow(poolSize,iterationSet)-1, numBlocks-1); 
            }
            else
            {
                break;
            }
            
            // The row, page Number 
            priority_queue<pair<vector<int>, int>, vector<pair<vector<int>, int>>, decltype(&sortComparatorPQ)> pq(sortComparatorPQ); 

            vector<int> rowNumber(poolSize,0);

            //  Read all pages from Start to end into the buffer
            for (int i=start; i<=end; i=i+factor)
            {
                Page tablePage = bufferManager.getPage(this->tableName, i);
                pq.push({tablePage.getRow(0), i});
            }
            logger.log("Table::sort::PQRead Size = "+to_string(pq.size()));

            // Page writePage = 
            vector<vector<int>> writePage;
            int writePageNum = numBlocks;

            while(1)
            {
                // if the writepage is filled, write it out and get a new write page
                if (writePage.size() == this->maxRowsPerBlock)
                {
                    logger.log("Table::sort::WritePage::PNo = "+to_string(writePageNum));
                    bufferManager.writePage(this->tableName, writePageNum, writePage, writePage.size());
                    this->blockCount++;
                    if (this->rowsPerBlockCount.size() > writePageNum)
                        this->rowsPerBlockCount[writePageNum] = writePage.size();
                    else
                        this->rowsPerBlockCount.emplace_back(writePage.size());

                    // bufferManager.writePage(this->tableName, this->blockCount, newRowsInPage, newRowsInPage.size());
                    // this->blockCount++;
                    // blocksWrite++;
                    // this->rowsPerBlockCount.emplace_back(newRowsInPage.size());
                
                    writePageNum++;
                    writePage.clear();
                }

                // check if we have already written 9 (Block_Count - 1) pages (or finished reading all 9 pages)
                if (pq.empty())
                {
                    logger.log("Table::sort::PQempty");
                    // write the last page
                    if (writePage.size() != 0)
                    {    
                        bufferManager.writePage(this->tableName, writePageNum, writePage, writePage.size());
                        this->blockCount++;
                        if (this->rowsPerBlockCount.size() > writePageNum)
                            this->rowsPerBlockCount[writePageNum] = writePage.size();
                        else
                            this->rowsPerBlockCount.emplace_back(writePage.size());
                        
                        writePageNum++;
                        writePage.clear();
                    }
                    break;
                }

                // Get the top
                // alumnos &al = const_cast<alumnos &>(mypq.top());
                // pair<vector<int>, int> &myTop = (pair<vector<int>, int> &)pq.top();
                pair<vector<int>, int> myTop = pq.top();
                pq.pop();

                // change made:
                // page numbers are no more from 0 to 9. they are scaled by the factor.
                rowNumber[(myTop.second - start)/factor]++;

                logger.log("Table::sort::Toppage"+to_string(myTop.second));

                writePage.push_back(myTop.first);
                
                Page tablePage = bufferManager.getPage(this->tableName, myTop.second);

                // Change made:
                // So when we use Cursor to get next row using getNext(), if the page is already fully read, 
                // it will go to the next page. However, this doesnt happen with Page.getRow().
                // if the page is fully read, it returns a blank array...
                // so changing the page number also now...

                if (rowNumber[(myTop.second - start)/factor] >= this->rowsPerBlockCount[myTop.second])
                {
                    int nextPage = myTop.second + 1;
                    logger.log("Table::sort::Page "+to_string(myTop.second)+" finished");

                    if (nextPage > end)
                    {
                        logger.log("Table::sort Chunk "+to_string((myTop.second - start)/factor)+" done. not pushed");                        
                        rowNumber[(myTop.second - start)/factor] = -1;
                    }
                    else if ((nextPage-start)/factor == (myTop.second - start)/factor)
                    {
                    // this checks if the next page is still in our chunk of pages
                        rowNumber[(nextPage - start)/factor] = 0;
                        logger.log("Table::sort::NextPage "+to_string(nextPage)+" row 0");
                        Page tablePage = bufferManager.getPage(this->tableName, nextPage);
                        pq.push({tablePage.getRow(0), nextPage});
                    }
                    else
                    {
                        logger.log("Table::sort Chunk "+to_string((myTop.second - start)/factor)+" done. not pushed");
                        rowNumber[(myTop.second - start)/factor] = -1;
                    }
                }
                else 
                {
                    logger.log("Table::sort::Toppage"+to_string(myTop.second)+" row "+to_string(rowNumber[(myTop.second - start)/factor]));
                    vector<int> nectRowTemp = tablePage.getRow(rowNumber[(myTop.second - start)/factor]);
                    logger.log("Table::sort::Toppage"+to_string(myTop.second)+" row "+to_string(rowNumber[(myTop.second - start)/factor])+" value "+to_string(nectRowTemp[0]));
                    pq.push({nectRowTemp, myTop.second});
                }

            }

            for(int i=start;i<=end;i++)
            {
                bufferManager.deleteFile(this->tableName, i);
                
                // bufferManager.writePage(this->tableName, this->blockCount, newRowsInPage, newRowsInPage.size());
                // this->blockCount++;
                // blocksWrite++;
                this->rowsPerBlockCount[i] = 0;
            }

            logger.log("Table::sort::Deleted Pages from "+to_string(start)+" to "+to_string(end));

            logger.log("Table::sort::rowsPerBlockCount.size()  "+to_string(this->rowsPerBlockCount.size()));
            for(int i=numBlocks;i<writePageNum;i++)
            {
                vector<vector<int>> tempPage;
                Page sourcePage = bufferManager.getPage(this->tableName, i);
                logger.log("Table::sort::Read Page "+to_string(i));
                for (int j=0; j<this->rowsPerBlockCount[i]; j++)
                {
                    tempPage.push_back(sourcePage.getRow(j));
                }

                // overwriting
                bufferManager.writePage(this->tableName, (i-numBlocks)+start, tempPage, tempPage.size());
                
                // bufferManager.writePage(this->tableName, this->blockCount, newRowsInPage, newRowsInPage.size());
                // this->blockCount++;
                // blocksWrite++;
                // this->rowsPerBlockCount.emplace_back(newRowsInPage.size());
                this->rowsPerBlockCount[(i-numBlocks)+start] = tempPage.size();

                bufferManager.deleteFile(this->tableName, i);
                this->rowsPerBlockCount[i] = 0;
                this->blockCount--;

                logger.log("Table:sort::Renamed Page "+to_string(i)+" to "+to_string((i-numBlocks)+start));
            }
            for (int i=numBlocks; i<writePageNum; i++)
            {
                this->rowsPerBlockCount.pop_back();
            }



            // TO DO:
            // Remove the original pages and replace it with the new pages
            // Original pages are from Start to end inclusive
            // New pages are from numBlocks to writePageNum-1 inclusive

            // Update the start and end and repeat
            start = end + 1;
        }

        factor = factor * poolSize;
        iterationSet++;
    }
}
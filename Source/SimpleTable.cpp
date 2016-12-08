#include "SimpleTable.h"
#include <inttypes.h>
#include <set>

Register<Table, SimpleTable, uint64_t&, uint64_t&, uint64_t&, const string&, bool&, uint64_t&> sTable("SimpleTable");

SimpleTable::SimpleTable(uint64_t tableID, uint64_t numRows, uint64_t numCols, const string& tableName, bool defaultStorageLayout, uint64_t partitionID) :
Table(tableID, numRows, numCols, tableName, defaultStorageLayout) {
    //Declaration of the 2D array
    tablePartitionID = new uint64_t*[numRows];
    for (int i = 0; i < numRows; i++) {
        tablePartitionID[i] = new uint64_t[numCols];
        //Set array to partitionID
        for (int j = 0; j < numCols; j++) {
            tablePartitionID[i][j] = partitionID;
        }
    }
};

uint64_t** SimpleTable::getPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) {

    uint64_t** partitionIDs;
    partitionIDs = new uint64_t*[height];
    for (int i = rowID; i < rowID + height; i++) {
        partitionIDs[i - rowID] = new uint64_t[width];
        for (int j = columnID; j < columnID + width; j++) {
            partitionIDs[i - rowID][j - columnID] = (this->tablePartitionID[i][j]);
        }
    }
    return partitionIDs;
};

std::set<uint64_t>* SimpleTable::getDistinctPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) {
    std::set<uint64_t>* partitionIDs = new std::set<uint64_t>;
    for (int i = rowID; i < rowID + height; i++) {
        for (int j = columnID; j < columnID + width; j++) {
            partitionIDs->insert((this->tablePartitionID[i][j]));
        }
    }
    return partitionIDs;
};

void SimpleTable::updatePartitionIndex(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height, uint64_t partitionID) {
    for (int i = rowID; i < rowID + width; i++) {
        for (int j = columnID; j < columnID + height; j++) {
            tablePartitionID[i][j] = partitionID;
        }
    }
};
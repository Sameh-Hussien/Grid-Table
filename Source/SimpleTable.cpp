#include "SimpleTable.h"
#include <inttypes.h>
#include <set>
#include <list>

Register<Table, SimpleTable, uint64_t&, uint64_t&, uint64_t&, const string&, bool&, uint64_t&, const string&> sTable("SimpleTable");

SimpleTable::SimpleTable(uint64_t tableID, uint64_t numCols, uint64_t numRows, const string& tableName, bool defaultStorageLayout, uint64_t partitionID, const string& overlapResolvingMethod) :
Table(tableID, numCols, numRows, tableName, defaultStorageLayout, overlapResolvingMethod) {
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

/**
 * Check if these are valid bounds of a partition
 */
bool SimpleTable::checkValidPartition(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height){
    return (
            (columnID >= 0 && columnID < this->numCols) &&
            (rowID >= 0 && rowID < this->numRows) &&
            (width >= 0 && width <= this->numCols - columnID) &&
            (height >= 0 && height <= this->numRows - rowID));
};

/**
 * Get the partition IDs of a certain rectangular area
 */
std::vector<std::vector<uint64_t>> SimpleTable::getPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) {
    if (SimpleTable::checkValidPartition( columnID,  rowID,  width,  height)) {
        width = (width == 0) ? this->numCols - columnID : width;
        height = (height == 0) ? this->numRows - rowID : height;
        std::vector<std::vector<uint64_t>> partitionIDs(height);
        for (int i = rowID; i < rowID + height; i++) {
            for (int j = columnID; j < columnID + width; j++) {
                partitionIDs[i - rowID].push_back(this->tablePartitionID[i][j]);
            }
        }
        return partitionIDs;
    } else {
        throw std::out_of_range("error: provided parameters are out of table range\r\n");
    }
};

/**
 * Get the distinct partition IDs of a certain rectangular area
 */
std::set<uint64_t>* SimpleTable::getDistinctPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) {
    if (SimpleTable::checkValidPartition( columnID,  rowID,  width,  height)) {
        width = (width == 0) ? this->numCols - columnID : width;
        height = (height == 0) ? this->numRows - rowID : height;
        std::set<uint64_t>* partitionIDs = new std::set<uint64_t>;
        for (int i = rowID; i < rowID + height; i++) {
            for (int j = columnID; j < columnID + width; j++) {
                partitionIDs->insert((this->tablePartitionID[i][j]));
            }
        }
        return partitionIDs;
    } else {
        throw std::out_of_range("error: provided parameters are out of table range\r\n");
    }
};

/**
 * Update the table partition index
 */
void SimpleTable::updatePartitionIndex(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height, uint64_t partitionID) {
    if (SimpleTable::checkValidPartition( columnID,  rowID,  width,  height)) {
        width = (width == 0) ? this->numCols - columnID : width;
        height = (height == 0) ? this->numRows - rowID : height;
        for (int i = rowID; i < rowID + height; i++) {
            for (int j = columnID; j < columnID + width; j++) {
                tablePartitionID[i][j] = partitionID;
            }
        }
    } else {
        throw std::out_of_range("error: provided parameters are out of table range\r\n");
    }
};

/**
 * Get the resolving way of overlapped partitions
 */
std::string SimpleTable::getTableOverlapResolvingMethod(){
    return this->overlapResolvingMethod;
};
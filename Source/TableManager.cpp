#include <iostream>
#include "TableManager.h"
#include "PartitionManager.h"
#include <string>
#include <vector>

//Initialization
uint64_t TableManager::nextTableID = 1;
std::map<uint64_t, Table*> *TableManager::tables = new std::map<uint64_t, Table*>;

/**
 *Create a new table defined by its size(rows,columns), name and default storage layout
 */
uint64_t TableManager::createTable(uint64_t numCols, uint64_t numRows, const std::string &tableName, bool defaultStorageLayout, std::string tableType, const std::string &overlapResolvingMethod) {
    bool tableTypeCheck = !(Factory< Table, uint64_t&, uint64_t&, uint64_t&, const string&, bool&, uint64_t&, const string&>::instance()->m_stock.find(tableType) ==
            Factory < Table, uint64_t&, uint64_t&, uint64_t&, const string&, bool&, uint64_t&, const string&>::instance()->m_stock.end());
    if (numRows > 0 && numCols > 0 && tableTypeCheck) {
        uint64_t newPartitionID = PartitionManager::createPartition(numRows, numCols, defaultStorageLayout, defaultStorageLayout == 1 ? "RowStorePartition" : "ColumnStorePartition");
        uint64_t newTableID = TableManager::nextTableID + 1;
        Table* newTable = Factory<Table, uint64_t&, uint64_t&, uint64_t&, const string&, bool&, uint64_t&, const string&>::instance()->create(tableType, newTableID, numCols, numRows, tableName, defaultStorageLayout, newPartitionID, overlapResolvingMethod);
        TableManager::nextTableID++;

        //Saving a reference to the table
        TableManager::tables->insert(std::pair<uint64_t, Table*>(newTable->tableID, newTable));
        std::cout << "You have now " << TableManager::tables->size() << " table(s)" << std::endl;

        std::cout << "Table ID: " << newTable->tableID << std::endl;
        std::cout << "Number of Rows: " << newTable->numRows << std::endl;
        std::cout << "Number of Columns: " << newTable->numCols << std::endl;
        std::cout << "Table Name: " << newTable->tableName << std::endl;
        std::cout << "Default Storage Layout: " << newTable->defaultStorageLayout << std::endl;
        std::cout << "Overlap Resolving Method: " << newTable->overlapResolvingMethod << std::endl;

        return newTable->tableID;
    } else {
        std::cout << "error: Table hasn't been created" << std::endl;
        throw std::invalid_argument("error: invalid table argument\r\n");
    }
}

/**
 *Drop an existing table
 */
void TableManager::dropTable(uint64_t tableID) {
    std::map<uint64_t, Table*>::const_iterator pos = TableManager::tables->find(tableID);
    if (pos == TableManager::tables->end()) {
        throw std::invalid_argument("error: no table found with id = " + tableID);
    } else {
        Table* tbl = pos->second;
        TableManager::tables->erase(pos);
        PartitionManager::dropPartition(tbl->getDistinctPartitionIDs(0, 0, tbl->numCols, tbl->numRows));
        delete tbl;

        std::cout << "You have now " << TableManager::tables->size() << " table(s)" << std::endl;
    }
}

/**
 *Print the partition IDs of a certain rectangular area
 */
std::vector<std::vector<uint64_t>> TableManager::getPartitionIDs(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) {
    std::map<uint64_t, Table*>::const_iterator pos = TableManager::tables->find(tableID);
    if (pos == TableManager::tables->end()) {
        throw std::invalid_argument("error: no table found with id = " + tableID);
    } else {
        Table* tbl = pos->second;
        return tbl->getPartitionIDs(columnID, rowID, width, height);
    }
}

/**
 *Print the distinct partition IDs of a certain rectangular area
 */
std::set<uint64_t>* TableManager::getdistinctPartitionIDs(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) {
    std::map<uint64_t, Table*>::const_iterator pos = TableManager::tables->find(tableID);
    if (pos == TableManager::tables->end()) {
        throw std::invalid_argument("error: no table found with id = " + tableID);
    } else {
        Table* tbl = pos->second;
        return tbl->getDistinctPartitionIDs(columnID, rowID, width, height);
    }
}

/**
 * Check if the provided partition data exists within the table range
 */
bool TableManager::checkValidPartition(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) {
    std::map<uint64_t, Table*>::const_iterator pos = TableManager::tables->find(tableID);
    if (pos == TableManager::tables->end()) {
        throw std::invalid_argument("error: no table found with id = " + tableID);
    } else {
        Table* tbl = pos->second;
        return tbl->checkValidPartition(columnID, rowID, width, height);
    }
}

/**
 * Get the overlap resolving method for a table
 */
std::string TableManager::getTableOverlapResolvingMethod(uint64_t tableID){
    std::map<uint64_t, Table*>::const_iterator pos = TableManager::tables->find(tableID);
    if (pos == TableManager::tables->end()) {
        throw std::invalid_argument("error: no table found with id = " + tableID);
    } else {
        Table* tbl = pos->second;
        return tbl->getTableOverlapResolvingMethod();
    }
}

/**
 * Update table partition index
 */
void TableManager::updateTablePartitionIndex(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height, uint64_t partitionID){
    std::map<uint64_t, Table*>::const_iterator pos = TableManager::tables->find(tableID);
    if (pos == TableManager::tables->end()) {
        throw std::invalid_argument("error: no table found with id = " + tableID);
    } else {
        Table* tbl = pos->second;
        tbl->updatePartitionIndex(columnID, rowID, width, height, partitionID);
    }
}
#include "PartitionManager.h"
#include <set>

//Initialization
uint64_t PartitionManager::nextPartitionID = 1;
std::map<uint64_t, Partition*> *PartitionManager::Partitions = new std::map<uint64_t, Partition*>;

/**
 * Create a new partition defined by its size(rows,columns), storage layout and type
 * The function is for direct trusted internal use by the table manger and the consistency checker
 */
uint64_t PartitionManager::createPartition(uint64_t numRows, uint64_t numCols, bool storageLayout, std::string partitionType) {

    uint64_t id = PartitionManager::nextPartitionID + 1;
    Partition* newPartition = Factory<Partition, uint64_t&, uint64_t&, uint64_t&, bool& >::instance()->create(partitionType, id, numRows, numCols, storageLayout);
    PartitionManager::nextPartitionID++;

    //Saving a reference to the partition
    PartitionManager::Partitions->insert(std::pair<uint64_t, Partition*>(newPartition->partitionID, newPartition));
    std::cout << "You have now " << PartitionManager::Partitions->size() << " partition(s)" << std::endl;

    std::cout << "Partition ID: " << newPartition->partitionID << std::endl;
    std::cout << "Number of Rows: " << newPartition->numRows << std::endl;
    std::cout << "Number of Columns: " << newPartition->numCols << std::endl;
    std::cout << "Storage Layout: " << newPartition->storageLayout << std::endl;

    return newPartition->partitionID;
    std::cout << "error: Partition hasn't been created" << std::endl;
    return 0;
}

/*
 * Create a new partition defined by its size(rows,columns), storage layout and type inside a table
 * The function acts as a partition creation interface for external use
 */
uint64_t PartitionManager::createPartition(uint64_t tableID, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols, bool storageLayout, std::string partitionType) {

    return ConsistencyChecker::checkNewPartitionIssues(tableID, rowID, columnID, numRows, numCols, storageLayout, partitionType);
}

/**
 *Drop an existing partition
 */
void PartitionManager::dropPartition(uint64_t partitionID) {
    std::map<uint64_t, Partition*>::const_iterator pos = PartitionManager::Partitions->find(partitionID);
    if (pos == PartitionManager::Partitions->end()) {
        std::cout << "no partition found with id = " << partitionID << std::endl;
    } else {
        Partition* partition = pos->second;
        PartitionManager::Partitions->erase(pos);
        delete partition;
        std::cout << "You have now " << PartitionManager::Partitions->size() << " partition(s)" << std::endl;
    }
}

/**
 *Drop a set of partitions
 */
void PartitionManager::dropPartition(std::set<uint64_t>* partitionID) {
    std::set<uint64_t>::iterator it;
    for (it = partitionID->begin(); it != partitionID->end(); ++it) {
        uint64_t pID = *it;
        std::map<uint64_t, Partition*>::const_iterator pos = PartitionManager::Partitions->find(pID);
        if (pos == PartitionManager::Partitions->end()) {
            std::cout << "no partition found with id = " << pID << std::endl;
        } else {
            Partition* partition = pos->second;
            PartitionManager::Partitions->erase(pos);
            delete partition;
            std::cout << "You have now " << PartitionManager::Partitions->size() << " partition(s)" << std::endl;
        }
    }
}


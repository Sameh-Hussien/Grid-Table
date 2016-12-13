#ifndef PARTITIONMANAGER_H
#define PARTITIONMANAGER_H
#include <map>
#include <set>
#include <inttypes.h>
#include "ConsistencyChecker.h"
#include "Partition.h"

class PartitionManager {
private:
    //The ID that will be assigned for next created partition
    static uint64_t nextPartitionID;
    //References to the pool of existing partitions
    static std::map<uint64_t, Partition*> *Partitions;

    friend class TableManager;
    friend class ConsistencyChecker;
    PartitionManager() {
    };
    /*
     * Create a new partition defined by its size(rows,columns), storage layout and type
     * The function is for direct trusted internal use by the table manger and the consistency checker
     */
    static uint64_t createPartition(uint64_t numRows, uint64_t numCols, bool storageLayout, std::string partitionType);
    //Drop an existing partition
    static void dropPartition(uint64_t partitionID);
    //Drop a set of partitions
    static void dropPartition(std::set<uint64_t>* partitionID);
public:
    /*
     * Create a new partition defined by its size(rows,columns), storage layout and type inside a table
     * The function acts as a partition creation interface for external use
     */
    static uint64_t createPartition(uint64_t tableID, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols, bool storageLayout, std::string partitionType);
};

#endif /* PARTITIONMANAGER_H */


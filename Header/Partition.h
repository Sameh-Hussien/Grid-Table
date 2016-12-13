#ifndef PARTITION_H
#define PARTITION_H
#include <inttypes.h>
#include "FactoryCreator.h"

class Partition {
private:
    //A unique ID for the partition
    uint64_t partitionID;
    //partition number of rows and columns
    uint64_t numRows, numCols;
    //The storage layout for the partition (true==>row, false==>column)
    bool storageLayout;

    friend class PartitionManager;

protected:

    Partition(uint64_t partitionID, uint64_t numRows, uint64_t numCols, bool storageLayout) {
        this->partitionID = partitionID;
        this->numRows = numRows;
        this->numCols = numCols;
        this->storageLayout = storageLayout;
    };


};
#endif /* PARTITION_H */


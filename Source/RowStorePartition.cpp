#include"RowStorePartition.h"
#include <inttypes.h>

Register<Partition, RowStorePartition, uint64_t&, uint64_t&, uint64_t&, bool& > hPartition("RowStorePartition");

RowStorePartition::RowStorePartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols, bool storageLayout) :
    Partition(partitionID, numRows, numCols, storageLayout) {};

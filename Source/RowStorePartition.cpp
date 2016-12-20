#include"RowStorePartition.h"
#include <inttypes.h>

Register<Partition, RowStorePartition, uint64_t&, uint64_t&, uint64_t&> hPartition("RowStorePartition");

RowStorePartition::RowStorePartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols) :
    Partition(partitionID, numRows, numCols, 1) {};

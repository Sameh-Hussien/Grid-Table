#include "ColumnStorePartition.h"
#include <inttypes.h>

Register<Partition, ColumnStorePartition, uint64_t&, uint64_t&, uint64_t&> vPartition("ColumnStorePartition");

ColumnStorePartition::ColumnStorePartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols) :
    Partition(partitionID, numRows, numCols, 0) {};

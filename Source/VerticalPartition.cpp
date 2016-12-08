#include "VerticalPartition.h"
#include <inttypes.h>

Register<Partition, VerticalPartition, uint64_t&, uint64_t&, uint64_t&, bool& > vPartition("VerticalPartition");

VerticalPartition::VerticalPartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols, bool storageLayout) :
    Partition(partitionID, numRows, numCols, storageLayout) {};

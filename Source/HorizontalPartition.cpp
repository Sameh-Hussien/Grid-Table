#include"HorizontalPartition.h"
#include <inttypes.h>

Register<Partition, HorizontalPartition, uint64_t&, uint64_t&, uint64_t&, bool& > hPartition("HorizontalPartition");

HorizontalPartition::HorizontalPartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols, bool storageLayout) :
    Partition(partitionID, numRows, numCols, storageLayout) {};

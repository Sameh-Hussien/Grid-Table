#ifndef HORIZONTALPARTITION_H
#define HORIZONTALPARTITION_H
#include "Partition.h"

class HorizontalPartition : public Partition {
private:
    friend class PartitionManager;
    friend class Factory<Partition, uint64_t&, uint64_t&, uint64_t&, bool& >::Creator<HorizontalPartition>;

    HorizontalPartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols, bool storageLayout);
};
#endif /* HORIZONTALPARTITION_H */
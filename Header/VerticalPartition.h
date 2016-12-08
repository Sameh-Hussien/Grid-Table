#ifndef VERTICALPARTITION_H
#define VERTICALPARTITION_H
#include "Partition.h"

class VerticalPartition : public Partition {
private:
    friend class PartitionManager;
    friend class Factory<Partition, uint64_t&, uint64_t&, uint64_t&, bool& >::Creator<VerticalPartition>;

    VerticalPartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols, bool storageLayout);
};
#endif /* VERTICALPARTITION_H */


#ifndef COLUMNSTOREPARTITION_H
#define COLUMNSTOREPARTITION_H
#include "Partition.h"

class ColumnStorePartition : public Partition {
private:
    friend class Factory<Partition, uint64_t&, uint64_t&, uint64_t&, bool& >::Creator<ColumnStorePartition>;

    ColumnStorePartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols, bool storageLayout);
};
#endif /* COLUMNSTOREPARTITION_H */


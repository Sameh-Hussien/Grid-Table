#ifndef ROWSTOREPARTITION_H
#define ROWSTOREPARTITION_H
#include "Partition.h"

class RowStorePartition : public Partition {
private:
    friend class Factory<Partition, uint64_t&, uint64_t&, uint64_t&>::Creator<RowStorePartition>;

    RowStorePartition(uint64_t partitionID, uint64_t numRows, uint64_t numCols);
};
#endif /* ROWSTOREPARTITION_H */
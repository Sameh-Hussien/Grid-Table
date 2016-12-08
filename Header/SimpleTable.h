#include "Table.h"

class SimpleTable : public Table {
private:
    friend class TableManager;
    friend class Factory<Table, uint64_t&, uint64_t&, uint64_t&, const string&, bool&, uint64_t&>::Creator<SimpleTable>;
    //Grid Partition Index
    uint64_t** tablePartitionID;

    SimpleTable(uint64_t tableID, uint64_t numRows, uint64_t numCols, const string& tableName, bool defaultStorageLayout, uint64_t partitionID);
    void updatePartitionIndex(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height, uint64_t partitionID);
    uint64_t** getPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
    std::set<uint64_t>* getDistinctPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
};
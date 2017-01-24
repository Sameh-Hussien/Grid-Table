#include "Table.h"

class SimpleTable : public Table {
private:
    friend class Factory<Table, uint64_t&, uint64_t&, uint64_t&, const string&, bool&, uint64_t&, const string&>::Creator<SimpleTable>;
    //Table Partition Index
    uint64_t** tablePartitionID;

    SimpleTable(uint64_t tableID, uint64_t numCols, uint64_t numRows, const string& tableName, bool defaultStorageLayout, uint64_t partitionID, const string& overlapResolvingMethod);
    //Check if these are valid bounds of a partition
    bool checkValidPartition(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
    //Update the table partition index
    void updatePartitionIndex(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height, uint64_t partitionID);
    //Get the partition IDs of a certain rectangular area
    std::vector<std::vector<uint64_t>> getPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
    //Get the distinct partition IDs of a certain rectangular area
    std::set<uint64_t>* getDistinctPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
    //Get the resolving way of overlapped partitions
    std::string getTableOverlapResolvingMethod();
    //Get the table partition index
    const uint64_t** getTablePartitionIndex();
    //Drop the table partition index
    void dropTablePartitionIndex();
};
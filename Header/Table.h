#include <string>
#include <set>
#include <vector>
#include <map>
#include <inttypes.h>
#include "FactoryCreator.h"

class Table {
protected:
    //A unique ID for the table
    uint64_t tableID;
    //the name of the table
    std::string tableName;
    //table number of rows and columns
    uint64_t numRows, numCols;
    //The default storage layout for the table (true==>row, false==>column)
    bool defaultStorageLayout;
    //The method to use to resolve partitions overlap
    std::string overlapResolvingMethod;

    friend class TableManager;
    virtual bool checkValidPartition(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height)=0;
    virtual void updatePartitionIndex(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height, uint64_t partitionID) = 0;
    virtual std::vector<std::vector<uint64_t>> getPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) = 0;
    virtual std::set<uint64_t>* getDistinctPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height)=0;
    virtual std::string getTableOverlapResolvingMethod()=0;
    virtual const uint64_t** getTablePartitionIndex()=0;
    virtual void dropTablePartitionIndex()=0;

protected:

    Table(uint64_t tableID, uint64_t numCols, uint64_t numRows, const std::string& tableName, bool defaultStorageLayout, const std::string& overlapResolvingMethod) {
        this->tableID = tableID;
        this->tableName = tableName;
        this->numRows = numRows;
        this->numCols = numCols;
        this->defaultStorageLayout = defaultStorageLayout;
        this->overlapResolvingMethod = overlapResolvingMethod;
    };


};


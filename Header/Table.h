#include <string>
#include <set>
#include <vector>
#include <map>
#include <inttypes.h>
#include "FactoryCreator.h"

class Table {
private:
    //A unique ID for the table
    uint64_t tableID;
    //the name of the table
    std::string tableName;
    //table number of rows and columns
    uint64_t numRows, numCols;
    //The default storage layout for the table (true==>row, false==>column)
    bool defaultStorageLayout;

    friend class TableManager;
    virtual void updatePartitionIndex(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height, uint64_t partitionID) = 0;
    virtual uint64_t** getPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height) = 0;
    virtual std::set<uint64_t>* getDistinctPartitionIDs(uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height)=0;

public:

    Table(uint64_t tableID, uint64_t numRows, uint64_t numCols, const std::string& tableName, bool defaultStorageLayout) {
        this->tableID = tableID;
        this->tableName = tableName;
        this->numRows = numRows;
        this->numCols = numCols;
        this->defaultStorageLayout = defaultStorageLayout;
    };


};


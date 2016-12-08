#include <list>
#include <map>
#include <vector>
#include "Table.h"
#include <string>

class TableManager {
private:
    //The ID that will be assigned for next created table
    static uint64_t nextTableID;
    //References to the pool of existing tables
    static std::map<uint64_t, Table*> *tables;

    TableManager() {};
public:
    //Print a certain column partitions
    static uint64_t** getTablePartitions(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
    //Drop an existing table
    static void dropTable(uint64_t tableID);
    //Create a new table defined by its size(rows,columns), name, default storage layout and type
    static uint64_t createTable(uint64_t numRows, uint64_t numCols, const std::string &tableName, bool defaultStorageLayout, std::string tableType);
};
#include <list>
#include <map>
#include <set>
#include <vector>
#include "Table.h"
#include <string>

class TableManager {
private:
    //The ID that will be assigned for next created table
    static uint64_t nextTableID;
    //References to the pool of existing tables
    static std::map<uint64_t, Table*> *tables;

    friend class ConsistencyChecker;
    
    TableManager() {};
    //Update table partition index
    static void updateTablePartitionIndex(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height, uint64_t partitionID);
public:
    //Check if the provided partition data exists within the table range
    static bool checkValidPartition(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
    //Print the partition IDs of a certain rectangular area
    static std::vector<std::vector<uint64_t>> getPartitionIDs(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
    //Print the distinct partition IDs of a certain rectangular area
    static std::set<uint64_t>* getdistinctPartitionIDs(uint64_t tableID, uint64_t columnID, uint64_t rowID, uint64_t width, uint64_t height);
    //Get the overlap resolving method for a table
    static std::string getTableOverlapResolvingMethod(uint64_t tableID);
    //Drop an existing table
    static void dropTable(uint64_t tableID);
    //Create a new table defined by its size(rows,columns), name, default storage layout and type
    static uint64_t createTable(uint64_t numCols, uint64_t numRows, const std::string &tableName, bool defaultStorageLayout, std::string tableType, const std::string &overlapResolvingMethod);
};
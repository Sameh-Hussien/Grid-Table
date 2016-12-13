#ifndef CONSISTENCYCHECKER_H
#define CONSISTENCYCHECKER_H

#include <inttypes.h>
#include <string>

class ConsistencyChecker {
private:
    friend class PartitionManager;
    ConsistencyChecker();
    //check for any issues could result from the creation of new partition
    static uint64_t checkNewPartitionIssues(uint64_t tableID, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols, bool storageLayout, std::string partitionType);
};

#endif /* CONSISTENCYCHECKER_H */


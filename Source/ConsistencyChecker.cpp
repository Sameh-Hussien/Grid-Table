#include "ConsistencyChecker.h"
#include "TableManager.h"
#include "OverlapResolver.h"
#include "PartitionManager.h"

/**
 * check for any issues could result from the creation of new partition
 */
uint64_t ConsistencyChecker::checkNewPartitionIssues(uint64_t tableID, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols, std::string partitionType) {
    uint64_t newPartitionID = 0;
    if (TableManager::checkValidPartition(tableID, columnID, rowID, numCols, numRows)) {
        const uint64_t** partitionIndex = TableManager::getTablePartitionIndex(tableID);
        OverlapResolver* newOverlapResolver = Factory<OverlapResolver>::instance()->create(TableManager::getTableOverlapResolvingMethod(tableID));
        std::vector<std::vector<uint64_t>*> partitionList = newOverlapResolver->ResolveOverlapIssue(partitionIndex, rowID, columnID, numRows, numCols);

        newPartitionID = PartitionManager::createPartition(numRows, numCols, partitionType);
        for (int i = 0; i < partitionList.size(); i++) {
            if (partitionList[i]->data()[0] == 1) {
                PartitionManager::changePartitionSize(partitionList[i]->data()[1],-1 * (int64_t)(partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1), -1*(int64_t)(partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1));
                partitionList[i]->data()[1] = PartitionManager::createPartition(partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1, partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1, partitionType);
            } else if (partitionList[i]->data()[0] == 0) {
                PartitionManager::dropPartition(partitionList[i]->data()[1]);
            }
        }
        
        TableManager::updateTablePartitionIndex(tableID, columnID, rowID, numCols, numRows, newPartitionID);
        for (int i = 0; i < partitionList.size(); i++) {
            if (partitionList[i]->data()[0] == 1) {
                TableManager::updateTablePartitionIndex(tableID, partitionList[i]->data()[4], partitionList[i]->data()[2], partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1, partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1,partitionList[i]->data()[1]);
            }
        }
    }
    return newPartitionID;
};
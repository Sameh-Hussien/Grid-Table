#include "ConsistencyChecker.h"
#include "TableManager.h"
#include "OverlapResolver.h"
#include "PartitionManager.h"
#include "Test.h"
#include <chrono>
#include <windows.h>                // for Windows APIs

/**
 * check for any issues could result from the creation of new partition
 */
uint64_t ConsistencyChecker::checkNewPartitionIssues(uint64_t tableID, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols, std::string partitionType) {
    LARGE_INTEGER timeBefore, timeAfter;
    LARGE_INTEGER frequency;
    storingTime = 0;
    uint64_t newPartitionID = 0, partitionID = 0, newPartitionsNumber = 0, p;
    if (TableManager::checkValidPartition(tableID, columnID, rowID, numCols, numRows)) {
        const uint64_t** partitionIndex = TableManager::getTablePartitionIndex(tableID);
        OverlapResolver* newOverlapResolver = Factory<OverlapResolver>::instance()->create(TableManager::getTableOverlapResolvingMethod(tableID));
        std::vector<std::vector<uint64_t>*> partitionList = newOverlapResolver->ResolveOverlapIssue(partitionIndex, rowID, columnID, numRows, numCols);
        newPartitionID = PartitionManager::createPartition(numRows, numCols, partitionType);
        for (int i = 0; i < partitionList.size(); i++) {
            p = partitionList[i]->data()[1];
            if (partitionList[i]->data()[0] == 1) {
                if (partitionList[i]->data()[1] != partitionID) {
                    partitionID = partitionList[i]->data()[1];
                    newPartitionsNumber = 0;
                } else {
                    newPartitionsNumber++;
                }
                if (newPartitionsNumber != 1) {
                    if (TableManager::getTableOverlapResolvingMethod(tableID) == "HorizontalOverlapResolver") {
                        PartitionManager::changePartitionSize(partitionList[i]->data()[1], -1 * (int64_t) (partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1), 0);
                    } else {
                        PartitionManager::changePartitionSize(partitionList[i]->data()[1], 0, -1 * (int64_t) (partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1));
                    }
                }
                partitionList[i]->data()[1] = PartitionManager::createPartition(partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1, partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1, partitionType);
            } else if (partitionList[i]->data()[0] == 5) {
                if (TableManager::getTableOverlapResolvingMethod(tableID) == "HorizontalOverlapResolver") {
                    PartitionManager::changePartitionSize(partitionList[i]->data()[1], -1 * (int64_t) (partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1 + numRows), 0);
                } else {
                    PartitionManager::changePartitionSize(partitionList[i]->data()[1], 0, -1 * (int64_t) (partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1 + numCols));
                }
                partitionList[i]->data()[1] = PartitionManager::createPartition(partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1, partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1, partitionType);
            } else if (partitionList[i]->data()[0] == 10) {
                if (TableManager::getTableOverlapResolvingMethod(tableID) == "HorizontalOverlapResolver") {
                    PartitionManager::changePartitionSize(partitionList[i]->data()[1], -1 * (int64_t) (partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1), 0);
                } else {
                    PartitionManager::changePartitionSize(partitionList[i]->data()[1], 0, -1 * (int64_t) (partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1));
                }
                partitionList[i]->data()[1] = PartitionManager::createPartition(partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1, partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1, partitionType);
            } else if (partitionList[i]->data()[0] == 2) {
                if (partitionList[i]->data()[2] == (uint64_t) (-1) || partitionList[i]->data()[3] == (uint64_t) (-1)) {
                    partitionList[i]->data()[2] = (partitionList[i]->data()[2] == (uint64_t) (-1)) ? rowID : partitionList[i]->data()[2];
                    partitionList[i]->data()[3] = (partitionList[i]->data()[3] == (uint64_t) (-1)) ? rowID + numRows - 1 : partitionList[i]->data()[3];
                    PartitionManager::changePartitionSize(partitionList[i]->data()[1], -1 * (int64_t) (partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1), 0);
                } else {
                    partitionList[i]->data()[4] = (partitionList[i]->data()[4] == (uint64_t) (-1)) ? columnID : partitionList[i]->data()[4];
                    partitionList[i]->data()[5] = (partitionList[i]->data()[5] == (uint64_t) (-1)) ? columnID + numCols - 1 : partitionList[i]->data()[5];
                    PartitionManager::changePartitionSize(partitionList[i]->data()[1], 0, -1 * (int64_t) (partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1));
                }
            } else {
                PartitionManager::dropPartition(partitionList[i]->data()[1]);
            }
        }

        // get ticks per second
        QueryPerformanceFrequency(&frequency);
        // start timer
        QueryPerformanceCounter(&timeBefore);
        //timeBefore = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        TableManager::updateTablePartitionIndex(tableID, columnID, rowID, numCols, numRows, newPartitionID);
        // stop timer
        QueryPerformanceCounter(&timeAfter);
        //storingTime += (timeAfter.QuadPart - timeBefore.QuadPart) * 1000000.0 / frequency.QuadPart;
        //timeAfter = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        //storingTime += (timeAfter - timeBefore);
        for (int i = 0; i < partitionList.size(); i++) {
            if (partitionList[i]->data()[0] == 1 || partitionList[i]->data()[0] == 10 || partitionList[i]->data()[0] == 5) {

                // get ticks per second
                QueryPerformanceFrequency(&frequency);
                // start timer
                QueryPerformanceCounter(&timeBefore);
                //timeBefore = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                TableManager::updateTablePartitionIndex(tableID, partitionList[i]->data()[4], partitionList[i]->data()[2], partitionList[i]->data()[5] - partitionList[i]->data()[4] + 1, partitionList[i]->data()[3] - partitionList[i]->data()[2] + 1, partitionList[i]->data()[1]);
                // stop timer
                QueryPerformanceCounter(&timeAfter);
                storingTime += (timeAfter.QuadPart - timeBefore.QuadPart) * 1000000.0 / frequency.QuadPart;
                //timeAfter = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                //storingTime += (timeAfter - timeBefore);
            }
        }
    }
    return newPartitionID;
};
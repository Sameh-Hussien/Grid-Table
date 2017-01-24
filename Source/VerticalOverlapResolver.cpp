#include <vector>
#include <iostream>
#include <set>
#include "VerticalOverlapResolver.h"
#include "Test.h"
#include <chrono>

Register<OverlapResolver, VerticalOverlapResolver> vOverlapResolver("VerticalOverlapResolver");

VerticalOverlapResolver::VerticalOverlapResolver() {
}

/**
 * Resolve overlap issue resulted from the creation of new partition
 */
std::vector<std::vector<uint64_t>*> VerticalOverlapResolver::ResolveOverlapIssue(const uint64_t** partitionIndex, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols) {
    uint64_t timeBefore = 0, timeAfter = 0;
    scaningTime = 0;
    scans = 0;
    overlappedPartitions = 0;

    uint64_t tableNoOfRows = partitionIndex[0][1];
    uint64_t tableNoOfColumns = partitionIndex[0][2];
    partitionIndex = &partitionIndex[1];

    std::vector<std::vector < uint64_t>*> partitionList;
    std::map<uint64_t, std::vector < uint64_t>*> *partitionTableHit = new std::map<uint64_t, std::vector < uint64_t>*>;

    std::vector<uint64_t>* tempPartition = new std::vector<uint64_t>;
    std::vector<uint64_t>* partition;
    int64_t partitionUpperBound, partitionLowerBound, partitionLeftBound, partitionRightBound;
    uint64_t p0 = 0;

    for (int j = columnID; j < columnID + numCols; j++) {
        for (int i = rowID; i < rowID + numRows; i++) {
            std::map<uint64_t, std::vector < uint64_t>*>::const_iterator pos = partitionTableHit->find(partitionIndex[i][j]);
            if (pos == partitionTableHit->end()) {
                partition = new std::vector < uint64_t>;
                partition->push_back(4);
                p0 = partition->data()[0];
                //left boundary
                if (j != columnID) {
                    partition->push_back(j);
                    (partition->data()[0])--;
                    p0 = partition->data()[0];
                } else {
                    partition->push_back(-1);
                }
                //right boundary
                if (j != columnID + numCols - 1) {
                    partition->push_back(j);
                    (partition->data()[0])--;
                    p0 = partition->data()[0];
                } else {
                    partition->push_back(-1);
                }
                //upper boundary
                if (i != rowID) {
                    partition->push_back(i);
                    (partition->data()[0])--;
                    p0 = partition->data()[0];
                } else {
                    partition->push_back(-1);
                }
                //lower boundary
                if (i != rowID + numRows - 1) {
                    partition->push_back(i);
                    (partition->data()[0])--;
                    p0 = partition->data()[0];
                } else {
                    partition->push_back(-1);
                }
                overlappedPartitions++;
                partitionTableHit->insert(std::pair<uint64_t, std::vector < uint64_t>*>(partitionIndex[i][j], partition));
            } else {
                partition = pos->second;
                p0 = partition->data()[0];
                //right boundary
                if (j != columnID + numCols - 1) {
                    if (partition->data()[2] == -1)
                        partition->data()[0] = (partition->data()[0]) - 1;
                    p0 = partition->data()[0];
                    (partition->data()[2]) = j;
                } else if (j == columnID + numCols - 1) {
                    if (partition->data()[2] != -1)
                        partition->data()[0] = (partition->data()[0]) + 1;
                    p0 = partition->data()[0];
                    (partition->data()[2]) = -1;
                }
                //lower boundary
                if (i != rowID + numRows - 1) {
                    if (partition->data()[4] == -1)
                        partition->data()[0] = (partition->data()[0]) - 1;
                    p0 = partition->data()[0];
                    (partition->data()[4]) = i;
                } else if (i == rowID + numRows - 1) {
                    if (partition->data()[4] != -1)
                        partition->data()[0] = (partition->data()[0]) + 1;
                    p0 = partition->data()[0];
                    (partition->data()[4]) = -1;
                }
            }
        }
    }


    for (std::map<uint64_t, std::vector < uint64_t>*>::iterator it = partitionTableHit->begin(); it != partitionTableHit->end(); it++) {
        uint64_t partitionID = it->first;
        partition = it->second;
        int64_t x;
        bool partionCreated = false;
        if (partition->data()[0] > 1) {
            //Step 1 (Up)
            x = rowID - 1;
            if (partition->data()[3] == -1 && partition->data()[0] > 1) {
                partitionUpperBound = rowID;
                partitionLowerBound = rowID - 1;
                partitionRightBound = (partition->data()[2] == -1) ? columnID + numCols - 1 : partition->data()[2];
                partitionLeftBound = (partition->data()[1] == -1) ? columnID : partition->data()[1];

                timeBefore = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                while (x >= 0) {
                    scans++;
                    if (partitionIndex[x][partitionRightBound] != partitionIndex[rowID][partitionRightBound]) {
                        partitionUpperBound = x + 1;
                        break;
                    } else {
                        partitionUpperBound = x;
                        x--;
                    }
                }
                timeAfter = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                scaningTime += (timeAfter - timeBefore);

                if (partitionUpperBound <= partitionLowerBound) {
                    //create partition
                    tempPartition->push_back(1);
                    tempPartition->push_back(partitionID);
                    tempPartition->push_back(partitionUpperBound);
                    tempPartition->push_back(partitionLowerBound);
                    tempPartition->push_back(partitionLeftBound);
                    tempPartition->push_back(partitionRightBound);

                    partitionList.push_back(tempPartition);
                    partionCreated = true;
                }
                tempPartition = new std::vector<uint64_t>;

                partition->data()[3] = x + 1;
                partition->data()[0]--;
            }

            //Step 2 (Down)
            x = rowID + numRows;
            if ((partition->data()[4] == -1 && partition->data()[0] > 1)
                    || (partition->data()[4] == -1 && partionCreated)) {
                partitionUpperBound = rowID + numRows;
                partitionLowerBound = rowID + numRows - 1;
                partitionRightBound = (partition->data()[2] == -1) ? columnID + numCols - 1 : partition->data()[2];
                partitionLeftBound = (partition->data()[1] == -1) ? columnID : partition->data()[1];

                timeBefore = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                while (x < tableNoOfRows) {
                    scans++;
                    if (partitionIndex[x][partitionRightBound] != partitionIndex[rowID + numRows - 1][partitionRightBound]) {
                        partitionLowerBound = x - 1;
                        break;
                    } else {
                        partitionLowerBound = x;
                        x++;
                    }
                }
                timeAfter = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                scaningTime += (timeAfter - timeBefore);

                if (partitionUpperBound <= partitionLowerBound) {
                    //create partition
                    tempPartition->push_back(1);
                    tempPartition->push_back(partitionID);
                    tempPartition->push_back(partitionUpperBound);
                    tempPartition->push_back(partitionLowerBound);
                    tempPartition->push_back(partitionLeftBound);
                    tempPartition->push_back(partitionRightBound);

                    partitionList.push_back(tempPartition);
                    partionCreated = true;
                }
                tempPartition = new std::vector<uint64_t>;

                partition->data()[4] = x - 1;
                partition->data()[0]--;
            }

            //Step 3 (Left)
            x = columnID - 1;
            if (partition->data()[1] == -1 && partition->data()[0] > 1) {
                partitionUpperBound = partition->data()[3];
                partitionLowerBound = partition->data()[4];
                partitionRightBound = columnID - 1;
                partitionLeftBound = columnID;

                timeBefore = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                while (x >= 0) {
                    scans++;
                    if (partitionIndex[partitionLowerBound][x] != partitionIndex[partitionLowerBound][columnID]) {
                        partitionLeftBound = x + 1;
                        break;
                    } else {
                        partitionLeftBound = x;
                        x--;
                    }
                }
                timeAfter = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
                scaningTime += (timeAfter - timeBefore);

                if (partitionLeftBound <= partitionRightBound) {
                    //create partition
                    if (partition->data()[1] == -1 && partition->data()[2] == -1 && !partionCreated) {
                        tempPartition->push_back(5);
                    } else if (partition->data()[1] == -1 && partition->data()[2] == -1 &&
                            (partitionUpperBound >= rowID || partitionLowerBound <= rowID + numRows - 1)) {
                        tempPartition->push_back(10);
                    } else {
                        tempPartition->push_back(1);
                    }
                    tempPartition->push_back(partitionID);
                    tempPartition->push_back(partitionUpperBound);
                    tempPartition->push_back(partitionLowerBound);
                    tempPartition->push_back(partitionLeftBound);
                    tempPartition->push_back(partitionRightBound);

                    partitionList.push_back(tempPartition);
                    partionCreated = true;
                }
                tempPartition = new std::vector<uint64_t>;

                partition->data()[1] = x + 1;
                partition->data()[0]--;
            }
        } else if (partition->data()[0] == 0) {
            tempPartition->push_back(0);
            tempPartition->push_back(partitionID);
            tempPartition->push_back(0);
            tempPartition->push_back(0);
            tempPartition->push_back(0);
            tempPartition->push_back(0);

            partitionList.push_back(tempPartition);
            tempPartition = new std::vector<uint64_t>;
        }

        if (partition->data()[0] == 1 && partionCreated == false) {
            tempPartition->push_back(2);
            tempPartition->push_back(partitionID);
            uint64_t i = partition->data()[3];
            i = partition->data()[4];
            i = partition->data()[1];
            i = partition->data()[2];
            tempPartition->push_back(partition->data()[3]);
            tempPartition->push_back(partition->data()[4]);
            tempPartition->push_back(partition->data()[1]);
            tempPartition->push_back(partition->data()[2]);

            partitionList.push_back(tempPartition);
            tempPartition = new std::vector<uint64_t>;
        }
    }
    /*
        tempPartition = new std::vector<uint64_t>;

        for (std::map<uint64_t, std::vector < uint64_t>*>::iterator it = partitionTableHit->begin(); it != partitionTableHit->end(); it++) {
            if (it->second->data()[0] == 0) {
                tempPartition->push_back(0);
                tempPartition->push_back(it->first);
                tempPartition->push_back(0);
                tempPartition->push_back(0);
                tempPartition->push_back(0);
                tempPartition->push_back(0);

                partitionList.push_back(tempPartition);
                tempPartition = new std::vector<uint64_t>;
            }
        }
     */

    return partitionList;
}
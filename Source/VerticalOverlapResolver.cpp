#include <vector>
#include <iostream>
#include <set>
#include "VerticalOverlapResolver.h"

Register<OverlapResolver, VerticalOverlapResolver> vOverlapResolver("VerticalOverlapResolver");

VerticalOverlapResolver::VerticalOverlapResolver() {
}

/**
 * Resolve overlap issue resulted from the creation of new partition
 */
std::vector<std::vector<uint64_t>*> VerticalOverlapResolver::ResolveOverlapIssue(const uint64_t** partitionIndex, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols) {

    uint64_t tableNoOfRows = partitionIndex[0][1];
    uint64_t tableNoOfColumns = partitionIndex[0][2];
    partitionIndex = &partitionIndex[1];

    std::vector<std::vector < uint64_t>*> partitionList;
    std::map<uint64_t, std::vector < uint64_t>*> *partitionTableHit = new std::map<uint64_t, std::vector < uint64_t>*>;

    std::vector<uint64_t>* tempPartition = new std::vector<uint64_t>;
    std::vector<uint64_t>* partition = new std::vector<uint64_t>;
    int64_t partitionUpperBound, partitionLowerBound, partitionLeftBound, partitionRightBound;
    uint64_t p0 = 0;

    for (int j = columnID; j < columnID + numCols; j++) {
        for (int i = rowID; i < rowID + numRows; i++) {
            std::map<uint64_t, std::vector < uint64_t>*>::const_iterator pos = partitionTableHit->find(partitionIndex[i][j]);
            if (pos == partitionTableHit->end()) {
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
                partitionTableHit->insert(std::pair<uint64_t, std::vector < uint64_t>*>(partitionIndex[i][j], partition));
                partition = new std::vector < uint64_t>;
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
                partition = new std::vector < uint64_t>;
            }
        }
    }

    for (std::map<uint64_t, std::vector < uint64_t>*>::iterator it = partitionTableHit->begin(); it != partitionTableHit->end(); it++) {
        uint64_t partitionID = it->first;
        partition = it->second;
        int64_t x;
        if (partition->data()[0] > 1) {

            //Step 1 (Up)
            x = rowID - 1;
            if (partition->data()[3] == -1 && partition->data()[0] > 1) {
                partitionUpperBound = rowID;
                partitionLowerBound = rowID - 1;
                partitionRightBound = partition->data()[2];
                partitionLeftBound = partition->data()[1];
                while (x >= 0) {
                    if (partitionIndex[x][partitionRightBound] != partitionIndex[rowID][partitionRightBound]) {
                        partitionUpperBound = x + 1;
                        break;
                    } else {
                        partitionUpperBound = x;
                        x--;
                    }
                }
                if (partitionUpperBound <= partitionLowerBound) {
                    //create partition
                    tempPartition->push_back(1);
                    tempPartition->push_back(partitionID);
                    tempPartition->push_back(partitionUpperBound);
                    tempPartition->push_back(partitionLowerBound);
                    tempPartition->push_back(partitionLeftBound);
                    tempPartition->push_back(partitionRightBound);

                    partitionList.push_back(tempPartition);
                }
                tempPartition = new std::vector<uint64_t>;

                partition->data()[3] = x + 1;
                partition->data()[0]--;
            }

            //Step 2 (Down)
            x = rowID + numRows;
            if (partition->data()[4] == -1 && partition->data()[0] > 1) {
                partitionUpperBound = rowID + numRows;
                partitionLowerBound = rowID + numRows - 1;
                partitionRightBound = partition->data()[2];
                partitionLeftBound = partition->data()[1];
                while (x < tableNoOfRows) {
                    if (partitionIndex[x][partitionRightBound] != partitionIndex[rowID + numRows - 1][partitionRightBound]) {
                        partitionLowerBound = x - 1;
                        break;
                    } else {
                        partitionLowerBound = x;
                        x++;
                    }
                }
                if (partitionUpperBound <= partitionLowerBound) {
                    //create partition
                    tempPartition->push_back(1);
                    tempPartition->push_back(partitionID);
                    tempPartition->push_back(partitionUpperBound);
                    tempPartition->push_back(partitionLowerBound);
                    tempPartition->push_back(partitionLeftBound);
                    tempPartition->push_back(partitionRightBound);

                    partitionList.push_back(tempPartition);
                }
                tempPartition = new std::vector<uint64_t>;

                partition->data()[4] = x - 1;
                partition->data()[0]--;
            }

            //Step 3 (Left)
            x = columnID - 1;
            if (partition->data()[1] == -1 && partition->data()[0] > 1) {
                partitionUpperBound = (partition->data()[3] == -1) ? rowID : partition->data()[3];
                partitionLowerBound = (partition->data()[4] == -1) ? rowID + numRows - 1 : partition->data()[4];
                partitionRightBound = columnID - 1;
                partitionLeftBound = columnID;
                while (x >= 0) {
                    if (partitionIndex[partitionLowerBound][x] != partitionIndex[partitionLowerBound][columnID]) {
                        partitionLeftBound = x + 1;
                        break;
                    } else {
                        partitionLeftBound = x;
                        x--;
                    }
                }
                if (partitionLeftBound <= partitionRightBound) {
                    //create partition
                    tempPartition->push_back(1);
                    tempPartition->push_back(partitionID);
                    tempPartition->push_back(partitionUpperBound);
                    tempPartition->push_back(partitionLowerBound);
                    tempPartition->push_back(partitionLeftBound);
                    tempPartition->push_back(partitionRightBound);

                    partitionList.push_back(tempPartition);
                }
                tempPartition = new std::vector<uint64_t>;

                partition->data()[1] = x + 1;
                partition->data()[0]--;
            }
        }
    }

    tempPartition = new std::vector<uint64_t>;

    for (std::map<uint64_t, std::vector < uint64_t>*>::iterator it = partitionTableHit->begin(); it != partitionTableHit->end(); it++) {
        if (it->second->data()[0] == -1) {
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

    return partitionList;
}
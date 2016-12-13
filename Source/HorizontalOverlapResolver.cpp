#include <vector>
#include <iostream>
#include <set>
#include "HorizontalOverlapResolver.h"

Register<OverlapResolver, HorizontalOverlapResolver> hOverlapResolver("HorizontalOverlapResolver");

HorizontalOverlapResolver::HorizontalOverlapResolver() {
}

/**
 * Resolve overlap issue resulted from the creation of new partition
 */
std::vector<std::vector<uint64_t>*> HorizontalOverlapResolver::ResolveOverlapIssue(std::vector<std::vector<uint64_t>> partitionIndex, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols, bool storageLayout, std::string partitionType) {
    std::vector<std::vector < uint64_t>*> tempPartitionList, partitionList;
    int64_t partitionUpperBound, partitionLowerBound, partitionLeftBound, partitionRightBound;
    bool createPartiotion;
    //Step 1
    partitionUpperBound = rowID;
    partitionLowerBound = rowID;
    partitionRightBound = columnID - 1;
    partitionLeftBound = columnID;
    std::vector<uint64_t>* partition = new std::vector<uint64_t>;
    int64_t x;
    x = columnID - 1;
    for (int i = rowID + 1; i < rowID + numRows; i++) {
        if (partitionIndex[i][columnID] != partitionIndex[i - 1][columnID]) {
            while (x >= 0) {
                if (partitionIndex[i - 1][x] != partitionIndex[i - 1][columnID]) {
                    partitionLeftBound = x + 1;
                    break;
                } else {
                    partitionLeftBound = x;
                    x--;
                }
            }
            if (partitionLeftBound <= partitionRightBound) {
                partition->push_back(partitionUpperBound);
                partition->push_back(partitionLowerBound);
                partition->push_back(partitionLeftBound);
                partition->push_back(partitionRightBound);
                partition->push_back(partitionIndex[partitionUpperBound][columnID]);

                tempPartitionList.push_back(partition);
            }
            partition = new std::vector<uint64_t>;
            partitionUpperBound = i;
            partitionLowerBound = i;
            partitionLeftBound = columnID;
            x = columnID - 1;
        } else {
            partitionLowerBound = i;
        }
    }
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
        partition->push_back(partitionUpperBound);
        partition->push_back(partitionLowerBound);
        partition->push_back(partitionLeftBound);
        partition->push_back(partitionRightBound);
        partition->push_back(partitionIndex[partitionUpperBound][columnID]);

        tempPartitionList.push_back(partition);
    }
    partition = new std::vector<uint64_t>;

    //Step 2
    partitionUpperBound = rowID;
    partitionLowerBound = rowID;
    partitionRightBound = columnID + numCols - 1;
    partitionLeftBound = columnID + numCols;
    partition = new std::vector<uint64_t>;
    x = columnID + numCols;
    for (int i = rowID + 1; i < rowID + numRows; i++) {
        if (partitionIndex[i][columnID + numCols - 1] != partitionIndex[i - 1][columnID + numCols - 1]) {
            while (x < partitionIndex[0].size()) {
                if (partitionIndex[i - 1][x] != partitionIndex[i - 1][columnID + numCols - 1]) {
                    partitionRightBound = x - 1;
                    break;
                } else {
                    partitionRightBound = x;
                    x++;
                }
            }
            if (partitionLeftBound <= partitionRightBound) {
                partition->push_back(partitionUpperBound);
                partition->push_back(partitionLowerBound);
                partition->push_back(partitionLeftBound);
                partition->push_back(partitionRightBound);
                partition->push_back(partitionIndex[partitionUpperBound][columnID + numCols - 1]);

                tempPartitionList.push_back(partition);
            }
            partition = new std::vector<uint64_t>;
            partitionUpperBound = i;
            partitionLowerBound = i;
            partitionRightBound = columnID + numCols - 1;
            x = columnID + numCols;
        } else {
            partitionLowerBound = i;
        }
    }
    while (x < partitionIndex[0].size()) {
        if (partitionIndex[partitionLowerBound][x] != partitionIndex[partitionLowerBound][columnID + numCols - 1]) {
            partitionRightBound = x - 1;
            break;
        } else {
            partitionRightBound = x;
            x++;
        }
    }
    if (partitionLeftBound <= partitionRightBound) {
        partition->push_back(partitionUpperBound);
        partition->push_back(partitionLowerBound);
        partition->push_back(partitionLeftBound);
        partition->push_back(partitionRightBound);
        partition->push_back(partitionIndex[partitionUpperBound][columnID]);

        tempPartitionList.push_back(partition);
    }
    partition = new std::vector<uint64_t>;

    //Step 3
    partitionUpperBound = rowID;
    partitionLowerBound = rowID - 1;
    partitionRightBound = columnID;
    partitionLeftBound = columnID;
    partition = new std::vector<uint64_t>;
    x = rowID - 1;
    for (int i = columnID + 1; i < columnID + numCols; i++) {
        if (partitionIndex[rowID][i] != partitionIndex[rowID][i - 1]) {
            while (x >= 0) {
                if (partitionIndex[x][i - 1] != partitionIndex[rowID][i - 1]) {
                    partitionUpperBound = x + 1;
                    break;
                } else {
                    partitionUpperBound = x;
                    x--;
                }
            }
            createPartiotion = true;
            for (int k = 0; k < tempPartitionList.size(); k++) {
                if (tempPartitionList[k]->data()[4] == partitionIndex[partitionLeftBound][rowID]) {
                    createPartiotion = false;
                    break;
                }
            }
            if (partitionUpperBound <= partitionLowerBound && (((partitionLeftBound != columnID || (partitionLeftBound == columnID && columnID == 0)) && (partitionRightBound != columnID + numCols - 1 || (partitionRightBound == columnID + numCols - 1 && columnID + numCols - 1 == partitionIndex[0].size() - 1))) || createPartiotion)) {
                partition->push_back(partitionUpperBound);
                partition->push_back(partitionLowerBound);
                partition->push_back(partitionLeftBound);
                partition->push_back(partitionRightBound);
                partition->push_back(partitionIndex[partitionLeftBound][rowID]);

                tempPartitionList.push_back(partition);
            }
            partition = new std::vector<uint64_t>;
            partitionRightBound = i;
            partitionLeftBound = i;
            partitionUpperBound = rowID;
            x = rowID - 1;
        } else {
            partitionRightBound = i;
        }
    }
    while (x >= 0) {
        if (partitionIndex[x][partitionRightBound] != partitionIndex[rowID][partitionRightBound]) {
            partitionUpperBound = x + 1;
            break;
        } else {
            partitionUpperBound = x;
            x--;
        }
    }
    createPartiotion = true;
    for (int k = 0; k < tempPartitionList.size(); k++) {
        if (tempPartitionList[k]->data()[4] == partitionIndex[partitionLeftBound][rowID]) {
            createPartiotion = false;
            break;
        }
    }
    if (partitionUpperBound <= partitionLowerBound && (((partitionLeftBound != columnID || (partitionLeftBound == columnID && columnID == 0)) && (partitionRightBound != columnID + numCols - 1 || (partitionRightBound == columnID + numCols - 1 && columnID + numCols - 1 == partitionIndex[0].size() - 1))) || createPartiotion)) {
        partition->push_back(partitionUpperBound);
        partition->push_back(partitionLowerBound);
        partition->push_back(partitionLeftBound);
        partition->push_back(partitionRightBound);
        partition->push_back(partitionIndex[partitionLeftBound][rowID]);

        tempPartitionList.push_back(partition);
    }
    partition = new std::vector<uint64_t>;

    //Step 4
    partitionUpperBound = rowID + numRows;
    partitionLowerBound = rowID + numRows - 1;
    partitionRightBound = columnID;
    partitionLeftBound = columnID;
    partition = new std::vector<uint64_t>;
    x = rowID + numRows;
    for (int i = columnID + 1; i < columnID + numCols; i++) {
        if (partitionIndex[rowID + numRows - 1][i] != partitionIndex[rowID + numRows - 1][i - 1]) {
            while (x < partitionIndex.size()) {
                if (partitionIndex[x][i - 1] != partitionIndex[rowID + numRows - 1][i - 1]) {
                    partitionLowerBound = x - 1;
                    break;
                } else {
                    partitionLowerBound = x;
                    x++;
                }
            }
            createPartiotion = true;
            for (int k = 0; k < tempPartitionList.size(); k++) {
                if (tempPartitionList[k]->data()[4] == partitionIndex[partitionLeftBound][partitionUpperBound]) {
                    createPartiotion = false;
                    break;
                }
            }
            if (partitionUpperBound <= partitionLowerBound && (((partitionLeftBound != columnID || (partitionLeftBound == columnID && columnID == 0)) && (partitionRightBound != columnID + numCols - 1 || (partitionRightBound == columnID + numCols - 1 && columnID + numCols - 1 == partitionIndex[0].size() - 1))) || createPartiotion)) {
                partition->push_back(partitionUpperBound);
                partition->push_back(partitionLowerBound);
                partition->push_back(partitionLeftBound);
                partition->push_back(partitionRightBound);
                partition->push_back(partitionIndex[partitionLeftBound][rowID]);

                tempPartitionList.push_back(partition);
            }
            partition = new std::vector<uint64_t>;
            partitionRightBound = i;
            partitionLeftBound = i;
            partitionLowerBound = rowID + numRows - 1;
            x = rowID + numRows;
        } else {
            partitionRightBound = i;
        }
    }
    while (x < partitionIndex.size()) {
        if (partitionIndex[x][partitionRightBound] != partitionIndex[rowID + numRows - 1][partitionRightBound]) {
            partitionLowerBound = x - 1;
            break;
        } else {
            partitionLowerBound = x;
            x++;
        }
    }
    createPartiotion = true;
    for (int k = 0; k < tempPartitionList.size(); k++) {
        if (tempPartitionList[k]->data()[4] == partitionIndex[partitionLeftBound][partitionUpperBound]) {
            createPartiotion = false;
            break;
        }
    }
    if (partitionUpperBound <= partitionLowerBound && (((partitionLeftBound != columnID || (partitionLeftBound == columnID && columnID == 0)) && (partitionRightBound != columnID + numCols - 1 || (partitionRightBound == columnID + numCols - 1 && columnID + numCols - 1 == partitionIndex[0].size() - 1))) || createPartiotion)) {
        partition->push_back(partitionUpperBound);
        partition->push_back(partitionLowerBound);
        partition->push_back(partitionLeftBound);
        partition->push_back(partitionRightBound);
        partition->push_back(partitionIndex[partitionLeftBound][rowID]);

        tempPartitionList.push_back(partition);
    }
    partition = new std::vector<uint64_t>;


    for (int i = 0; i < tempPartitionList.size(); i++) {
        if (columnID != 0 && tempPartitionList[i]->data()[2] != columnID + numCols) {
            if (tempPartitionList[i]->data()[3] == columnID - 1 && tempPartitionList[i]->data()[0] == rowID && rowID != 0) {
                partitionLowerBound = tempPartitionList[i]->data()[0] - 1;
                partitionLeftBound = tempPartitionList[i]->data()[2];
                partitionUpperBound = partitionLowerBound + 1;
                partitionRightBound = partitionLeftBound;
                for (int64_t j = partitionLowerBound; j >= 0; j--) {
                    if (partitionIndex[j][partitionLeftBound] != tempPartitionList[i]->data()[4]) {
                        partitionUpperBound = j + 1;
                        break;
                    } else {
                        partitionUpperBound = j;
                    }
                }
                for (int64_t j = partitionLeftBound + 1; j < partitionIndex[0].size(); j++) {
                    if (partitionIndex[partitionLowerBound][j] != tempPartitionList[i]->data()[4]) {
                        partitionRightBound = j - 1;
                        break;
                    } else {
                        partitionRightBound = j;
                    }
                }

                if (partitionUpperBound <= partitionLowerBound) {
                    //create partition
                    partition->push_back(1);
                    partition->push_back(tempPartitionList[i]->data()[4]);
                    partition->push_back(partitionUpperBound);
                    partition->push_back(partitionLowerBound);
                    partition->push_back(partitionLeftBound);
                    partition->push_back(partitionRightBound);
                    partitionList.push_back(partition);
                    partition = new std::vector<uint64_t>;
                }

            }
            if (tempPartitionList[i]->data()[3] == columnID - 1 && tempPartitionList[i]->data()[1] == rowID + numRows - 1 && rowID + numRows - 1 < partitionIndex.size() - 1) {
                partitionUpperBound = tempPartitionList[i]->data()[1] + 1;
                partitionLeftBound = tempPartitionList[i]->data()[2];
                partitionLowerBound = partitionUpperBound - 1;
                partitionRightBound = partitionLeftBound;
                for (int64_t j = partitionUpperBound; j < partitionIndex.size(); j++) {
                    if (partitionIndex[j][partitionLeftBound] != tempPartitionList[i]->data()[4]) {
                        partitionLowerBound = j - 1;
                        break;
                    } else {
                        partitionLowerBound = j;
                    }
                }
                for (int64_t j = partitionLeftBound + 1; j < partitionIndex[0].size(); j++) {
                    if (partitionIndex[partitionUpperBound][j] != tempPartitionList[i]->data()[4]) {
                        partitionRightBound = j - 1;
                        break;
                    } else {
                        partitionRightBound = j;
                    }
                }

                if (partitionUpperBound <= partitionLowerBound) {
                    //create partition
                    partition->push_back(1);
                    partition->push_back(tempPartitionList[i]->data()[4]);
                    partition->push_back(partitionUpperBound);
                    partition->push_back(partitionLowerBound);
                    partition->push_back(partitionLeftBound);
                    partition->push_back(partitionRightBound);
                    partitionList.push_back(partition);
                    partition = new std::vector<uint64_t>;
                }

            }
        } else if (columnID + numCols - 1 < partitionIndex[0].size() - 1) {
            if (tempPartitionList[i]->data()[2] == columnID + numCols && tempPartitionList[i]->data()[0] == rowID && rowID != 0 && (columnID == 0 || tempPartitionList[i]->data()[4] != partitionIndex[tempPartitionList[i]->data()[0]][columnID - 1])) {
                partitionLowerBound = tempPartitionList[i]->data()[0] - 1;
                partitionRightBound = tempPartitionList[i]->data()[3];
                partitionUpperBound = partitionLowerBound + 1;
                partitionLeftBound = partitionRightBound;
                for (int64_t j = partitionLowerBound; j >= 0; j--) {
                    if (partitionIndex[j][partitionRightBound] != tempPartitionList[i]->data()[4]) {
                        partitionUpperBound = j + 1;
                        break;
                    } else {
                        partitionUpperBound = j;
                    }
                }
                for (int64_t j = partitionRightBound - 1; j >= 0; j--) {
                    if (partitionIndex[partitionLowerBound][j] != tempPartitionList[i]->data()[4]) {
                        partitionLeftBound = j + 1;
                        break;
                    } else {
                        partitionLeftBound = j;
                    }
                }

                if (partitionUpperBound <= partitionLowerBound) {
                    //create partition
                    partition->push_back(1);
                    partition->push_back(tempPartitionList[i]->data()[4]);
                    partition->push_back(partitionUpperBound);
                    partition->push_back(partitionLowerBound);
                    partition->push_back(partitionLeftBound);
                    partition->push_back(partitionRightBound);
                    partitionList.push_back(partition);
                    partition = new std::vector<uint64_t>;
                }

            }
            if (tempPartitionList[i]->data()[2] == columnID + numCols && tempPartitionList[i]->data()[1] == rowID + numRows - 1 && rowID + numRows - 1 < partitionIndex.size() - 1 && (columnID == 0 || tempPartitionList[i]->data()[4] != partitionIndex[tempPartitionList[i]->data()[0]][columnID - 1])) {
                partitionUpperBound = tempPartitionList[i]->data()[1] + 1;
                partitionRightBound = tempPartitionList[i]->data()[3];
                partitionLowerBound = partitionUpperBound - 1;
                partitionLeftBound = partitionRightBound;
                for (int64_t j = partitionUpperBound; j < partitionIndex.size(); j++) {
                    if (partitionIndex[j][partitionRightBound] != tempPartitionList[i]->data()[4]) {
                        partitionLowerBound = j - 1;
                        break;
                    } else {
                        partitionLowerBound = j;
                    }
                }
                for (int64_t j = partitionRightBound - 1; j >= 0; j--) {
                    if (partitionIndex[partitionUpperBound][j] != tempPartitionList[i]->data()[4]) {
                        partitionLeftBound = j + 1;
                        break;
                    } else {
                        partitionLeftBound = j;
                    }
                }

                if (partitionUpperBound <= partitionLowerBound) {
                    //create partition
                    partition->push_back(1);
                    partition->push_back(tempPartitionList[i]->data()[4]);
                    partition->push_back(partitionUpperBound);
                    partition->push_back(partitionLowerBound);
                    partition->push_back(partitionLeftBound);
                    partition->push_back(partitionRightBound);
                    partitionList.push_back(partition);
                    partition = new std::vector<uint64_t>;
                }

            }
        }

        //create partition
        partition->push_back(1);
        partition->push_back(tempPartitionList[i]->data()[4]);
        partition->push_back(tempPartitionList[i]->data()[0]);
        partition->push_back(tempPartitionList[i]->data()[1]);
        partition->push_back(tempPartitionList[i]->data()[2]);
        partition->push_back(tempPartitionList[i]->data()[3]);
        partitionList.push_back(partition);
        partition = new std::vector<uint64_t>;

    }

    std::set<uint64_t>* dropPartitionIDs = new std::set<uint64_t>;
    for (int64_t i = rowID; i < rowID + numRows; i++) {
        for (int64_t j = columnID; j < columnID + numCols; j++) {
            dropPartitionIDs->insert(partitionIndex[i][j]);
        }
    }

    std::set<uint64_t>::iterator it;
    for (it = dropPartitionIDs->begin(); it != dropPartitionIDs->end(); ++it) {
        uint64_t id = *it;
        //drop partition
        partition->push_back(0);
        partition->push_back(id);
        partition->push_back(0);
        partition->push_back(0);
        partition->push_back(0);
        partition->push_back(0);
        partitionList.push_back(partition);
        partition = new std::vector<uint64_t>;
    }

    return partitionList;
}
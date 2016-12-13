#include <iostream>
#include <vector>
#include "TableManager.h"
#include "PartitionManager.h"
using namespace std;

int main() {
    uint64_t tableID;
    try {
        cout << "//Create Table:" << endl;
        tableID = TableManager::createTable(8, 8, "Test1", 0, "SimpleTable", "HorizontalOverlapResolver");
        cout << endl;
        cout << "//Get Distinct Partition IDs:" << endl;
        std::set<uint64_t>* distinctPartitionIDs = TableManager::getdistinctPartitionIDs(tableID, 1, 2, 2, 2);
        std::set<uint64_t>::iterator it;
        cout << "Distinct Partition IDs: ";
        for (it = distinctPartitionIDs->begin(); it != distinctPartitionIDs->end(); ++it) {
            uint64_t id = *it;
            cout << id << "\t";
        }
        cout << "\r\n";
        cout << endl;
        cout << "//Get Part of Partition Index:" << endl;
        std::vector<std::vector < uint64_t>> partitionIDs = TableManager::getPartitionIDs(tableID, 1, 2, 0, 0);
        for (uint64_t i = 0; i < partitionIDs.size(); i++) {
            for (uint64_t j = 0; j < partitionIDs[0].size(); j++) {
                cout << partitionIDs[i][j] << "\t";
            }
            cout << "\r\n";
        }
        cout << endl;

        //Test 1
        PartitionManager::createPartition(tableID, 2, 2, 3, 2, 0, "ColumnStorePartition");

        cout << "//Get Distinct Partition IDs:" << endl;
        distinctPartitionIDs = TableManager::getdistinctPartitionIDs(tableID, 0, 0, 0, 0);
        cout << "Distinct Partition IDs: ";
        for (it = distinctPartitionIDs->begin(); it != distinctPartitionIDs->end(); ++it) {
            uint64_t id = *it;
            cout << id << "\t";
        }
        cout << "\r\n";
        cout << endl;
        cout << "//Get Part of Partition Index:" << endl;
        partitionIDs = TableManager::getPartitionIDs(tableID, 0, 0, 0, 0);
        for (uint64_t i = 0; i < partitionIDs.size(); i++) {
            for (uint64_t j = 0; j < partitionIDs[0].size(); j++) {
                cout << partitionIDs[i][j] << "\t";
            }
            cout << "\r\n";
        }
        cout << endl;


        //Test 2
        PartitionManager::createPartition(tableID, 4, 4, 3, 2, 0, "ColumnStorePartition");

        cout << "//Get Distinct Partition IDs:" << endl;
        distinctPartitionIDs = TableManager::getdistinctPartitionIDs(tableID, 0, 0, 0, 0);
        cout << "Distinct Partition IDs: ";
        for (it = distinctPartitionIDs->begin(); it != distinctPartitionIDs->end(); ++it) {
            uint64_t id = *it;
            cout << id << "\t";
        }
        cout << "\r\n";
        cout << endl;
        cout << "//Get Part of Partition Index:" << endl;
        partitionIDs = TableManager::getPartitionIDs(tableID, 0, 0, 0, 0);
        for (uint64_t i = 0; i < partitionIDs.size(); i++) {
            for (uint64_t j = 0; j < partitionIDs[0].size(); j++) {
                cout << partitionIDs[i][j] << "\t";
            }
            cout << "\r\n";
        }
        cout << endl;


        //Test 3
        PartitionManager::createPartition(tableID, 2, 2, 2, 3, 0, "ColumnStorePartition");

        cout << "//Get Distinct Partition IDs:" << endl;
        distinctPartitionIDs = TableManager::getdistinctPartitionIDs(tableID, 0, 0, 0, 0);
        cout << "Distinct Partition IDs: ";
        for (it = distinctPartitionIDs->begin(); it != distinctPartitionIDs->end(); ++it) {
            uint64_t id = *it;
            cout << id << "\t";
        }
        cout << "\r\n";
        cout << endl;
        cout << "//Get Part of Partition Index:" << endl;
        partitionIDs = TableManager::getPartitionIDs(tableID, 0, 0, 0, 0);
        for (uint64_t i = 0; i < partitionIDs.size(); i++) {
            for (uint64_t j = 0; j < partitionIDs[0].size(); j++) {
                cout << partitionIDs[i][j] << "\t";
            }
            cout << "\r\n";
        }
        cout << endl;
        

        //Test 4
        PartitionManager::createPartition(tableID, 5, 2, 2, 2, 0, "ColumnStorePartition");

        cout << "//Get Distinct Partition IDs:" << endl;
        distinctPartitionIDs = TableManager::getdistinctPartitionIDs(tableID, 0, 0, 0, 0);
        cout << "Distinct Partition IDs: ";
        for (it = distinctPartitionIDs->begin(); it != distinctPartitionIDs->end(); ++it) {
            uint64_t id = *it;
            cout << id << "\t";
        }
        cout << "\r\n";
        cout << endl;
        cout << "//Get Part of Partition Index:" << endl;
        partitionIDs = TableManager::getPartitionIDs(tableID, 0, 0, 0, 0);
        for (uint64_t i = 0; i < partitionIDs.size(); i++) {
            for (uint64_t j = 0; j < partitionIDs[0].size(); j++) {
                cout << partitionIDs[i][j] << "\t";
            }
            cout << "\r\n";
        }
        cout << endl;

        cout << "//Drop Table:" << endl;
        TableManager::dropTable(tableID);
    } catch (const std::exception& e) {
        cout << e.what();
    }
    try {
        TableManager::dropTable(tableID);
        tableID = TableManager::createTable(5, 3, "Test1", 0, "SimplTable", "HorizontalOverlaResolver");
        cout << tableID << endl;
    } catch (const std::exception& e) {
        cout << e.what();
    }
    return 0;
}
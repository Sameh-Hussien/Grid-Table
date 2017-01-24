#include <iostream>
#include <vector>
#include "TableManager.h"
#include "PartitionManager.h"
#include <math.h>
#include <string> 
#include <sstream>
#include <chrono>

using namespace std;

uint64_t scans = 0;
uint64_t overlappedPartitions = 0;
uint64_t scanHeight = 0;
uint64_t scanWidth = 0;
double storingTime = 0;
double scaningTime = 0;

int main() {
    uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::cout << now << std::endl;
    std::string stats[10000][8];
    int ind = 0;
    uint64_t tableID;
    cout << "//Create Table:" << endl;
    tableID = TableManager::createTable(10000, 10000, "EvaluationTest1", 0, "SimpleTable", "HorizontalOverlapResolver");
    cout << endl;
    std::set<uint64_t>* distinctPartitionIDs = TableManager::getdistinctPartitionIDs(tableID, 0, 0, 0, 0);
    std::set<uint64_t>::iterator it;
    cout << "Distinct Partition IDs: ";
    for (it = distinctPartitionIDs->begin(); it != distinctPartitionIDs->end(); ++it) {
        uint64_t id = *it;
        cout << id << "\t";
    }
    cout << "\r\n";
    cout << endl;
    //for (int i = 1; i <= 4; i++) {
    double p = pow(5, 1);
    /*for (int j = 0; j < p; j++) {
        //PartitionManager::createPartition(tableID, (625 / p) * j, 0 , (625 / p), 625,  "ColumnStorePartition");
        distinctPartitionIDs = TableManager::getdistinctPartitionIDs(tableID, 0, 0, 0, 0);
        cout << "Distinct Partition IDs: ";
        for (it = distinctPartitionIDs->begin(); it != distinctPartitionIDs->end(); ++it) {
            uint64_t id = *it;
            cout << id << "\t";
        }
        cout << "\r\n";
        cout << endl;
    }*/
    for (int k = 0; k < 30; k++) {
        PartitionManager::createPartition(tableID, 0, 0, 10000, 10000, "ColumnStorePartition");

        PartitionManager::createPartition(tableID, 2500, 0, 2500, 5000, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 2500, 5000, 2500, 5000, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 5000, 0, 2500, 5000, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 5000, 5000, 2500, 5000, "ColumnStorePartition");

        PartitionManager::createPartition(tableID, 2500, 2500, 5000, 5000, "ColumnStorePartition");
        /*distinctPartitionIDs = TableManager::getdistinctPartitionIDs(tableID, 0, 0, 0, 0);
        cout << "Distinct Partition IDs: ";
        for (it = distinctPartitionIDs->begin(); it != distinctPartitionIDs->end(); ++it) {
            uint64_t id = *it;
            cout << id << "\t";
        }
        cout << "\r\n";
        cout << endl;*/
        uint64_t start = 250 - (250 % (int) (625 / p));
        uint64_t end = (375 - (375 % (int) (625 / p)));
        scanHeight = 5000; //125 * overlappedPartitions;
        scanWidth = 10000;

        std::stringstream ss;
        ss.str(std::string());
        ss << k;
        stats[ind] [0] = ss.str();
        stats[ind] [1] = "H";
        ss.str(std::string());
        ss << scans;
        stats[ind] [2] = ss.str();
        ss.str(std::string());
        ss << overlappedPartitions;
        stats[ind] [3] = ss.str();
        ss.str(std::string());
        ss << scanHeight;
        stats[ind] [4] = ss.str();
        ss.str(std::string());
        ss << scanWidth;
        stats[ind] [5] = ss.str();
        ss.str(std::string());
        ss << storingTime;
        stats[ind] [6] = ss.str();
        ss.str(std::string());
        ss << scaningTime;
        stats[ind++] [7] = ss.str();

        for (int i = 100; i <= 2500; i += 100) {
            /*for (int j = 0; j < p; j++) {
                //PartitionManager::createPartition(tableID, (625 / p) * j, 0 , (625 / p), 625,  "ColumnStorePartition");
            }*/
            PartitionManager::createPartition(tableID, 0, 0, 10000, 10000, "ColumnStorePartition");

            PartitionManager::createPartition(tableID, 2500 - i, 0 + i, 2500 + i, 5000 - i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 2500 - i, 5000, 2500 + i, 5000 - i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 5000, 0 + i, 2500 + i, 5000 - i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 5000, 5000, 2500 + i, 5000 - i, "ColumnStorePartition");

            PartitionManager::createPartition(tableID, 2500, 2500, 5000, 5000, "ColumnStorePartition");

            scanHeight = 5000 + (i * 2); //(125 * overlappedPartitions)+250;
            scanWidth = (10000 - (i * 2));

            ss.str(std::string());
            ss << k;
            stats[ind] [0] = ss.str();
            stats[ind] [1] = "H";
            ss.str(std::string());
            ss << scans;
            stats[ind] [2] = ss.str();
            ss.str(std::string());
            ss << overlappedPartitions;
            stats[ind] [3] = ss.str();
            ss.str(std::string());
            ss << scanHeight;
            stats[ind] [4] = ss.str();
            ss.str(std::string());
            ss << scanWidth;
            stats[ind] [5] = ss.str();
            ss.str(std::string());
            ss << storingTime;
            stats[ind] [6] = ss.str();
            ss.str(std::string());
            ss << scaningTime;
            stats[ind++] [7] = ss.str();
            
            for (int j = 0; j < 8; j++) {
                cout << stats[ind - 1][j] << ", ";
            }
            cout << endl;

        }
    }
    //}
    for (int i = 0; i < ind; i++) {
        for (int j = 0; j < 8; j++) {
            cout << stats[i][j] << ", ";
        }
        cout << endl;
    }

    return 0;
}
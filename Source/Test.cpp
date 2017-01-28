#include <iostream>
#include <vector>
#include "TableManager.h"
#include "PartitionManager.h"
#include <math.h>
#include <string> 
#include <sstream>
#include <chrono>
#include <fstream>

using namespace std;

uint64_t scans = 0;
uint64_t overlappedPartitions = 0;
uint64_t scanHeight = 0;
uint64_t scanWidth = 0;
double storingTime = 0;
double scaningTime = 0;

int main() {
    std::string stats[10000][8];
    int ind = 0;
    uint64_t tableID;
    cout << "Iteration,Resolver Type,No Of Touched Cells,No Of Overlapped Partitions,Height,Width,Storing Time(MicroSec),Scaning Time(MicroSec)," << endl;

    //Horizontal Resolver Evaluation
    //Create Table
    tableID = TableManager::createTable(10000, 10000, "EvaluationTest1", 0, "SimpleTable", "HorizontalOverlapResolver");

    for (int k = 0; k < 30; k++) {
        PartitionManager::createPartition(tableID, 0, 0, 10000, 10000, "ColumnStorePartition");

        PartitionManager::createPartition(tableID, 2500, 0, 2500, 5000, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 2500, 5000, 2500, 5000, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 5000, 0, 2500, 5000, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 5000, 5000, 2500, 5000, "ColumnStorePartition");

        PartitionManager::createPartition(tableID, 2500, 2500, 5000, 5000, "ColumnStorePartition");

        scanHeight = 5000;
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
            
            for (int j = 0; j < 8; j++) {
                cout << stats[ind - 1][j] << ", ";
            }
            cout << endl;

        for (int i = 100; i <= 2500; i += 100) {

            PartitionManager::createPartition(tableID, 0, 0, 10000, 10000, "ColumnStorePartition");

            PartitionManager::createPartition(tableID, 2500 - i, 0 + i, 2500 + i, 5000 - i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 2500 - i, 5000, 2500 + i, 5000 - i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 5000, 0 + i, 2500 + i, 5000 - i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 5000, 5000, 2500 + i, 5000 - i, "ColumnStorePartition");

            PartitionManager::createPartition(tableID, 2500, 2500, 5000, 5000, "ColumnStorePartition");

            scanHeight = 5000 + (i * 2);
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
    TableManager::dropTable(tableID);

    //Vertical Resolver Evaluation
    //Create Table
    tableID = TableManager::createTable(10000, 10000, "EvaluationTest1", 0, "SimpleTable", "VerticalOverlapResolver");

    for (int k = 0; k < 30; k++) {
        PartitionManager::createPartition(tableID, 0, 0, 10000, 10000, "ColumnStorePartition");

        PartitionManager::createPartition(tableID, 0, 2500, 5000, 2500, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 0, 5000, 5000, 2500, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 5000, 2500, 5000, 2500, "ColumnStorePartition");
        PartitionManager::createPartition(tableID, 5000, 5000, 5000, 2500, "ColumnStorePartition");

        PartitionManager::createPartition(tableID, 2500, 2500, 5000, 5000, "ColumnStorePartition");

        scanHeight = 10000;
        scanWidth = 5000;

        std::stringstream ss;
        ss.str(std::string());
        ss << k;
        stats[ind] [0] = ss.str();
        stats[ind] [1] = "V";
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

        for (int i = 100; i <= 2500; i += 100) {

            PartitionManager::createPartition(tableID, 0, 0, 10000, 10000, "ColumnStorePartition");

            PartitionManager::createPartition(tableID, 0 + i, 2500 - i, 5000 - i, 2500 + i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 0 + i, 5000, 5000 - i, 2500 + i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 5000, 2500 - i, 5000 - i, 2500 + i, "ColumnStorePartition");
            PartitionManager::createPartition(tableID, 5000, 5000, 5000 - i, 2500 + i, "ColumnStorePartition");

            PartitionManager::createPartition(tableID, 2500, 2500, 5000, 5000, "ColumnStorePartition");

            scanHeight = (10000 - (i * 2));
            scanWidth = 5000 + (i * 2);

            ss.str(std::string());
            ss << k;
            stats[ind] [0] = ss.str();
            stats[ind] [1] = "V";
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
    TableManager::dropTable(tableID);

    //Write Evaluation Data to file
    ofstream evalFile;
    evalFile.open("Evaluation-Data.csv");

    evalFile << "Iteration,Resolver Type,No Of Touched Cells,No Of Overlapped Partitions,Height,Width,Storing Time(MicroSec),Scaning Time(MicroSec)" << endl;

    for (int i = 0; i < ind; i++) {
        for (int j = 0; j < 8; j++) {
            evalFile << stats[i][j];
            if (j != 7) {
                evalFile << ", ";
            }
        }
        evalFile << endl;
    }
    evalFile.close();

    return 0;
}
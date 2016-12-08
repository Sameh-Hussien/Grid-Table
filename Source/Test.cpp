#include <iostream>
#include <vector>
#include "TableManager.h"
using namespace std;

int main() {
    uint64_t tableID;
    try {
        tableID = TableManager::createTable(5, 3, "Test1", 0, "SimpleTable");
        cout << tableID << endl;
        uint64_t** partitionIDs = TableManager::getTablePartitions(tableID, 1, 2, 2, 2);
        for (uint64_t i = 0; i < 2; i++) {
            for (uint64_t j = 0; j < 2; j++) {
                cout <<partitionIDs[i][j]<<"\t";
            }
            cout << "\r\n";
        }
    } catch (const std::invalid_argument& e) {
        cout << e.what();
    }
    try {
        tableID = TableManager::createTable(5, 3, "Test1", 0, "SimplTable");
        cout << tableID << endl;
    } catch (const std::invalid_argument& e) {
        cout << e.what();
    }
    TableManager::dropTable(2);

    TableManager::dropTable(3);
    return 0;
}
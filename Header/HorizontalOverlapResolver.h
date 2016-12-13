#ifndef HORIZONTALOVERLAPRESOLVER_H
#define HORIZONTALOVERLAPRESOLVER_H

#include <string>
#include "OverlapResolver.h"

class HorizontalOverlapResolver : public OverlapResolver{
private:
    friend class Factory<OverlapResolver>::Creator<HorizontalOverlapResolver>;
    HorizontalOverlapResolver();
    //Resolve overlap issue resulted from the creation of new partition
    std::vector<std::vector<uint64_t>*> ResolveOverlapIssue(std::vector<std::vector<uint64_t>> partitionIndex, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols, bool storageLayout, std::string partitionType);
};
#endif /* HORIZONTALOVERLAPRESOLVER_H */


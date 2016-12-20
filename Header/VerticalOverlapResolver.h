#ifndef VERTICALOVERLAPRESOLVER_H
#define VERTICALOVERLAPRESOLVER_H

#include <string>
#include "OverlapResolver.h"

class VerticalOverlapResolver  : public OverlapResolver{
private:
    friend class Factory<OverlapResolver>::Creator<VerticalOverlapResolver>;
    VerticalOverlapResolver();
    //Resolve overlap issue resulted from the creation of new partition
    std::vector<std::vector<uint64_t>*> ResolveOverlapIssue(const uint64_t** partitionIndex, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols);
};

#endif /* VERTICALOVERLAPRESOLVER_H */


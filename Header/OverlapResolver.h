#ifndef OVERLAPRESOLVER_H
#define OVERLAPRESOLVER_H

#include <inttypes.h>
#include<list>
#include "FactoryCreator.h"

class OverlapResolver {
private:
    friend class ConsistencyChecker;
    virtual std::vector<std::vector<uint64_t>*> ResolveOverlapIssue(const uint64_t** partitionIndex, uint64_t rowID, uint64_t columnID, uint64_t numRows, uint64_t numCols)=0;
protected:
    OverlapResolver(){};
};
#endif /* OVERLAPRESOLVER_H */


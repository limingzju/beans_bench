#ifndef STRING_UTIL_H
#define STRING_UTIL_H

#include <string>
#include <vector>

#include "slice.h"

void split_string(const std::string& str,
                  char sep,
                  std::vector<std::string>* stringlist);


Slice TrimSpace(Slice s);
#endif
